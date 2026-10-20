"""DataHub ingestion logs: read run results from DataHub's own Kafka (MetadataChangeLog topic),
index them for browsing, and forward them to user-configured destinations (see sinks.py)."""
import io
import hashlib
import hmac
import json
import logging
import os
import re
import struct
import threading
import time
import urllib.request
from contextlib import asynccontextmanager

import fastavro
import psycopg
from confluent_kafka import Consumer
from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from fastapi import Body, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from starlette.concurrency import run_in_threadpool

import sinks

log = logging.getLogger("app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GMS_URL = os.environ["GMS_URL"].rstrip("/")  # GraphQL + the Avro schema registry live on GMS
TOKEN = os.environ["DATAHUB_TOKEN"]
TOPIC = os.environ.get("MCL_TOPIC", "MetadataChangeLog_Versioned_v1")
GROUP = os.environ.get("KAFKA_GROUP", "datahub-logs-app")
DATABASE_URL = os.environ["DATABASE_URL"]  # e.g. postgresql://user:pass@host:5432/db?sslmode=require
SESSION_MS = 7 * 86400_000
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "30"))  # 0 = keep runs forever
COOKIE_SECURE = os.environ.get("COOKIE_SECURE") == "1"  # set when served over https
STUCK_MS = int(os.environ.get("STUCK_HOURS", "3")) * 3600_000
MAX_ATTEMPTS = 5
RUNNING = ("RUNNING", "PENDING")
FAILED = ("FAILURE", "TIMEOUT", "ABORTED", "CANCELLED")
state = {"last_event_ms": None, "consumer_error": None}


# ---------- postgres: the app keeps no state of its own ----------
def db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)  # `with db() as c` commits and closes


def ph(n):
    return ",".join(["%s"] * n)


try:  # secrets in the database are encrypted with this key; comma-separate keys to rotate (first one encrypts)
    _fernet = MultiFernet([Fernet(k.strip()) for k in os.environ["APP_ENCRYPTION_KEY"].split(",")])
except (KeyError, ValueError):
    raise SystemExit("APP_ENCRYPTION_KEY missing or invalid. Make one with:\n"
                     "  python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'")


def enc(cfg):
    return _fernet.encrypt(json.dumps(cfg).encode()).decode()


def dec(token):
    try:
        return json.loads(_fernet.decrypt(token.encode()))
    except InvalidToken:  # wrong key: show the integration as having no secrets so they can be re-entered
        log.warning("cannot decrypt an integration config (APP_ENCRYPTION_KEY changed?)")
        return {}


def hash_password(pw, salt=None):
    salt = salt or os.urandom(16)
    return f"scrypt${salt.hex()}${hashlib.scrypt(pw.encode(), salt=salt, n=2**14, r=8, p=1).hex()}"


def check_password(pw, stored):
    _, salt, h = stored.split("$")
    return hmac.compare_digest(hash_password(pw, bytes.fromhex(salt)).split("$")[2], h)


def init_db():
    with db() as c:
        c.execute("SELECT pg_advisory_xact_lock(1, 0)")  # replicas may start together
        c.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            urn TEXT PRIMARY KEY, source_urn TEXT, source_name TEXT, source_type TEXT,
            status TEXT, start_ms BIGINT, duration_ms BIGINT, summary TEXT, report TEXT,
            updated_ms BIGINT, final_ms BIGINT);
        CREATE INDEX IF NOT EXISTS runs_start ON runs(start_ms DESC);
        CREATE TABLE IF NOT EXISTS destinations (
            id SERIAL PRIMARY KEY, name TEXT, type TEXT,
            config TEXT, statuses TEXT, source_urns TEXT, max_report_kb INTEGER,
            enabled INTEGER, since_ms BIGINT);
        CREATE TABLE IF NOT EXISTS deliveries (
            dest_id INTEGER, run_urn TEXT, state TEXT, attempts INTEGER, next_try_ms BIGINT,
            error TEXT, sent_ms BIGINT, PRIMARY KEY (dest_id, run_urn));
        CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, username TEXT NOT NULL, expires_ms BIGINT NOT NULL);
        """)
        user, pw = os.environ.get("ADMIN_USER"), os.environ.get("ADMIN_PASSWORD")
        if user and pw and len(pw) < 8:
            raise SystemExit("ADMIN_PASSWORD must be at least 8 characters")
        if user and pw:  # the env is the source of truth for this account: change it there to reset the password
            c.execute("INSERT INTO users VALUES (%s, %s) ON CONFLICT (username) DO UPDATE SET password_hash = excluded.password_hash",
                      (user, hash_password(pw)))
        if not c.execute("SELECT 1 FROM users").fetchone():
            raise SystemExit("No users yet: set ADMIN_USER and ADMIN_PASSWORD")


def now_ms():
    return int(time.time() * 1000)


# ---------- DataHub access (schema registry + GraphQL on GMS) ----------
def gms(path, body=None):
    req = urllib.request.Request(GMS_URL + path, json.dumps(body).encode() if body else None,
                                 {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def gql(query, variables=None):
    r = gms("/api/graphql", {"query": query, "variables": variables or {}})
    if r.get("errors"):
        raise RuntimeError(json.dumps(r["errors"])[:300])
    return r["data"]


_schemas = {}


def avro_decode(b):
    """Confluent wire format: 0x00 + 4-byte schema id + avro payload."""
    sid = struct.unpack(">I", b[1:5])[0]
    if sid not in _schemas:
        _schemas[sid] = fastavro.parse_schema(json.loads(gms(f"/schema-registry/api/schemas/ids/{sid}")["schema"]))
    return fastavro.schemaless_reader(io.BytesIO(b[5:]), _schemas[sid])


# ---------- consumer: DataHub Kafka -> postgres ----------
PY_EXC = re.compile(r"""^['"]?(?:[\w.]+\.)?\w*(?:Error|Exception)\b""")  # traceback tail, maybe inside a quoted string
HINT = re.compile(r"\b(error|exception|caused by|failed)\b", re.I)
NOISE = re.compile(r"Execution finished with errors|'errors': \[\]|~~~~")


def clean(l):  # drop the python-repr quoting the executor wraps lines in
    return re.sub(r"""(\\n)?['",\]}]*$""", "", l.lstrip("'\" "))[:300]


def summarize(report):
    """Best-effort one-liner on why a run failed: last Python exception, else last error-ish line."""
    lines = [l.strip() for l in (report or "").splitlines() if l.strip()]
    for l in reversed(lines):
        if PY_EXC.match(l):
            return clean(l)
    for l in reversed(lines):
        if HINT.search(l) and not NOISE.search(l):
            return clean(l)
    return ""


def apply_event(c, ev, ts):
    """Events for one run share a Kafka key (= same partition), so the last one seen is the newest."""
    if ev["entityType"] != "dataHubExecutionRequest" or not ev.get("aspect"):
        return
    urn, aspect = ev["entityUrn"], json.loads(ev["aspect"]["value"])
    if ev["aspectName"] == "dataHubExecutionRequestInput":
        c.execute("""INSERT INTO runs (urn, status, source_urn, start_ms, updated_ms) VALUES (%s, 'PENDING', %s, %s, %s)
                     ON CONFLICT(urn) DO UPDATE SET source_urn = excluded.source_urn,
                     start_ms = COALESCE(runs.start_ms, excluded.start_ms)""",
                  (urn, (aspect.get("source") or {}).get("ingestionSource"), aspect.get("requestedAt"), ts))
    elif ev["aspectName"] == "dataHubExecutionRequestResult":
        status, report = aspect.get("status") or "RUNNING", aspect.get("report") or ""
        c.execute("""INSERT INTO runs (urn, status, start_ms, duration_ms, summary, report, updated_ms, final_ms)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                     ON CONFLICT(urn) DO UPDATE SET status = excluded.status,
                     start_ms = COALESCE(excluded.start_ms, runs.start_ms), duration_ms = excluded.duration_ms,
                     summary = excluded.summary, report = excluded.report, updated_ms = excluded.updated_ms,
                     final_ms = COALESCE(runs.final_ms, excluded.final_ms)""",
                  (urn, status, aspect.get("startTimeMs"), aspect.get("durationMs"),
                   summarize(report) if status in FAILED else "", report, ts, None if status in RUNNING else ts))


_source_cache = {"at": 0, "by_urn": {}}


def resolve_sources(c):
    """Fill in source name/type for runs that don't have it yet."""
    rows = c.execute("SELECT urn, source_urn FROM runs WHERE source_name IS NULL LIMIT 200").fetchall()
    if rows and any(r["source_urn"] not in _source_cache["by_urn"] for r in rows) and time.time() - _source_cache["at"] > 30:
        srcs = gql("{ listIngestionSources(input:{start:0,count:500}){ ingestionSources { urn name type } } }")
        _source_cache.update(at=time.time(), by_urn={s["urn"]: s for s in srcs["listIngestionSources"]["ingestionSources"]})
    for r in rows:
        s = _source_cache["by_urn"].get(r["source_urn"])
        if not s:  # input event already expired from Kafka, or source deleted: ask DataHub per run
            s = (gql("query($u: String!) { executionRequest(urn: $u) { source { urn name type } } }", {"u": r["urn"]})
                 ["executionRequest"] or {}).get("source") or {"urn": r["source_urn"], "name": "(unknown source)", "type": ""}
        c.execute("UPDATE runs SET source_urn = %s, source_name = %s, source_type = %s WHERE urn = %s",
                  (s["urn"], s["name"], s["type"], r["urn"]))
        c.commit()  # don't hold a transaction open across the next HTTP call


def kafka_conf():
    """Aiven mTLS. Each KAFKA_* value is a file path, or the PEM itself (as Aiven app integrations provide)."""
    conf = {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "security.protocol": "SSL"}
    for env, as_file, as_pem in (("KAFKA_CA", "ssl.ca.location", "ssl.ca.pem"),
                                 ("KAFKA_CERT", "ssl.certificate.location", "ssl.certificate.pem"),
                                 ("KAFKA_KEY", "ssl.key.location", "ssl.key.pem")):
        v = os.environ.get(env, "")
        if v:
            conf[as_pem if v.lstrip().startswith("-----BEGIN") else as_file] = v
    return conf


def consumer():
    c = Consumer({**kafka_conf(), "group.id": GROUP, "auto.offset.reset": "earliest", "enable.auto.commit": False,
                  "fetch.message.max.bytes": 8 << 20, "max.partition.fetch.bytes": 8 << 20})
    c.subscribe([TOPIC])
    while True:
        try:
            batch = c.consume(200, timeout=1.0)
            if not batch:
                continue
            with db() as conn:
                for m in batch:
                    if m.error():
                        log.error("consume: %s", m.error())
                    elif m.value():
                        apply_event(conn, avro_decode(m.value()), m.timestamp()[1])
                state["last_event_ms"] = max(m.timestamp()[1] for m in batch if not m.error())
                resolve_sources(conn)
            c.commit(asynchronous=False)  # only after the database has the data
            state["consumer_error"] = None
        except Exception as e:  # e.g. GMS briefly down; uncommitted batch is re-read after restart
            log.exception("consumer failed")
            state["consumer_error"] = str(e)
            time.sleep(5)


# ---------- delivery: postgres -> destinations ----------
def pending_runs(c, d, limit=25):
    statuses, srcs = json.loads(d["statuses"]), json.loads(d["source_urns"])
    sql = f"""SELECT * FROM runs r WHERE final_ms >= %s AND source_name IS NOT NULL
        AND status IN ({ph(len(statuses))})
        {f"AND source_urn IN ({ph(len(srcs))})" if srcs else ""}
        AND NOT EXISTS (SELECT 1 FROM deliveries x WHERE x.dest_id = %s AND x.run_urn = r.urn
                        AND (x.state IN ('sent', 'dead') OR x.next_try_ms > %s))
        ORDER BY final_ms LIMIT %s"""
    return c.execute(sql, [d["since_ms"], *statuses, *srcs, d["id"], now_ms(), limit]).fetchall()


def make_event(r, max_kb):
    return {"run_id": r["urn"].rsplit(":", 1)[-1], "urn": r["urn"], "source_urn": r["source_urn"],
            "source_name": r["source_name"], "source_type": r["source_type"], "status": r["status"],
            "start_ms": r["start_ms"], "duration_ms": r["duration_ms"], "summary": r["summary"] or "",
            "report": sinks.clip(r["report"], max_kb * 1024)}


def deliver(c, d, r):
    prev = c.execute("SELECT attempts FROM deliveries WHERE dest_id = %s AND run_urn = %s", (d["id"], r["urn"])).fetchone()
    attempts = (prev["attempts"] if prev else 0) + 1
    try:
        sinks.TYPES[d["type"]]["send"](dec(d["config"]), make_event(r, d["max_report_kb"]))
        row = ("sent", attempts, None, None, now_ms())  # (state, attempts, next_try_ms, error, sent_ms)
    except Exception as e:
        log.warning("destination %s failed for %s: %s", d["name"], r["urn"], e)
        dead = attempts >= MAX_ATTEMPTS
        row = ("dead" if dead else "retry", attempts, now_ms() + 30_000 * 2 ** attempts, str(e)[:500], None)
    c.execute("""INSERT INTO deliveries VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (dest_id, run_urn) DO UPDATE SET
                 state = excluded.state, attempts = excluded.attempts, next_try_ms = excluded.next_try_ms,
                 error = excluded.error, sent_ms = excluded.sent_ms""", (d["id"], r["urn"], *row))
    return row[0] == "sent"


def prune(c):
    """Runs (with their logs) are the only data that grows; drop what is older than RETENTION_DAYS."""
    if RETENTION_DAYS > 0:
        c.execute("DELETE FROM runs WHERE COALESCE(final_ms, updated_ms) < %s", (now_ms() - RETENTION_DAYS * 86400_000,))
        c.execute("DELETE FROM deliveries d WHERE NOT EXISTS (SELECT 1 FROM runs r WHERE r.urn = d.run_urn)")
    c.execute("DELETE FROM sessions WHERE expires_ms < %s", (now_ms(),))


def delivery_worker():
    last_prune = 0
    while True:
        try:
            if time.time() - last_prune > 3600:
                with db() as c:
                    prune(c)
                last_prune = time.time()
            with db() as c:
                for d in c.execute("SELECT * FROM destinations WHERE enabled = 1").fetchall():
                    if not c.execute("SELECT pg_try_advisory_lock(2, %s) AS ok", (d["id"],)).fetchone()["ok"]:
                        continue  # another replica is delivering to this destination
                    for r in pending_runs(c, d):
                        if not deliver(c, d, r):
                            break  # destination is unhappy: don't hammer it, retry on a later pass
                        c.commit()
        except Exception:
            log.exception("delivery pass failed")
        time.sleep(5)


@asynccontextmanager
async def lifespan(_):
    init_db()
    for t in (consumer, delivery_worker):
        threading.Thread(target=t, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


# ---------- login: users + sessions live in postgres, so any replica can serve any request ----------
def sha(tok):
    return hashlib.sha256(tok.encode()).hexdigest()


def session_user(request):
    tok = request.cookies.get("session")
    if not tok:
        return None
    with db() as c:
        r = c.execute("SELECT username FROM sessions WHERE token_hash = %s AND expires_ms > %s", (sha(tok), now_ms())).fetchone()
    return r["username"] if r else None


@app.middleware("http")
async def security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.update({
        "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "same-origin",
        "Content-Security-Policy": "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'"})
    return resp


@app.middleware("http")
async def require_login(request: Request, call_next):
    p = request.url.path
    if p.startswith("/api/") and p != "/api/login" and not await run_in_threadpool(session_user, request):
        return JSONResponse({"detail": "login required"}, 401)
    return await call_next(request)


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/api/login")
def login(response: Response, body: dict = Body(...)):
    with db() as c:
        u = c.execute("SELECT * FROM users WHERE username = %s", (body.get("username"),)).fetchone()
        if not (u and check_password(body.get("password") or "", u["password_hash"])):
            time.sleep(1)  # ponytail: slows guessing per request only; real rate limiting belongs in a reverse proxy
            raise HTTPException(401, "Wrong username or password")
        tok = os.urandom(32).hex()
        c.execute("DELETE FROM sessions WHERE expires_ms < %s", (now_ms(),))
        c.execute("INSERT INTO sessions VALUES (%s, %s, %s)", (sha(tok), u["username"], now_ms() + SESSION_MS))
    response.set_cookie("session", tok, max_age=SESSION_MS // 1000, httponly=True, samesite="strict", secure=COOKIE_SECURE)
    return {"user": u["username"]}


@app.post("/api/logout")
def logout(request: Request, response: Response):
    with db() as c:
        c.execute("DELETE FROM sessions WHERE token_hash = %s", (sha(request.cookies.get("session", "")),))
    response.delete_cookie("session")
    return {"ok": True}


@app.get("/api/me")
def me(request: Request):
    return {"user": session_user(request)}


# ---------- browse API ----------
@app.get("/api/runs")
def runs(status: str = "", source: str = "", exclude: str = "", q: str = "", limit: int = 100, offset: int = 0):
    """status: comma list, or 'problems' = failed + stuck runs."""
    where, args = [], []
    stuck_before = now_ms() - STUCK_MS
    if status == "problems":
        where.append(f"(status IN ({ph(len(FAILED))}) OR (status IN ('RUNNING','PENDING') AND updated_ms < %s))")
        args += [*FAILED, stuck_before]
    elif status:
        sts = status.split(",")
        where.append(f"status IN ({ph(len(sts))})")
        args += sts
    if source:
        where.append("source_urn = %s")
        args.append(source)
    if exclude:
        ex = exclude.split(",")
        where.append(f"(source_urn IS NULL OR source_urn NOT IN ({ph(len(ex))}))")
        args += ex
    if q:  # searches the full log text
        where.append("(report ILIKE %s OR source_name ILIKE %s)")
        args += [f"%{q}%"] * 2
    w = " WHERE " + " AND ".join(where) if where else ""
    with db() as c:
        total = c.execute(f"SELECT COUNT(*) AS n FROM runs{w}", args).fetchone()["n"]
        rows = c.execute(f"""SELECT urn, source_urn, source_name, source_type, status, start_ms, duration_ms, summary, updated_ms
                             FROM runs{w} ORDER BY COALESCE(start_ms, updated_ms) DESC LIMIT %s OFFSET %s""",
                         [*args, limit, offset]).fetchall()
    return {"total": total, "items": [dict(r) | {"stuck": r["status"] in RUNNING and r["updated_ms"] < stuck_before} for r in rows]}


@app.get("/api/runs/{run_id}")
def run(run_id: str):
    with db() as c:
        r = c.execute("SELECT * FROM runs WHERE urn = %s", (f"urn:li:dataHubExecutionRequest:{run_id}",)).fetchone()
    if not r:
        raise HTTPException(404)
    return dict(r)


@app.get("/api/sources")
def sources():
    with db() as c:
        return [dict(r) for r in c.execute(
            f"""SELECT source_urn, source_name, COUNT(*) AS runs,
            COUNT(*) FILTER (WHERE status IN ({ph(len(FAILED))})) AS failed
            FROM runs WHERE source_name IS NOT NULL GROUP BY source_urn, source_name ORDER BY source_name""", FAILED)]


@app.get("/api/health")
def health():
    with db() as c:  # consumer state resets on restart; the index remembers
        last = c.execute("SELECT MAX(updated_ms) AS m FROM runs").fetchone()["m"]
    return {**state, "last_event_ms": state["last_event_ms"] or last}


# ---------- destinations API ----------
def public(d, c):
    secrets, full = sinks.secret_fields(d["type"]), dec(d["config"])
    cfg = {k: ("" if k in secrets else v) for k, v in full.items()}
    counts = {r["state"]: r["n"] for r in c.execute(
        "SELECT state, COUNT(*) AS n FROM deliveries WHERE dest_id = %s GROUP BY state", (d["id"],))}
    last = c.execute("SELECT error FROM deliveries WHERE dest_id = %s AND error IS NOT NULL ORDER BY next_try_ms DESC LIMIT 1", (d["id"],)).fetchone()
    return {"id": d["id"], "name": d["name"], "type": d["type"], "enabled": bool(d["enabled"]), "config": cfg,
            "secrets_set": [k for k in secrets if full.get(k)],
            "statuses": json.loads(d["statuses"]), "source_urns": json.loads(d["source_urns"]),
            "max_report_kb": d["max_report_kb"], "sent": counts.get("sent", 0), "retrying": counts.get("retry", 0),
            "dead": counts.get("dead", 0), "last_error": last["error"] if last else None}


def clean_config(type_, cfg, old=None):
    """Apply defaults, keep an existing secret when the form leaves it blank, check required fields."""
    if type_ not in sinks.TYPES:
        raise HTTPException(400, f"unknown destination type {type_}")
    out = {}
    for fld in sinks.TYPES[type_]["fields"]:
        v = (cfg.get(fld["name"]) or "").strip() or (old or {}).get(fld["name"]) or fld.get("default", "")
        if fld.get("required") and not v:
            raise HTTPException(400, f"{fld['label']} is required")
        out[fld["name"]] = v
    return out


@app.get("/api/destination-types")
def destination_types():
    return [{"type": k, "label": v["label"], "fields": v["fields"]} for k, v in sinks.TYPES.items()]


@app.get("/api/destinations")
def destinations():
    with db() as c:
        return [public(d, c) for d in c.execute("SELECT * FROM destinations ORDER BY id").fetchall()]


def save(body, dest_id=None):
    with db() as c:
        old = c.execute("SELECT * FROM destinations WHERE id = %s", (dest_id,)).fetchone() if dest_id else None
        cfg = clean_config(body["type"], body.get("config", {}), dec(old["config"]) if old else None)
        statuses = [s for s in body.get("statuses", FAILED) if s in FAILED + ("SUCCESS",)] or list(FAILED)
        vals = (body.get("name") or body["type"], body["type"], enc(cfg), json.dumps(statuses),
                json.dumps(body.get("source_urns", [])), max(1, min(int(body.get("max_report_kb", 900)), 5000)))
        if old:
            c.execute("UPDATE destinations SET name=%s, type=%s, config=%s, statuses=%s, source_urns=%s, max_report_kb=%s, enabled=%s WHERE id=%s",
                      (*vals, int(body.get("enabled", True)), dest_id))
        else:  # new destinations only get runs that finish from now on, unless the user asks for the backlog
            dest_id = c.execute("INSERT INTO destinations (name, type, config, statuses, source_urns, max_report_kb, enabled, since_ms) VALUES (%s,%s,%s,%s,%s,%s,1,%s) RETURNING id",
                                (*vals, 0 if body.get("backfill") else now_ms())).fetchone()["id"]
        return public(c.execute("SELECT * FROM destinations WHERE id = %s", (dest_id,)).fetchone(), c)


@app.post("/api/destinations")
def create_destination(body: dict = Body(...)):
    return save(body)


@app.put("/api/destinations/{dest_id}")
def update_destination(dest_id: int, body: dict = Body(...)):
    return save(body, dest_id)


@app.delete("/api/destinations/{dest_id}")
def delete_destination(dest_id: int):
    with db() as c:
        c.execute("DELETE FROM destinations WHERE id = %s", (dest_id,))
        c.execute("DELETE FROM deliveries WHERE dest_id = %s", (dest_id,))
    return {"ok": True}


@app.post("/api/destinations/{dest_id}/retry")
def retry_dead(dest_id: int):
    with db() as c:
        c.execute("DELETE FROM deliveries WHERE dest_id = %s AND state != 'sent'", (dest_id,))
    return {"ok": True}


@app.post("/api/destination-test")
def test_destination(body: dict = Body(...)):
    """Send a sample event with the form's settings (id = reuse the saved secrets)."""
    with db() as c:
        old = c.execute("SELECT config FROM destinations WHERE id = %s", (body.get("id"),)).fetchone() if body.get("id") else None
    cfg = clean_config(body["type"], body.get("config", {}), dec(old["config"]) if old else None)
    ev = {"run_id": f"test-{now_ms()}", "urn": f"urn:li:dataHubExecutionRequest:test-{now_ms()}", "source_urn": "urn:test",
          "source_name": "datahub-logs-app test", "source_type": "test", "status": "SUCCESS", "start_ms": now_ms(),
          "duration_ms": 0, "summary": "", "report": "Test event from the DataHub logs app. Connection works."}
    try:
        sinks.TYPES[body["type"]]["send"](cfg, ev)
    except Exception as e:
        return {"ok": False, "error": str(e)[:500]}
    return {"ok": True}


# UI (built from web/ in the Docker image); mounted last so /api/* wins
app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="ui")
