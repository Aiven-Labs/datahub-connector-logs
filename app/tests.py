"""Run against the image, without overriding its default env: docker run --rm -e DATABASE_URL=x IMAGE python tests.py"""
import os

import sinks
import main
from fastapi import HTTPException
from main import DEFAULTS, check_password, clean_config, dec, enc, hash_password, kafka_conf, summarize

# --- failure summary picks the real error ---
assert summarize('x\n  File "a"\ndatahub.PipelineInitError: Failed to configure: boom\n\n') == \
    "datahub.PipelineInitError: Failed to configure: boom"
assert summarize("~~~~\nExecution finished with errors.\n 'errors': []}\n 'Caused by: operation timed out\\n'\n") == \
    "Caused by: operation timed out"
assert summarize("  'x\\n'\n\"OSError: [Errno 122] Disk quota exceeded: '/tmp/x'\\n\"],\n") == \
    "OSError: [Errno 122] Disk quota exceeded: '/tmp/x'"
assert summarize("all good") == ""

# --- clip keeps the end of the log ---
assert sinks.clip("a" * 10 + "END", 3).endswith("END") and sinks.clip("short", 100) == "short"

# --- request shapes sent to Datadog / OpenSearch ---
sent = []
sinks.http = lambda *a: sent.append(a)
ev = {"run_id": "r1", "urn": "urn:li:dataHubExecutionRequest:r1", "source_urn": "s", "source_name": "pg", "source_type": "postgres",
      "status": "FAILURE", "start_ms": 1_700_000_000_000, "duration_ms": 5, "summary": "boom", "report": "LOG"}

sinks.send_datadog({"api_key": "K", "site": "datadoghq.eu", "service": "svc", "tags": "env:x"}, ev)
method, url, body, headers = sent.pop()
assert (method, url, headers) == ("POST", "https://http-intake.logs.datadoghq.eu/api/v2/logs", {"DD-API-KEY": "K"})
assert body[0]["status"] == "error" and "run_id:r1" in body[0]["ddtags"] and "env:x" in body[0]["ddtags"]
assert body[0]["message"].endswith("LOG") and body[0]["service"] == "svc"

sinks.send_opensearch({"url": "https://os:1/", "index": "idx", "username": "u", "password": "p"}, ev)
method, url, body, headers = sent.pop()
assert (method, url) == ("PUT", "https://os:1/idx/_doc/r1")  # keyed by run id => idempotent
assert headers["Authorization"] == "Basic dTpw" and body["@timestamp"].startswith("2023-11-14")

# --- config handling: defaults, required fields, secrets kept when the form leaves them blank ---
assert clean_config("datadog", {"api_key": "K"})["site"] == "datadoghq.com"
assert clean_config("datadog", {"api_key": ""}, {"api_key": "OLD", "site": "datadoghq.com"})["api_key"] == "OLD"
try:
    clean_config("datadog", {})
    raise SystemExit("missing required field was accepted")
except Exception as e:
    assert "required" in str(e.detail)

# --- secrets are encrypted at rest; passwords are hashed ---
tok = enc({"api_key": "SECRET"})
assert "SECRET" not in tok and dec(tok) == {"api_key": "SECRET"}
assert dec("not-a-token") == {}
h = hash_password("pw")
assert "pw" not in h and check_password("pw", h) and not check_password("nope", h)
# --- Kafka client config: PEM text or file path, per value ---
conf = kafka_conf({"kafka_bootstrap": "h:1", "kafka_ca": "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----",
                   "kafka_cert": "/certs/service.cert", "kafka_key": ""})
assert conf["ssl.ca.pem"].endswith("\n") and conf["ssl.certificate.location"] == "/certs/service.cert"
assert "ssl.key.pem" not in conf and "ssl.key.location" not in conf
# --- secrets never follow a changed endpoint, and bad endpoints are refused ---
def refused(fn, *a):
    try:
        fn(*a)
    except (HTTPException, ValueError):
        return True
    return False


old = {"url": "https://os.example.com", "index": "i", "username": "u", "password": "secret"}
kept = clean_config("opensearch", {"url": "https://os.example.com", "index": "i2"}, old)
assert kept["password"] == "secret" and kept["index"] == "i2"
assert refused(clean_config, "opensearch", {"url": "https://evil.example.net", "index": "i"}, old)  # password blank
moved = clean_config("opensearch", {"url": "https://evil.example.net", "password": "new"}, old)
assert moved["password"] == "new"
assert refused(clean_config, "datadog", {"site": "evil.com/x?", "api_key": "k"}, None)
assert refused(sinks.check_url, "file:///etc/passwd") and refused(sinks.check_url, "http://169.254.169.254/latest")
assert refused(sinks.check_url, "https://user:pw@host.example.com") and not refused(sinks.check_url, "https://127.0.0.1:8080")
assert refused(sinks.check_hostport, "bad host:1") and not refused(sinks.check_hostport, "10.0.0.1:9092,10.0.0.2:9092")

cur = {"gms_url": "https://gms.example.com", "token": "t", "kafka_bootstrap": "10.0.0.1:9092",
       "kafka_ca": "ca", "kafka_cert": "crt", "kafka_key": "key"}
main.settings = lambda: cur
assert main.merged_settings({})["token"] == "t"
assert main.merged_settings({"gms_url": "https://gms.example.com/"})["token"] == "t"  # same URL, trailing slash
assert refused(main.merged_settings, {"gms_url": "https://evil.example.net"})  # token not re-entered
assert main.merged_settings({"gms_url": "https://other.example.net", "token": "n"})["token"] == "n"
assert refused(main.merged_settings, {"kafka_bootstrap": "10.9.9.9:1", "kafka_key": "k"})  # ca and cert missing
assert refused(main.merged_settings, {"gms_url": "http://169.254.169.254"})

# --- a type change must not carry saved secrets to the new type's endpoint ---
assert main.old_config({"type": "opensearch", "config": enc({"password": "secret"})}, "opensearch") == {"password": "secret"}
assert main.old_config({"type": "opensearch", "config": enc({"password": "secret"})}, "kafka") is None
assert refused(clean_config, "kafka", {"bootstrap": "evil.example.net:9093", "topic": "t"}, {"password": "secret", "bootstrap": ""})  # old lacked the field

# --- only errors that blame the message itself may be skipped; GMS trouble never is ---
import json
import urllib.error
import psycopg
assert main.skippable(ValueError("bad")) and main.skippable(KeyError("x")) and main.skippable(psycopg.errors.DataError("nul"))
assert not main.skippable(main.GmsError("x")) and not main.skippable(urllib.error.HTTPError("u", 401, "x", None, None))
assert not main.skippable(urllib.error.URLError("down")) and not main.skippable(psycopg.OperationalError())
real_decode = main.avro_decode


def gms_fails(exc):
    def f(*a, **k):
        raise exc
    return f


for exc in (urllib.error.HTTPError("u", 401, "Unauthorized", None, None), urllib.error.HTTPError("u", 404, "x", None, None),
            urllib.error.URLError("down"), json.JSONDecodeError("html login page", "<html>", 0)):
    main.gms = gms_fails(exc)  # expired token, unknown schema, outage, or HTML instead of JSON
    try:
        real_decode(b"\x00\x00\x00\x00\x07rest")
        raise AssertionError("schema fetch failures must raise")
    except main.GmsError:
        pass
try:
    real_decode(b"\x00\x01")
    raise AssertionError("a truncated message must raise")
except ValueError:
    pass

# --- a message that can never be stored is skipped (after retries), a transient failure never is ---
class Conn:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def transaction(self): return self


class Msg:
    def __init__(self, v, off): self.v, self.off = v, off
    def value(self): return self.v
    def topic(self): return "t"
    def partition(self): return 0
    def offset(self): return self.off
    def timestamp(self): return (0, 1)


applied = []
main.db = lambda: Conn()
main.apply_event = lambda c, ev, ts: applied.append(ev)


def decode(b):
    if b == b"bad":
        raise ValueError("bad avro")
    if b == b"down":
        raise urllib.error.URLError("gms restarting")
    return b


main.avro_decode = decode
batch = [Msg(b"a", 1), Msg(b"bad", 2), Msg(b"b", 3)]
try:
    main.store(batch, False)
    raise AssertionError("a bad message must fail the batch before the retries run out")
except ValueError:
    pass
applied.clear()
assert main.store(batch, True) == 1 and applied == [b"a", b"b"]
try:
    main.store([Msg(b"down", 4)], True)
    raise AssertionError("a transient error must never be skipped")
except urllib.error.URLError:
    pass
main.avro_decode, main.gms = real_decode, gms_fails(urllib.error.HTTPError("u", 401, "Unauthorized", None, None))
try:  # expired DataHub token: every message fails to decode, none may be skipped
    main.store([Msg(b"\x00\x00\x00\x00\x07rest", 5)], True)
    raise AssertionError("an expired token must never cause skipping")
except main.GmsError:
    pass

# --- the defaults baked into app/Dockerfile match the ones the app warns about ---
assert all(os.environ[k] == v for k, v in DEFAULTS.items()), "Dockerfile ENV and main.DEFAULTS differ"
print("ok")
