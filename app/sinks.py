"""Destinations. Each type declares `fields` (the UI renders its form from them) and
`send(cfg, event)`, which raises on failure. To add an integration, add one entry to TYPES.

event = {run_id, urn, source_urn, source_name, source_type, status, start_ms, duration_ms, summary, report}
"""
import base64
import ipaddress
import json
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from confluent_kafka import Producer


def clip(text, max_bytes):
    """Keep the END of the log (that's where the error is)."""
    b = (text or "").encode()
    return text if len(b) <= max_bytes else "[...truncated...]\n" + b[-max_bytes:].decode(errors="ignore")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **kw):  # urllib would replay our auth headers to wherever a server redirects
        return None


opener = urllib.request.build_opener(_NoRedirect)


def check_host(host):
    """Secrets are sent to hosts an admin typed in. Refuse cloud metadata / link-local targets (private and
    loopback stay allowed: DataHub and OpenSearch are often internal).
    ponytail: checked when saving, not on every request, so DNS rebinding isn't covered; pin the resolved IP to fix."""
    if not host or host.lower() in ("metadata.google.internal", "metadata"):
        raise ValueError("invalid host")
    try:
        addrs = {a[4][0] for a in socket.getaddrinfo(host, None)}
    except socket.gaierror:
        return  # unresolvable here: the connection test will say so
    for a in addrs:
        ip = ipaddress.ip_address(a.split("%")[0])
        if ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip == ipaddress.ip_address("fd00:ec2::254"):
            raise ValueError(f"{host} resolves to a link-local or reserved address")


def check_url(url):
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ("http", "https") or not u.hostname or u.username or u.password:
        raise ValueError("must be an http:// or https:// URL without credentials")
    check_host(u.hostname)


def check_hostport(value):
    for hp in value.split(","):
        host, sep, port = hp.strip().rpartition(":")
        if not (sep and port.isdigit() and re.fullmatch(r"[A-Za-z0-9.\-]+|\[[0-9A-Fa-f:]+\]", host)):
            raise ValueError("must look like host:port (comma-separated for several)")
        check_host(host.strip("[]"))


def check_site(site):  # goes into https://http-intake.logs.<site>/...
    if not re.fullmatch(r"[A-Za-z0-9]([A-Za-z0-9.\-]*[A-Za-z0-9])?", site):
        raise ValueError("must be a Datadog site such as datadoghq.com")


def http(method, url, body, headers):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json", **headers}, method=method)
    try:
        opener.open(req, timeout=30).read()
    except urllib.error.HTTPError as e:  # status only: response bodies can echo request data or internal details
        raise RuntimeError(f"HTTP {e.code} {e.reason}") from None


# ---------- Kafka ----------
_producers = {}


def send_kafka(cfg, ev):
    key = json.dumps(cfg, sort_keys=True)
    if key not in _producers:
        conf = {"bootstrap.servers": cfg["bootstrap"], "security.protocol": cfg["security_protocol"],
                "compression.type": "gzip", "message.max.bytes": 8 * 1024 * 1024}
        for field, prop in (("ca_pem", "ssl.ca.pem"), ("cert_pem", "ssl.certificate.pem"), ("key_pem", "ssl.key.pem")):
            if cfg.get(field):
                conf[prop] = cfg[field]
        if cfg["security_protocol"] == "SASL_SSL":
            conf.update({"sasl.mechanisms": cfg["sasl_mechanism"], "sasl.username": cfg["username"], "sasl.password": cfg["password"]})
        _producers[key] = Producer(conf)
    errors = []
    _producers[key].produce(cfg["topic"], json.dumps(ev).encode(), key=ev["urn"].encode(),
                            on_delivery=lambda err, _m: err and errors.append(err))
    if _producers[key].flush(20) or errors:
        _producers.pop(key, None)  # rebuild the connection next time
        raise RuntimeError(str(errors[0]) if errors else "timed out delivering to Kafka")


# ---------- Datadog ----------
def send_datadog(cfg, ev):
    tags = [f"source:{ev['source_name']}", f"source_type:{ev['source_type']}", f"status:{ev['status']}", f"run_id:{ev['run_id']}"]
    if cfg.get("tags"):
        tags.append(cfg["tags"])
    # No explicit timestamp: Datadog drops logs older than 18h, which would lose backfilled runs.
    # The run's real start time is in datahub.start_ms.
    http("POST", f"https://http-intake.logs.{cfg['site']}/api/v2/logs", [{
        "ddsource": "datahub", "service": cfg["service"], "ddtags": ",".join(tags),
        "status": "info" if ev["status"] == "SUCCESS" else "error",
        "message": f"DataHub ingestion {ev['status']}: {ev['source_name']}\n{ev['summary']}\n\n{ev['report']}",
        "datahub": {k: ev[k] for k in ("run_id", "source_name", "source_type", "status", "start_ms", "duration_ms", "summary")},
    }], {"DD-API-KEY": cfg["api_key"]})


# ---------- OpenSearch ----------
def send_opensearch(cfg, ev):
    auth = {}
    if cfg.get("username"):
        auth["Authorization"] = "Basic " + base64.b64encode(f"{cfg['username']}:{cfg['password']}".encode()).decode()
    doc = {**ev, "@timestamp": datetime.fromtimestamp((ev["start_ms"] or 0) / 1000, timezone.utc).isoformat()}
    # PUT by run id: re-sending the same run overwrites instead of duplicating.
    http("PUT", f"{cfg['url'].rstrip('/')}/{cfg['index']}/_doc/{ev['run_id']}", doc, auth)


def f(name, label, type="text", **kw):
    return {"name": name, "label": label, "type": type, **kw}


TYPES = {
    "datadog": {"label": "Datadog Logs", "send": send_datadog, "fields": [
        f("api_key", "API key", "password", secret=True, required=True),
        f("site", "Datadog site", default="datadoghq.com", required=True, endpoint=check_site, placeholder="datadoghq.com / datadoghq.eu / us5.datadoghq.com"),
        f("service", "Service name", default="datahub-ingestion", required=True),
        f("tags", "Extra tags", placeholder="env:prod,team:data"),
    ]},
    "kafka": {"label": "Apache Kafka", "send": send_kafka, "fields": [
        f("bootstrap", "Bootstrap servers", required=True, endpoint=check_hostport, placeholder="host:port"),
        f("topic", "Topic", required=True, default="datahub.ingestion.logs"),
        f("security_protocol", "Security", "select", options=["SSL", "SASL_SSL", "PLAINTEXT"], default="SSL"),
        f("ca_pem", "CA certificate (PEM)", "textarea"),
        f("cert_pem", "Client certificate (PEM, for SSL)", "textarea"),
        f("key_pem", "Client key (PEM, for SSL)", "textarea", secret=True),
        f("sasl_mechanism", "SASL mechanism", "select", options=["SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"], default="SCRAM-SHA-256"),
        f("username", "SASL username"),
        f("password", "SASL password", "password", secret=True),
    ]},
    "opensearch": {"label": "OpenSearch", "send": send_opensearch, "fields": [
        f("url", "Service URL", required=True, endpoint=check_url, placeholder="https://host:port"),
        f("index", "Index", required=True, default="datahub-ingestion-runs"),
        f("username", "Username"),
        f("password", "Password", "password", secret=True),
    ]},
}


def secret_fields(type_):
    return {x["name"] for x in TYPES[type_]["fields"] if x.get("secret")}
