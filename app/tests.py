"""Run against the image, without overriding its default env: docker run --rm -e DATABASE_URL=x IMAGE python tests.py"""
import os

import sinks
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
assert clean_config("datadog", {"api_key": ""}, {"api_key": "OLD"})["api_key"] == "OLD"
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
# --- the defaults baked into app/Dockerfile match the ones the app warns about ---
assert all(os.environ[k] == v for k, v in DEFAULTS.items()), "Dockerfile ENV and main.DEFAULTS differ"
print("ok")
