# Integrations

An integration forwards finished ingestion runs to an external system. Create them on the **Integrations** tab: pick a destination, fill in the form, press **Test connection** (sends a sample event with the current settings) and **Save**.

## Settings every integration has

| Setting | Default | Meaning |
|---|---|---|
| Name | destination name | Shown in the table. |
| Run statuses | FAILURE, TIMEOUT, ABORTED, CANCELLED | Which final statuses to send. Tick **SUCCESS** to send successful runs too. |
| Sources | none (all sources) | Send only runs of the selected ingestion sources. Useful to skip noise such as `datahub-documents` or `datahub-gc`. |
| Max log size per run | 900 KB | Longer logs are cut, keeping the **end** of the log. Range 1 to 5000 KB. |
| Also send runs already collected | off | On a new integration only. By default only runs that finish after you create it are sent. |

Credentials are encrypted before they are stored and are never shown again; when you edit an integration, leave a password field empty to keep the saved value.

The table shows per integration: **Delivered**, **Pending** (waiting to retry), **Failed** (gave up after 5 attempts) and the **last error**. The row menu has Edit, Pause/Resume, **Retry failed** (try the pending and failed runs again) and Delete. Delivery rules and retries are described in [How it works](how-it-works.md#delivery-to-integrations).

## Event format

Every destination receives the same event, built from a run:

```json
{
  "run_id": "6a1f0c3e-...",
  "urn": "urn:li:dataHubExecutionRequest:6a1f0c3e-...",
  "source_urn": "urn:li:dataHubIngestionSource:...",
  "source_name": "my-postgres",
  "source_type": "postgres",
  "status": "FAILURE",
  "start_ms": 1760000000000,
  "duration_ms": 8123,
  "summary": "datahub.ingestion.run.pipeline.PipelineInitError: Failed to configure the source ...",
  "report": "~~~~ Execution Summary - RUN_INGEST ~~~~\n..."
}
```

`summary` is a one-line failure reason (empty for successful runs). `report` is the full log, possibly truncated as described above.

## Datadog Logs

Sends each run as one log entry through the [HTTP logs intake](https://docs.datadoghq.com/api/latest/logs/#send-logs).

| Field | Notes |
|---|---|
| API key | A Datadog **API key** (not an application key). A 403 on *Test connection* means the key is wrong or belongs to a different site. |
| Datadog site | Must match your account: `datadoghq.com`, `datadoghq.eu`, `us3.datadoghq.com`, `us5.datadoghq.com`, `ap1.datadoghq.com`, `ddog-gov.com`. |
| Service name | The `service` of the log entries. Default `datahub-ingestion`. |
| Extra tags | Comma-separated, for example `env:prod,team:data`. |

In Datadog Logs, search `service:datahub-ingestion` (or your service name). Each entry has `ddsource:datahub`, the tags `source:<name>`, `source_type`, `status` and `run_id`, status `error` for anything but `SUCCESS`, and a `datahub` attribute with the structured fields (`start_ms`, `duration_ms`, `summary`, ...). The message is the status, source, summary and then the full log.

The entry carries **no explicit timestamp**, so Datadog stamps it on arrival. Datadog drops logs older than 18 hours, and an explicit run time would lose backfilled runs. The real start time is in `datahub.start_ms`.

## Apache Kafka

Publishes each run as one message to a topic on a Kafka cluster of your choice (not necessarily DataHub's).

| Field | Notes |
|---|---|
| Bootstrap servers | `host:port`. |
| Topic | Default `datahub.ingestion.logs`. It must exist, or the cluster must allow auto-creation. |
| Security | `SSL` (mutual TLS: paste CA, client certificate and client key), `SASL_SSL` (username and password, mechanism `SCRAM-SHA-256`, `SCRAM-SHA-512` or `PLAIN`, CA optional) or `PLAINTEXT` (only for local testing). |

Messages: key = the run URN (UTF-8), value = the JSON event above, gzip-compressed on the wire. Messages can be up to 8 MB, so the topic's `max.message.bytes` and the broker's limits must allow your *Max log size*. Because the key is the run URN, a log-compacted topic keeps one message per run, and consumers can deduplicate repeated deliveries.

On Aiven, a service user's *Access control* needs write access to the topic, and for `SSL` you paste that user's certificate and key from the service's *Connection information*. Inside the same VPC use the private port.

## OpenSearch

Indexes each run as one document. Works with Aiven for OpenSearch and any OpenSearch/Elasticsearch-compatible endpoint with basic auth.

| Field | Notes |
|---|---|
| Service URL | `https://host:port`, without credentials in the URL. |
| Index | Default `datahub-ingestion-runs`. Created on first write if the cluster allows it. |
| Username / Password | Optional basic auth. |

The document id is the run id (`PUT /<index>/_doc/<run_id>`), so re-sending a run overwrites rather than duplicates. `@timestamp` is the run's start time, so Dashboards time filters reflect when the ingestion ran.

## Adding another destination

A destination is one entry in `TYPES` in [`app/sinks.py`](../app/sinks.py): a label, the form fields, and a function that sends one event and raises on failure. The UI builds its form from the field list, so no frontend change is needed.

```python
def send_webhook(cfg, ev):
    http("POST", cfg["url"], ev, {"Authorization": f"Bearer {cfg['token']}"})  # http() raises on non-2xx

TYPES["webhook"] = {"label": "Webhook", "send": send_webhook, "fields": [
    f("url", "URL", required=True, placeholder="https://..."),
    f("token", "Token", "password", secret=True),   # secret=True: encrypted at rest, never returned by the API
]}
```

Field types are `text`, `password`, `textarea` and `select` (with `options`). Add a check next to the existing ones in [`app/tests.py`](../app/tests.py) that asserts the request your function makes.
