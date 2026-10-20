# How it works

## Components

One container (`app`, Python/FastAPI) plus Postgres. The web UI is a React single-page app built with the Aiven Aquarium design system (`@aivenio/aquarium`); it is compiled into the image and served by the same FastAPI process, so there is nothing else to deploy.

Inside the app process:

| Part | What it does |
|---|---|
| **Consumer** (thread) | Reads DataHub's `MetadataChangeLog_Versioned_v1` Kafka topic and upserts runs into Postgres. |
| **Delivery worker** (thread) | Every 5 seconds, sends finished runs to each enabled integration; once an hour, prunes old runs. |
| **HTTP API + UI** | Login, browsing runs, managing integrations. |

## Why Kafka and not the DataHub API

DataHub's GraphQL API can return run logs, but only by polling every source, and DataHub prunes old runs. Every ingestion run in DataHub is stored as a `dataHubExecutionRequest` entity, and each change to it is published as a *MetadataChangeLog* event. Those events carry the whole log, so listening to the topic gives push-based delivery, nothing to poll, and the full log text. See [Reading ingestion logs over the API](reading-logs-over-the-api.md) for what the API offers.

## Consuming DataHub's Kafka

The topic holds events for every entity type. The app keeps only `entityType = dataHubExecutionRequest` and two aspects:

- `dataHubExecutionRequestInput` (run requested): carries the ingestion source URN and the request time.
- `dataHubExecutionRequestResult` (progress and final result): carries `status`, `startTimeMs`, `durationMs` and the full `report` (the log). A run sends several of these while `RUNNING`; the last one has the final status.

Details that matter:

- **Wire format.** Messages are Avro in Confluent wire format (magic byte, 4-byte schema id, payload). Schemas are fetched from the schema registry that DataHub serves at `GMS_URL/schema-registry/api/schemas/ids/{id}`, authenticated with `DATAHUB_TOKEN`, and cached.
- **Ordering.** All events for one run share a Kafka key (the run URN), so they land in one partition and arrive in order. The last event seen is the newest.
- **Source names.** Events only contain the source URN. The app resolves names through the GraphQL `listIngestionSources` query (cached for 30 seconds). If the source is gone or the input event already expired from Kafka, it asks DataHub for that run individually, and otherwise shows `(unknown source)`.
- **Commit after write.** Kafka offsets are committed only after Postgres has the data, so a crash re-reads the batch instead of losing it. Writes are upserts, so re-reading is harmless.
- **Consumer group.** `KAFKA_GROUP` (default `datahub-logs-app`). To replay the retention window into an empty database, point the app at a new group name.
- **Failure summary.** For failed runs the app extracts a one-line reason: the last Python exception in the log, else the last line that looks like an error.

## Run status

DataHub statuses: `SUCCESS`, `FAILURE`, `TIMEOUT`, `CANCELLED`, `ABORTED`, `RUNNING`, `PENDING`.

A run that hangs never writes a final result ([datahub#17397](https://github.com/datahub-project/datahub/issues/17397)). The UI marks a `RUNNING` or `PENDING` run with no update for `STUCK_HOURS` (default 3) as **STUCK** and includes it in the *Problems only* filter. Stuck runs are not forwarded to integrations.

## Delivery to integrations

For every enabled integration, the worker finds runs that:

1. have a final status (not `RUNNING`/`PENDING`) and a resolved source name,
2. match the integration's **statuses** and **sources** filters,
3. finished after the integration was created (unless *Also send runs already collected* was ticked),
4. have not been delivered, dead-lettered, or scheduled for a later retry.

It sends them oldest first, 25 per pass. Per run and integration the app records a delivery row: `sent`, `retry` or `dead`.

- **Retries.** On failure the run is retried with exponential backoff (60 s, 2 min, 4 min, 8 min). After 5 failed attempts it becomes `dead`. When one run fails the worker stops for that integration for the pass instead of hammering a broken endpoint.
- **Retry failed** in the UI clears the `retry` and `dead` rows so the runs are attempted again.
- **At-least-once.** If the app crashes between sending and recording, the run is sent again. Receivers should deduplicate on the run id.
- **Large logs.** The log is cut to *Max log size per run* (default 900 KB) keeping the **end**, where the error usually is, prefixed with `[...truncated...]`. DataHub itself caps stored logs at roughly 3.8 MB.
- **Several replicas.** A Postgres advisory lock per integration ensures only one replica delivers to it at a time. The Kafka consumer group splits partitions between replicas.

## Data model (Postgres)

Created automatically on startup.

| Table | Purpose |
|---|---|
| `runs` | One row per ingestion run: status, source, timestamps, summary and the full log (`report`). Pruned after `RETENTION_DAYS`. |
| `destinations` | Integrations. `config` holds the encrypted JSON of the form (including credentials). |
| `deliveries` | Per integration and run: state, attempts, next retry, last error. |
| `users`, `sessions` | The admin account (scrypt hash) and login sessions (token stored as SHA-256). |

## HTTP API

The UI uses a JSON API under `/api`. Everything except `POST /api/login` requires the session cookie. `GET /healthz` is public.

| Endpoint | |
|---|---|
| `POST /api/login`, `POST /api/logout`, `GET /api/me` | Session handling. Login body: `{"username", "password"}`. |
| `GET /api/runs` | Query: `status` (`problems`, or comma list), `source`, `exclude` (comma list of source URNs), `q` (log text), `limit`, `offset`. |
| `GET /api/runs/{run_id}` | One run including the full `report`. |
| `GET /api/sources` | Sources with run and failure counts. |
| `GET /api/health` | Consumer state: last event time and last error. |
| `GET /api/destination-types` | Destination types and their form fields. |
| `GET/POST /api/destinations`, `PUT/DELETE /api/destinations/{id}` | Manage integrations. Secrets are never returned. |
| `POST /api/destinations/{id}/retry` | Clear failed/pending deliveries. |
| `POST /api/destination-test` | Send a sample event with the given settings. |
