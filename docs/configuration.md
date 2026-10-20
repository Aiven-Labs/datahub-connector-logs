# Configuration

The app needs only a handful of environment variables to start (database, encryption key, admin login). The **DataHub connection** (GMS URL, token, Kafka address and certificates) is entered in the app's **Settings** tab and stored encrypted in Postgres, see below. With Docker Compose they come from a `.env` file next to `docker-compose.yml`; copy [`.env.example`](../.env.example) to start. Never commit `.env` (it is in `.gitignore`).

## Required

| Variable | Description |
|---|---|
| `DATABASE_URL` | Postgres connection URI (see below). Optional with the bundled Compose database. |
| `APP_ENCRYPTION_KEY` | Fernet key that encrypts integration credentials and the DataHub connection settings in Postgres. Generate with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Comma-separate several keys to rotate; the first one encrypts, all can decrypt. **Back it up**: without it saved credentials can't be read. |
| `ADMIN_USER`, `ADMIN_PASSWORD` | The UI login. The account is created or updated on every start, so changing the password here resets it. Minimum 8 characters. |

## DataHub connection (Settings tab)

On first start the app opens the **Settings** tab. Fill in:

| Field | Description |
|---|---|
| DataHub URL (GMS) | URL of DataHub's backend, for example `https://datahub-gms.example.com`. Used for GraphQL and the Avro schema registry. This is not the web UI URL. |
| Access token | DataHub personal access token (*Settings → Access Tokens*). Its user needs the **Manage Ingestion** privilege; without it DataHub returns empty results instead of an error and sources show as `(unknown source)`. |
| Bootstrap servers | `host:port` of the Kafka cluster DataHub publishes to. On Aiven use the private port inside the VPC, or the public port that accepts client certificates from outside. |
| CA / client certificate / client key | Paste the PEM text from Aiven's *Connection information* (`ca.pem`, `service.cert`, `service.key`). |

**Test connection** checks both DataHub (including the Manage Ingestion privilege) and Kafka (including that the topic is visible) before you save. **Save** stores everything encrypted in Postgres; the consumer reconnects within a few seconds. The token and key are never sent back to the browser: leave those fields empty to keep the saved value.

The same settings can be provided as environment variables instead, which is handy for automated installs. Saved values win over the variables, and a blank field falls back to the variable.

| Variable | Setting |
|---|---|
| `GMS_URL`, `DATAHUB_TOKEN` | DataHub URL and token |
| `KAFKA_BOOTSTRAP` | Bootstrap servers |
| `KAFKA_CA`, `KAFKA_CERT`, `KAFKA_KEY` | Certificates: each is the PEM text or a **file path** (for example a volume you mount at `/certs`; the file must be readable by uid 10001) |

## Database

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | bundled `db` service | Postgres connection URI, for example `postgresql://user:pass@host:5432/db?sslmode=require`. When unset, Compose points at the bundled Postgres container. |
| `POSTGRES_PASSWORD` | `datahub` | Password for the bundled Postgres (Compose only). Set your own. |

## Optional

| Variable | Default | Description |
|---|---|---|
| `MCL_TOPIC` | `MetadataChangeLog_Versioned_v1` | Topic DataHub publishes metadata changes to. |
| `KAFKA_GROUP` | `datahub-logs-app` | Consumer group. A new name replays the topic from the beginning. |
| `STUCK_HOURS` | `3` | A `RUNNING`/`PENDING` run with no update for this long is shown as *stuck*. |
| `RETENTION_DAYS` | `30` | Runs (and their logs) older than this are deleted hourly. `0` keeps everything. |
| `COOKIE_SECURE` | off | Set to `1` when the app is served over HTTPS so the session cookie is only sent over TLS. |

## What is *not* configured here

Integration settings (Datadog API key, destination Kafka, OpenSearch URL, filters) are managed in the UI and stored encrypted in Postgres. See [Integrations](integrations.md).
