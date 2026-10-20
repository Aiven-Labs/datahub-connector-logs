# Configuration

The app is configured entirely through environment variables. With Docker Compose they come from a `.env` file next to `docker-compose.yml`; copy [`.env.example`](../.env.example) to start. Never commit `.env` (it is in `.gitignore`).

## Required

| Variable | Description |
|---|---|
| `GMS_URL` | URL of DataHub's backend (GMS), for example `https://datahub-gms.example.com`. The app uses it for GraphQL and for the Avro schema registry. This is not the web UI URL. |
| `DATAHUB_TOKEN` | DataHub personal access token (*Settings → Access Tokens*). Its user needs the **Manage Ingestion** privilege; without it DataHub returns empty results instead of an error and sources show as `(unknown source)`. |
| `KAFKA_BOOTSTRAP` | `host:port` of the Kafka cluster DataHub publishes to. On Aiven use the private port inside the VPC, or the public port that accepts client certificates from outside. |
| `KAFKA_CA`, `KAFKA_CERT`, `KAFKA_KEY` | mTLS material for that cluster. Each value is either a **file path** (default `/certs/ca.pem`, `/certs/service.cert`, `/certs/service.key`, from the `./certs` mount in Compose) or the **PEM text itself**, which is how container platforms usually pass secrets. |
| `APP_ENCRYPTION_KEY` | Fernet key that encrypts integration credentials in Postgres. Generate with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Comma-separate several keys to rotate; the first one encrypts, all can decrypt. **Back it up**: without it saved credentials can't be read. |
| `ADMIN_USER`, `ADMIN_PASSWORD` | The UI login. The account is created or updated on every start, so changing the password here resets it. Minimum 8 characters. |

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
