# Configuration

The app needs only a handful of environment variables to start (a database, an encryption key and an admin login), and the image ships defaults for the last two. The **DataHub connection** (GMS URL, token, Kafka address and certificates) is entered in the app's **Settings** tab and stored encrypted in Postgres, see below. With Docker Compose they come from a `.env` file next to `docker-compose.yml`; copy [`.env.example`](../.env.example) to start. Never commit `.env` (it is in `.gitignore`).

## Security and login (defaults included)

The image sets these three variables to **public default values** (see `app/Dockerfile`) so a fresh deploy starts with no setup. They are meant to be replaced: the Settings tab shows a warning for as long as the default password or key is in use. On a platform such as Aiven they appear in the application's environment variables, where you can edit them. Change them before exposing the app, because anyone can read the defaults in the repository.

| Variable | Description |
|---|---|
| `APP_ENCRYPTION_KEY` | Fernet key that encrypts integration credentials and the DataHub connection settings in Postgres. Default: a public key from the repository. Generate your own with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. **Changing it:** set it to `NEWKEY,OLDKEY` (new first) so existing data stays readable, then re-save your integrations and Settings to re-encrypt them with the new key, and finally drop the old key. **Back it up**: without it saved credentials can't be read. |
| `ADMIN_USER`, `ADMIN_PASSWORD` | The UI login. Defaults: `admin` / `change-me-now`. The account is created or updated on every start, so changing the password here resets it. Minimum 8 characters. |
| `COOKIE_SECURE` | The session cookie (default: automatic). It is marked `Secure` automatically when the request arrives over HTTPS (`X-Forwarded-Proto`). Set `1` to force it. |

## DataHub connection (Settings tab)

On first start the app opens the **Settings** tab. Fill in:

| Field | Description |
|---|---|
| DataHub URL (GMS) | URL of DataHub's backend, for example `https://datahub-gms.example.com`. Used for GraphQL and the Avro schema registry. This is not the web UI URL. |
| Access token | DataHub personal access token (*Settings → Access Tokens*). Its user needs the **Manage Ingestion** privilege; without it DataHub returns empty results instead of an error and sources show as `(unknown source)`. |
| Bootstrap servers | `host:port` of the Kafka cluster DataHub publishes to. On Aiven use the private port inside the VPC, or the public port that accepts client certificates from outside. |
| CA / client certificate / client key | Paste the PEM text from Aiven's *Connection information* (`ca.pem`, `service.cert`, `service.key`). |

**Test connection** checks both DataHub (including the Manage Ingestion privilege) and Kafka (including that the topic is visible) before you save. **Save** stores everything encrypted in Postgres; the consumer reconnects within a few seconds. The token and certificates are never sent back to the browser, and pasted text is masked (use *Show pasted values* to check it): leave a field empty to keep the saved value.

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

## What is *not* configured here

Integration settings (Datadog API key, destination Kafka, OpenSearch URL, filters) are managed in the UI and stored encrypted in Postgres. See [Integrations](integrations.md).
