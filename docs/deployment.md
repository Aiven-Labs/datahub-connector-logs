# Deployment

The app is a single container image plus Postgres. It keeps nothing on its own disk, so it can be restarted, replaced or scaled freely.

## What it needs to reach

| Target | Why | Typical port |
|---|---|---|
| **DataHub's Kafka** | Reads run events. Needs read access to the `MetadataChangeLog_Versioned_v1` topic and to its consumer group. | Aiven private port inside the VPC; the public port with client certificates from outside |
| **DataHub GMS** (`GMS_URL`) | Schema registry and GraphQL for source names. | 443 |
| **Postgres** (`DATABASE_URL`) | All app state. | 5432 or your provider's port |
| **Your destinations** | Datadog intake, your Kafka, OpenSearch. | per destination |

Inbound, users reach the app on port **8000** (HTTP). Put TLS in front of it.

## Docker Compose (single host)

The repository's `docker-compose.yml` runs the app and a bundled Postgres.

```bash
cp .env.example .env     # fill it in, see configuration.md
docker compose up -d --build
docker compose logs -f app
```

Before using it beyond a trial:

- Set `POSTGRES_PASSWORD` to your own value in `.env` **before the first start** (the database is created with it).
- Put the app behind a reverse proxy that terminates TLS, then set `COOKIE_SECURE=1`.
- The `db` service is not published on the host; keep it that way. The `kafka` service is a development helper behind the `dev` profile and is not started by default.
- Back up the `pg-data` volume (or use `pg_dump`) and keep `APP_ENCRYPTION_KEY` somewhere safe.

### Certificates

With Compose, mTLS files are read from `./certs`, mounted at `/certs`. The container runs as a non-root user (uid 10001), so the files must be readable by it: `chmod 644 certs/*` on Linux hosts, or avoid files altogether by pasting the PEM text into `KAFKA_CA`, `KAFKA_CERT` and `KAFKA_KEY`.

## Aiven

Run the image in the same VPC as the DataHub service's Kafka so the private Kafka port is reachable.

1. **Postgres.** Create (or reuse) an Aiven for PostgreSQL service in that VPC. A small plan is enough: the app runs a handful of short queries and stores run logs. Use its service URI as `DATABASE_URL` (it already includes `sslmode=require`).
2. **Image.** Build the image from this repository (`docker build -f app/Dockerfile -t datahub-connector-logs .`), push it to a registry the platform can pull from, and run it as a container listening on port 8000.
3. **Environment.** Set the variables from [Configuration](configuration.md). Pass `KAFKA_CA`, `KAFKA_CERT` and `KAFKA_KEY` as PEM text (Aiven's service credentials), `KAFKA_BOOTSTRAP` as the **private** `host:port`, `COOKIE_SECURE=1`, and keep `APP_ENCRYPTION_KEY`, `DATAHUB_TOKEN` and `ADMIN_PASSWORD` in your platform's secret store.
4. **Kafka user.** Use a service user with read access to the DataHub topic. DataHub's own service user works; a dedicated read-only user is better.
5. **Networking.** Expose port 8000 through HTTPS only. The app has a single admin login, so also consider restricting it to your network.
6. **Health check.** Point the platform's health check at `GET /healthz` (returns `{"ok": true}`, no login).

If you only have the public Kafka endpoint, use its client-certificate port (not the SASL one) and the same certificates. The app works from anywhere that can reach Kafka, GMS and Postgres.

## Running more than one replica

Supported. Kafka partitions are split between replicas through the consumer group, and a Postgres advisory lock lets only one replica deliver to a given integration at a time. One replica is plenty for typical DataHub volumes (a few runs per minute).

## Upgrading

```bash
git pull
docker compose up -d --build
```

Tables are created if missing on start, and data and integrations survive because they live in Postgres. There is no migration framework yet; check the diff of `init_db()` in `app/main.py` between versions if you upgrade across many changes.

## Backups and recovery

- **Postgres** holds everything: runs, integrations, users. Back it up the usual way. Run history older than the Kafka retention window can't be rebuilt from DataHub, so the database is the only copy.
- **`APP_ENCRYPTION_KEY`** must be backed up separately. A database backup without the key can't be used to recover integration credentials; you would have to re-enter them.
- **Empty database.** If you start with a fresh database, the app replays whatever Kafka still holds. To replay it again, set a new `KAFKA_GROUP`. New integrations only send runs that finish after they are created, so a replay does not flood destinations.

## Resource use

Stored data is dominated by run logs (up to a few MB each, typically far less). Runs older than `RETENTION_DAYS` (default 30) are deleted every hour. Memory use is small; the image is Python plus the Kafka client library.
