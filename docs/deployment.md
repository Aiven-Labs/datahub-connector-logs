# Deployment

The app is a single container image plus Postgres. It keeps nothing on its own disk, so it can be restarted, replaced or scaled freely.

## What it needs to reach

| Target | Why | Typical port |
|---|---|---|
| **DataHub's Kafka** | Reads run events. Needs read access to the `MetadataChangeLog_Versioned_v1` topic and to its consumer group. | Aiven private port inside the VPC; the public port with client certificates from outside |
| **DataHub GMS** (the URL in Settings) | Schema registry and GraphQL for source names. | 443 |
| **Postgres** (`DATABASE_URL`) | All app state. | 5432 or your provider's port |
| **Your destinations** | Datadog intake, your Kafka, OpenSearch. | per destination |

Inbound, users reach the app on port **8000** (HTTP). Put TLS in front of it.

## Docker Compose (single host)

The repository's `docker-compose.yml` runs the app and a bundled Postgres.

```bash
cp .env.example .env     # optional: nothing is required, see configuration.md
docker compose up -d --build
docker compose logs -f app
```

Then open <http://localhost:8000>, sign in (default `admin` / `change-me-now`, unless you set `ADMIN_USER`/`ADMIN_PASSWORD` in `.env`) and complete the **Settings** tab (DataHub URL and token, Kafka address and certificates).

Before using it beyond a trial:

- Set `POSTGRES_PASSWORD` to your own value in `.env` **before the first start** (the database is created with it).
- Put the app behind a reverse proxy that terminates TLS (it should send `X-Forwarded-Proto`; otherwise set `COOKIE_SECURE=1`).
- The `db` service is not published on the host; keep it that way. `docker-compose.yml` contains only the app and Postgres. The app does not run or need a Kafka of its own: DataHub's Kafka and your destinations are external services reached through settings.
- Back up the `pg-data` volume (or use `pg_dump`) and keep `APP_ENCRYPTION_KEY` somewhere safe.

## Importing the Compose file into a platform

`docker-compose.yml` defines two services, `app` and `db` (Postgres). That is everything the application itself needs. There is no Kafka service: DataHub's Kafka and your destinations are existing external systems that the app connects to using the details you enter in the Settings tab.

Platforms that read a Compose file (including Aiven's application deployment) turn each service into a managed resource: `app` becomes the application and `db` becomes a managed PostgreSQL. Whatever the platform generates for the database, make sure the app receives its connection string as `DATABASE_URL`. If you would rather use an existing Postgres, delete the `db` service and the app's `depends_on` from the file before importing and set `DATABASE_URL` yourself.

## Aiven

Run the image in the same VPC as the DataHub service's Kafka so the private Kafka port is reachable.

1. **Postgres.** Create (or reuse) an Aiven for PostgreSQL service in that VPC. A small plan is enough: the app runs a handful of short queries and stores run logs. Use its service URI as `DATABASE_URL` (it already includes `sslmode=require`), or let the platform's Postgres integration inject it.
2. **Image.** Either let Aiven build from this repository (connect the GitHub repo, Dockerfile `app/Dockerfile`, build context `./`, port 8000), or build it yourself (`docker build -f app/Dockerfile -t datahub-connector-logs .`), push it to a registry the platform can pull from, and run it as a container listening on port 8000.
3. **Environment.** `DATABASE_URL` is injected by the Postgres integration. The image brings default values for `ADMIN_USER`, `ADMIN_PASSWORD` and `APP_ENCRYPTION_KEY`, which Aiven lists under the application's environment variables. The app starts with them, but they are public: edit `ADMIN_PASSWORD` and `APP_ENCRYPTION_KEY` (mark them secret) once it is running. See [Configuration](configuration.md) for changing the key safely.
4. **Connect to DataHub.** Open the app, sign in and fill in the **Settings** tab: the GMS URL, an access token, the **private** Kafka `host:port` and the PEM text of the CA, client certificate and key from the Kafka service's *Connection information*. Press **Test connection**, then **Save**. Use a dedicated Kafka service user with read-only access to the DataHub topic and the consumer group. Don't reuse DataHub's own service user: it can write to DataHub's topics.
5. **Networking.** Expose port 8000 through HTTPS only. The app has a single admin login, so also consider restricting it to your network.
6. **Health check.** Point the platform's health check at `GET /healthz` (returns `{"ok": true}`, no login). It only proves the web server is up, not that the consumer or delivery worker are running; watch the header chip in the UI for that.

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

- **Postgres** holds everything: runs, integrations, users. Back it up the usual way. Run history older than the Kafka retention window can't be rebuilt from DataHub, so the database is the only copy (for as long as `RETENTION_DAYS` keeps it).
- **`APP_ENCRYPTION_KEY`** must be backed up separately once you have changed it from the default. A database backup without the key can't be used to recover credentials; you would have to re-enter them.
- **Empty database.** If you start with a fresh database, the app replays whatever Kafka still holds. To replay it again, set a new `KAFKA_GROUP`. New integrations only send runs that finish after they are created, so a replay does not flood destinations.

## Resource use

Stored data is dominated by run logs (up to a few MB each, typically far less). Runs older than `RETENTION_DAYS` (default 7, or 2 with `DISABLE_LOG_BROWSING=1`) are deleted every hour. Memory use is small; the image is Python plus the Kafka client library.
