# Troubleshooting

Start with the logs: `docker compose logs -f app`. The status chip in the page header also shows the last DataHub event time, or the last Kafka consumer error.

## The app won't start

| Message | Cause |
|---|---|
| `Missing required environment variables: ...` | The listed variables aren't set on the container. On a hosting platform they must be added to the application's environment; a `.env` file is only read by Docker Compose. See [Configuration](configuration.md). |
| `APP_ENCRYPTION_KEY missing or invalid` | Generate a key with the command in the message; it must be a Fernet key (44 characters). |
| `No users yet: set ADMIN_USER and ADMIN_PASSWORD` | First start without an admin account configured. |
| `ADMIN_PASSWORD must be at least 8 characters` | Use a longer password. |
| `connection refused` / `could not translate host name "db"` | Postgres isn't reachable. With Compose, check `docker compose ps`; with an external database check `DATABASE_URL` and the network. |

## I can't sign in

Use `ADMIN_USER` and `ADMIN_PASSWORD` from your `.env`. The account is recreated from them on every start, so after editing `.env` run `docker compose up -d` again. Behind HTTPS with `COOKIE_SECURE=1`, signing in over plain HTTP won't keep the session.

## The Runs tab is empty

1. Look at the header chip. A red **Kafka consumer error** points at the cause:
   - `SSL`/`certificate` errors: wrong or unreadable `KAFKA_CA`/`KAFKA_CERT`/`KAFKA_KEY`. With Compose and a non-root container, certificate files must be readable (`chmod 644`) or passed as PEM text.
   - `Failed to resolve` / timeouts: the Kafka host or port is wrong or not reachable from where the app runs (private port needs the VPC; public access must be enabled on the service).
   - `Subscribed topic not available` / authorization errors: the Kafka user needs read access to `MetadataChangeLog_Versioned_v1` and to the consumer group.
   - `HTTP Error 401`/`403` from the schema registry: `DATAHUB_TOKEN` is invalid or expired.
2. If there is no error but nothing arrives, DataHub may not be producing events yet. Trigger an ingestion run and watch the chip's *last event* time.
3. The filter might hide the rows: the default is *Problems only*, and *Hide sources* is remembered in the browser. Switch to *All runs* and clear *Hide sources*.

## Runs show `(unknown source)`

The app couldn't resolve the source name. Usually the token's user lacks **Manage Ingestion** (DataHub then returns `null` instead of an error), or the source was deleted. Fix the privilege; names are resolved for new runs automatically.

## Old runs are missing

The app only sees what Kafka still has. On Aiven that is about 24 hours. Runs from before the app first started (beyond that window) can't be recovered.

## An integration isn't sending

Open the integration in the table and check the counters and **Last error**.

- **Nothing delivered, no error.** Check the filters in *Edit*: by default only failed runs are sent, only runs finishing **after** the integration was created, and only the chosen sources. Tick **SUCCESS** to include successful runs, or create the integration with *Also send runs already collected*.
- **Datadog `403 Forbidden`.** Wrong API key, or the *Datadog site* doesn't match your account (`datadoghq.com` vs `datadoghq.eu` and so on). Use an API key, not an application key.
- **Datadog entries not visible.** Search `service:datahub-ingestion` (or your service name) with a wide enough time range; entries are stamped with their arrival time.
- **Kafka `timed out delivering`.** Bootstrap server unreachable from the app, wrong security protocol, or the certificate/user lacks write access to the topic. Make sure the topic exists or auto-creation is on.
- **Kafka `Message size too large`.** Lower *Max log size per run*, or raise `max.message.bytes` on the topic and the broker limits (messages can reach 8 MB).
- **OpenSearch `401`/`403`.** Credentials or index permissions. The user needs to create and write documents in the index.
- **Failed counter > 0.** After 5 attempts a run is given up. Fix the cause, then choose **Retry failed** in the row menu.

## Credentials show as empty after a restart

The log contains `cannot decrypt an integration config`: `APP_ENCRYPTION_KEY` differs from the one used when the integration was saved. Restore the old key (you can list the new one first, comma-separated, see [Security](security.md#rotating-secrets)) or enter the credentials again.

## Starting over

```bash
docker compose down -v      # deletes the bundled Postgres volume: runs, integrations, users
docker compose up -d --build
```

The app replays what Kafka still holds. To replay without deleting data, set a new `KAFKA_GROUP`.
