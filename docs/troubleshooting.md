# Troubleshooting

Start with the logs: `docker compose logs -f app`. The status chip in the page header shows the last DataHub event time, the last Kafka consumer error, or how many messages were skipped.

## The app won't start

| Message | Cause |
|---|---|
| `Missing required environment variables: ...` | Only `DATABASE_URL` is required to start. The listed variable isn't set on the container. On a hosting platform they must be added to the application's environment; a `.env` file is only read by Docker Compose. See [Configuration](configuration.md). |
| `APP_ENCRYPTION_KEY is invalid` | Generate a key with the command in the message; it must be a Fernet key (44 characters). |
| `ADMIN_PASSWORD must be at least 8 characters` | Use a longer password. |
| `connection refused` / `could not translate host name "db"` | Postgres isn't reachable. With Compose, check `docker compose ps`; with an external database check `DATABASE_URL` and the network. |

## I can't sign in

Use `ADMIN_USER` and `ADMIN_PASSWORD` from the environment (defaults: `admin` / `change-me-now`). The account is recreated from them on every start, so to reset a forgotten password set a new `ADMIN_PASSWORD` and restart. Behind HTTPS the session cookie is `Secure`, so signing in over plain HTTP won't keep the session.

## The Runs tab is empty

1. An orange chip *N message(s) skipped* means the app dropped messages it could not decode or store; the app log has one `skipping message ...` line per message with its partition and offset. Everything else keeps flowing.
2. Look at the header chip. A red **Kafka consumer error** points at the cause:
   - `Not configured yet`: finish the **Settings** tab.
   - `SSL`/`certificate` errors: wrong CA, certificate or key in Settings (paste the full PEM including the `-----BEGIN` lines). **Test connection** in Settings shows the exact error.
   - `Failed to resolve` / timeouts: the Kafka host or port is wrong or not reachable from where the app runs (private port needs the VPC; public access must be enabled on the service).
   - `Subscribed topic not available` / authorization errors: the Kafka user needs read access to `MetadataChangeLog_Versioned_v1` and to the consumer group.
   - `schema N from DataHub: HTTP Error 401`/`403`: the access token in Settings is invalid or expired. The consumer waits and retries; nothing is skipped or lost, and consumption resumes once the token is fixed. `HTTP Error 404` means DataHub doesn't know that schema id, and `HTTP Error 5xx` or a timeout means GMS is down.
3. If there is no error but nothing arrives, DataHub may not be producing events yet. Trigger an ingestion run and watch the chip's *last event* time.
4. The filter might hide the rows: the default is *Problems only*, and *Hide sources* is remembered in the browser. Switch to *All runs* and clear *Hide sources*.

## Runs show `(unknown source)`

The app couldn't resolve the source name. Usually the token's user lacks **Manage Ingestion** (DataHub then returns `null` instead of an error), or the source was deleted. Fix the privilege; names are resolved for new runs automatically.

## There is no Runs tab

The server runs with `DISABLE_LOG_BROWSING=1`, which hides the Runs tab and disables `/api/runs`. Unset it and restart to browse logs.

## Old runs are missing

The app only sees what Kafka still has: the topic's `retention.ms` (24 hours on the Aiven topic this was built against). Runs from before the app first started (beyond that window) can't be recovered.

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

The log contains `cannot decrypt an integration config`: `APP_ENCRYPTION_KEY` differs from the one used when the integration was saved. Restore the old key, or list both with the new one first (`NEWKEY,OLDKEY`, see [Security](security.md#rotating-secrets)), or enter the credentials again.

## Starting over

```bash
docker compose down -v      # deletes the bundled Postgres volume: runs, integrations, users
docker compose up -d --build
```

The app replays what Kafka still holds. To replay without deleting data, set a new `KAFKA_GROUP`.
