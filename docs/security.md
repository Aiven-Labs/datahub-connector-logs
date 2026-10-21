# Security

## Trust model

The app is an **admin tool for a single trusted team**. Anyone who can sign in can read every ingestion log and can create integrations that make the app connect to arbitrary hosts (a Kafka cluster, an OpenSearch URL, an HTTPS endpoint) from wherever it runs. Run it where that is acceptable, usually a private network, and share the one login only with people who should see ingestion logs.

Ingestion logs can contain hostnames, file paths, table names and sometimes connection errors that echo parts of configuration. Treat them as internal data.

## What is protected and how

| Asset | Where it lives | Protection |
|---|---|---|
| Integration credentials (Datadog API key, Kafka certificates and passwords, OpenSearch password) | `destinations.config` in Postgres | Encrypted with Fernet (AES-128-CBC + HMAC-SHA256) using `APP_ENCRYPTION_KEY`, which is never stored in the database, so a dump or backup does not reveal them. **The image ships a public default key and admin password so that a fresh deploy starts without setup. Replace both before exposing the app**; the Settings tab warns until you do. The API never returns secret values. |
| Admin password | `users` table | scrypt hash. The plain value only exists in your environment. |
| Sessions | `sessions` table | Random 256-bit tokens, stored as SHA-256 hashes, `HttpOnly` + `SameSite=Strict` cookie, 7-day lifetime, deleted on logout. |
| DataHub token, Kafka certificates and key for DataHub's cluster | `settings` table in Postgres (or environment variables) | Encrypted like integration credentials. The API never returns the token, certificates or key; the Settings tab shows `(unchanged)` for saved values and masks text while you paste it. |
| Run logs | `runs.report` in Postgres | **Not encrypted at the application level**, so they stay searchable. Rely on Postgres access control, encryption at rest (Aiven encrypts disks) and TLS in transit. |

Additional measures: all `/api` routes require login, responses carry `Content-Security-Policy`, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff` and `Referrer-Policy`, the container runs as a non-root user, and a failed login is delayed by one second.

**Secrets stay with their endpoint.** Saved secrets are only used for the host they were saved with. If you change the DataHub URL, the Kafka bootstrap servers, an OpenSearch URL, a Datadog site or a destination's bootstrap servers, the matching saved secrets are not reused: the form asks you to enter them again. Endpoints must be plain `http(s)://` URLs or `host:port` values and may not point at link-local or cloud-metadata addresses (private and loopback hosts are allowed). HTTP requests do not follow redirects, so a server can't bounce an API key to another host, and error messages carry the HTTP status but never the response body. The address check happens when you save, not on every request, so DNS rebinding isn't covered.

## Hardening checklist

- [ ] Serve the app over **HTTPS** (the session cookie is then marked `Secure` automatically).
- [ ] Replace the default `ADMIN_PASSWORD` and `APP_ENCRYPTION_KEY` (the Settings tab warns while they are in use).
- [ ] Use a long random admin password and a unique `POSTGRES_PASSWORD` (or an external Postgres with TLS, `sslmode=require`).
- [ ] Keep `APP_ENCRYPTION_KEY` and `ADMIN_PASSWORD` in a secret manager, not in the image or in git.
- [ ] Use a Kafka user with **read-only** access to the DataHub topic, and a DataHub token of a user that only has *Manage Ingestion*.
- [ ] Restrict network access to port 8000 (VPC, VPN, IP allow-list). Login throttling is only a one-second delay per attempt; real rate limiting belongs in a reverse proxy.
- [ ] Do not expose Postgres publicly.
- [ ] Back up Postgres and the encryption key (separately).

## Rotating secrets

**Admin password**: if you use `ADMIN_PASSWORD`, change it in the environment and restart. The account is updated at startup. Existing sessions stay valid until they expire; to end them all, run `DELETE FROM sessions;`.

**`APP_ENCRYPTION_KEY`**:

1. Set the variable to `NEWKEY,OLDKEY` (new first) and restart. The app now encrypts with the new key and can still read data encrypted with the old one.
2. Open each integration and press **Save**. Saving re-encrypts its configuration with the new key.
3. Once every integration has been saved, remove `OLDKEY` from the variable.

If the key is lost or wrong, integrations appear with empty credentials (a warning is logged). Enter the credentials again and save.

**DataHub token / Kafka certificates**: enter the new values in the Settings tab and save. The consumer reconnects by itself.

**Destination credentials**: edit the integration, enter the new value and save.

## Reporting a vulnerability

Please report security issues privately to the maintainers rather than opening a public issue.
