# Development

## Project layout

```
app/
  main.py          FastAPI app, Kafka consumer, delivery worker, login, database access
  sinks.py         Destinations: Datadog, Kafka, OpenSearch (add new ones here)
  tests.py         Self-contained checks (no test framework)
  Dockerfile       Builds the web UI, then the Python image that serves it
web/
  src/             React + TypeScript UI (Aquarium design system)
  scripts/         Generator for the dark theme (src/dark.css)
docs/              Documentation
docker-compose.yml     App + Postgres (this is what you deploy)
.env.example       All settings
```

## Run it

The normal way is Docker Compose, as in the [README](../README.md):

```bash
docker compose up -d --build     # rebuild after any change to app/ or web/
docker compose logs -f app
```

### Frontend with hot reload

Start the stack with Compose (the API on port 8000), then in another terminal:

```bash
cd web
npm ci
npm run dev        # http://localhost:5173, proxies /api to localhost:8000
```

`npm run build` type-checks (`tsc`) and builds into `web/dist`. The Docker build does the same and serves the result from FastAPI.

The UI uses `@aivenio/aquarium`. Aquarium ships a light theme only; `web/src/dark.css` mirrors its colour scales to get a dark one and is generated: after upgrading Aquarium run `python3 scripts/gen-dark.py > src/dark.css` from `web/`.

### Backend without Docker

```bash
cd app
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://user:pass@localhost:5432/datahublogs
export APP_ENCRYPTION_KEY=$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')
export ADMIN_USER=admin ADMIN_PASSWORD=choose-a-password   # the image's defaults only exist inside the image
mkdir -p static                            # the UI mount needs this directory to exist
uvicorn main:app --reload
```

## Tests

```bash
docker build -f app/Dockerfile -t datahub-connector-logs .
docker run --rm -e DATABASE_URL=x datahub-connector-logs python tests.py
```

Run it against the image without overriding its environment (it also checks that the defaults baked into the Dockerfile match the ones in `main.py`; `docker compose run` would load your `.env` and fail that check). It prints `ok` when everything passes. The checks cover failure-summary extraction, log clipping, the exact requests sent to Datadog and OpenSearch, integration config handling (defaults, required fields, secrets kept or re-requested when an endpoint changes), host validation, which consumer errors may be skipped (and that GMS errors never are), secret encryption and password hashing. No database or network is needed. CI runs the same checks plus a full image build, see [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).

## Conventions

- Keep it small. `main.py` and `sinks.py` are deliberately plain: no ORM, no framework beyond FastAPI.
- A change to delivery or parsing logic gets a check in `app/tests.py`.
- Don't commit `.env` or any certificates.
