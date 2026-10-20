# Build context is the repo root (see docker-compose.yml).
FROM node:24-slim AS web
WORKDIR /web
COPY web/package*.json ./
RUN npm ci
COPY web .
RUN npm run build

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
# Defaults so a fresh deploy starts without setup. Replace them in your platform's environment variables
# (Aiven: application settings) or in .env; the Settings tab warns while they are in use. See app/main.py DEFAULTS.
ENV ADMIN_USER=admin
ENV ADMIN_PASSWORD=change-me-now
ENV APP_ENCRYPTION_KEY=OUtBFNS7RLufbap6HmC4neMXDM4tT8liuMIQd89nLA8=
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app .
COPY --from=web /web/dist static
RUN useradd --system --uid 10001 app
USER app
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
