# Chargebee → Salesforce webhook (run on Railway, Render, Fly.io, Cloud Run, etc.)
# Build: docker build -t chargebee-sf-webhook .
# Run:  docker run --env-file .env -p 8080:8080 chargebee-sf-webhook
# Most hosts build from this Dockerfile and inject env vars in their UI (no local run required).

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY webhook_app.py chargebee_client.py seed_webhook_state.py sf_config.py ./
COPY salesforce_field_map.example.json ./

ENV PORT=8080
EXPOSE 8080

# Long timeout: Salesforce + optional sync; Chargebee full baseline merge runs async by default.
CMD ["sh", "-c", "exec gunicorn -w 1 -t 120 -b 0.0.0.0:${PORT:-8080} webhook_app:app"]
