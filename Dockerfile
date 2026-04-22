# Chargebee → Salesforce webhook (run on Railway, Render, Fly.io, Cloud Run, etc.)
# Build: docker build -t chargebee-sf-webhook .
# Run:  docker run --env-file .env -p 8080:8080 chargebee-sf-webhook
# Most hosts build from this Dockerfile and inject env vars in their UI (no local run required).

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY webhook_app.py chargebee_client.py seed_webhook_state.py ./

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "exec gunicorn -w 1 -b 0.0.0.0:${PORT:-8080} webhook_app:app"]
