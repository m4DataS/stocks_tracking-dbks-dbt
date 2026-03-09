FROM python:3.11-slim

# Avoid Python buffering logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps if needed (minimal here)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY app/ ./app/

# Default env (real values will come from Azure Container Apps)
ENV FINNHUB_API_KEY=""
ENV EVENTHUB_CONN_STR=""

# Expose HTTP port for FastAPI (Azure CA will map it)
EXPOSE 8000

# Run FastAPI app (app.main:app, main() starts WS thread + uvicorn)
CMD ["python", "-m", "app.main"]
