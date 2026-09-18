# Cloud Run container for the data contract registry.
#
# Distroless-adjacent slim base, non-root user, no build toolchain in the final image: the service
# has no compiled dependencies, so shipping gcc would only widen the attack surface.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    CONTRACTS_DIR=/app/contracts

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
# The contracts are baked in rather than read from GCS at runtime: a contract change is then a
# deliberate, reviewable release with an immutable artefact behind it, and the service has no
# runtime dependency that can fail. The cost is a deploy per contract change, which is correct --
# contracts should not change without one.
COPY contracts/ ./contracts/

RUN useradd --create-home --uid 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Cloud Run sets $PORT; honour it rather than hard-coding 8080.
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --workers 1
