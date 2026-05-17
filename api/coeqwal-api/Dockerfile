# syntax=docker/dockerfile:1.6
#
# COEQWAL FastAPI image
#
# Build context: api/coeqwal-api/ (this directory).
#
#   Local dev:   docker build --target dev  -t coeqwal-api:dev  .
#   Production:  docker build --target prod -t coeqwal-api:prod .
#                (or plain `docker build .` since prod is the default last stage)
#
# CI builds the prod target and pushes to ECR (see .github/workflows/api.yml).

############################################################
# Stage 1: base
#   Python runtime, system deps, Python deps, and app source.
#   Both dev and prod inherit from this stage so their layer
#   cache is shared.
############################################################
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

############################################################
# Stage 2: dev
#   Runs as root, default asyncio loop, no access log.
#   Override CMD with --reload locally if you want autoreload.
############################################################
FROM base AS dev

ENV ENVIRONMENT=development

CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000"]

############################################################
# Stage 3: prod (default target)
#   Non-root user, uvloop, access log, single worker.
#   This is the image CI pushes to ECR.
############################################################
FROM base AS prod

RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

ENV ENVIRONMENT=production

CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--access-log", \
     "--use-colors", \
     "--loop", "uvloop"]
