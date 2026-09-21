#!/bin/sh
set -eu

gunicorn \
  --bind "0.0.0.0:${PRODUCER_SETTINGS_PORT:-8042}" \
  --workers "${GUNICORN_WORKERS:-1}" \
  --threads "${GUNICORN_THREADS:-2}" \
  --access-logfile - \
  --error-logfile - \
  producer:app &

exec python producer.py
