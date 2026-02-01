#!/bin/bash

set -e
alembic upgrade head
exec uvicorn main:app --host 0.0.0.0 --port "${SERVICE_PORT}"