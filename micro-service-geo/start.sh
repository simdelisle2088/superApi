#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Define values for parameters
HOST="172.25.22.158"
PORT=8001
LOOP="uvloop"
HTTP="httptools"
LOG_LEVEL="info"

# Check Uvicorn version
uvicorn --version

# Start Uvicorn server
uvicorn geo:app \
    --host $HOST \
    --port $PORT \
    --loop $LOOP \
    --http $HTTP \
    --log-level $LOG_LEVEL \
    --reload \
    --no-use-colors \
    --proxy-headers \
    --no-server-header \
    --no-date-header
