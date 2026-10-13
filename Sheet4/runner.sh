#!/bin/bash
cd "/Users/vishnu/Desktop/Discovery-Engine/Sheet4"
export PYTHONPATH="/Users/vishnu/Desktop/Discovery-Engine:/Users/vishnu/Desktop/Discovery-Engine/Sheet4:$PYTHONPATH"
export PYTHONUNBUFFERED=1
"/Users/vishnu/Desktop/Discovery-Engine/.venv/bin/uvicorn" api:app --host 0.0.0.0 --port 8772 --workers 1 --log-level info >> "/Users/vishnu/Desktop/Discovery-Engine/Sheet4/Logs/api.logs" 2>&1
