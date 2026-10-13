#!/bin/bash
cd "/Users/vishnu/Desktop/Discovery-Engine/Sheet3"
export PYTHONPATH="/Users/vishnu/Desktop/Discovery-Engine:/Users/vishnu/Desktop/Discovery-Engine/Sheet3:$PYTHONPATH"
export PYTHONUNBUFFERED=1
"/Users/vishnu/Desktop/Discovery-Engine/.venv/bin/uvicorn" api:app --host 0.0.0.0 --port 8771 --workers 1 --log-level info >> "/Users/vishnu/Desktop/Discovery-Engine/Sheet3/Logs/api.logs" 2>&1
