#!/bin/bash
cd "/Users/vishnu/Desktop/Discovery-Engine/Sheet2"
export PYTHONPATH="/Users/vishnu/Desktop/Discovery-Engine:/Users/vishnu/Desktop/Discovery-Engine/Sheet2:$PYTHONPATH"
export PYTHONUNBUFFERED=1
"/Users/vishnu/Desktop/Discovery-Engine/.venv/bin/uvicorn" api:app --host 0.0.0.0 --port 8770 --workers 1 --log-level info >> "/Users/vishnu/Desktop/Discovery-Engine/Sheet2/Logs/api.logs" 2>&1
