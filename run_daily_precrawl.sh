#!/bin/bash
# ==========================================================
# 🚀 Discovery Engine - Daily Advance Pre-Crawl Runner
# Triggered at 11:05 PM IST (5 23 * * *)
# ==========================================================

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTNAME_LOWER=$(hostname 2>/dev/null | tr '[:upper:]' '[:lower:]')

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🚀 Starting 11:05 PM Advance Pre-Crawl on $HOSTNAME_LOWER..."

if [[ "$HOSTNAME_LOWER" == *"5080"* ]]; then
    echo "▶ Triggering Sheet 1 (port 8769) and Sheet 2 (port 8770)..."
    curl -s -X POST -H 'Authorization: Bearer Tracxn@SR' -H 'Content-Type: application/json' http://127.0.0.1:8769/discovery/advance-precrawl -d '{}' || true
    echo ""
    curl -s -X POST -H 'Authorization: Bearer Tracxn@SR' -H 'Content-Type: application/json' http://127.0.0.1:8770/discovery/advance-precrawl -d '{}' || true
    echo ""
elif [[ "$HOSTNAME_LOWER" == *"5321"* ]]; then
    echo "▶ Triggering Sheet 3 (port 8771) and Sheet 4 (port 8772)..."
    curl -s -X POST -H 'Authorization: Bearer Tracxn@SR' -H 'Content-Type: application/json' http://127.0.0.1:8771/discovery/advance-precrawl -d '{}' || true
    echo ""
    curl -s -X POST -H 'Authorization: Bearer Tracxn@SR' -H 'Content-Type: application/json' http://127.0.0.1:8772/discovery/advance-precrawl -d '{}' || true
    echo ""
else
    echo "▶ Testing all local endpoints (8769-8772)..."
    for port in 8769 8770 8771 8772; do
        curl -s -X POST -H 'Authorization: Bearer Tracxn@SR' -H 'Content-Type: application/json' http://127.0.0.1:$port/discovery/advance-precrawl -d '{}' 2>/dev/null || true
    done
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Advance Pre-Crawl dispatch complete."
