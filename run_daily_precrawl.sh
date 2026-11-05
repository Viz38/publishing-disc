#!/bin/bash
# ==========================================================
# 🚀 Discovery Engine - Daily Advance Pre-Crawl Runner
# Triggered at 11:05 PM IST (5 23 * * *)
# ==========================================================

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTNAME_LOWER=$(hostname 2>/dev/null | tr '[:upper:]' '[:lower:]')

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🚀 Starting 11:05 PM Advance Pre-Crawl on $HOSTNAME_LOWER..."

if [[ "$HOSTNAME_LOWER" == *"5080"* ]]; then
    echo "▶ Triggering Fleet Orchestrated Pre-Crawl on Primary Host (Desktop-5080 via Sheet 1 :8769)..."
    curl -s -X POST -H 'Authorization: Bearer REDACTED_SECRET' -H 'Content-Type: application/json' http://127.0.0.1:8769/fleet/orchestrate-precrawl -d '{}' || true
    echo ""
elif [[ "$HOSTNAME_LOWER" == *"5321"* ]]; then
    echo "▶ Desktop-5321 checking primary orchestrator on Desktop-5080..."
    STATUS_5080=$(curl -s -o /dev/null -w "%{http_code}" -H 'Authorization: Bearer REDACTED_SECRET' https://5080-sheet1.rajath.world/discovery/health || echo "000")
    if [ "$STATUS_5080" == "200" ]; then
        echo "✅ Desktop-5080 is active. Primary host is handling fleet pre-crawl orchestration."
    else
        echo "⚠️ Desktop-5080 unreachable (status $STATUS_5080). Triggering fleet pre-crawl fallback on Desktop-5321..."
        curl -s -X POST -H 'Authorization: Bearer REDACTED_SECRET' -H 'Content-Type: application/json' http://127.0.0.1:8771/fleet/orchestrate-precrawl -d '{}' || true
    fi
    echo ""
else
    echo "▶ Triggering local fleet pre-crawl orchestrator..."
    curl -s -X POST -H 'Authorization: Bearer REDACTED_SECRET' -H 'Content-Type: application/json' http://127.0.0.1:8769/fleet/orchestrate-precrawl -d '{}' 2>/dev/null || \
    python3 -m common.fleet_orchestrator
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Advance Pre-Crawl dispatch complete."
