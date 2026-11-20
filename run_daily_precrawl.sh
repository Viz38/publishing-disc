#!/bin/bash
# ==========================================================
# 🚀 Discovery Engine - Daily Advance Pre-Crawl Runner
# Triggered at 12:15 AM IST (15 0 * * *)
# ==========================================================

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$BASE_DIR/.env" ]; then
    export $(grep -v '^#' "$BASE_DIR/.env" | xargs)
fi

HOSTNAME_LOWER=$(hostname 2>/dev/null | tr '[:upper:]' '[:lower:]')

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🚀 Starting 12:15 AM Advance Pre-Crawl on $HOSTNAME_LOWER..."

if [[ "$HOSTNAME_LOWER" == *"508"* ]]; then
    echo "▶ Triggering Fleet Orchestrated Pre-Crawl on Primary Host (Desktop-5080 via Sheet 1 :8769)..."
    curl -s -X POST -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" -H 'Content-Type: application/json' http://127.0.0.1:8769/fleet/orchestrate-precrawl -d '{}' || true
    echo ""
elif [[ "$HOSTNAME_LOWER" == *"532"* ]]; then
    echo "▶ Desktop-5321 checking primary orchestrator on Desktop-5080..."
    STATUS_5080=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" https://5080-sheet1.rajath.world/discovery/health || echo "000")
    if [ "$STATUS_5080" == "200" ]; then
        echo "✅ Desktop-5080 is active. Primary host is handling fleet pre-crawl orchestration."
    else
        echo "⚠️ Desktop-5080 unreachable (status $STATUS_5080). Triggering fleet pre-crawl fallback on Desktop-5321..."
        curl -s -X POST -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" -H 'Content-Type: application/json' http://127.0.0.1:8771/fleet/orchestrate-precrawl -d '{}' || true
    fi
    echo ""
elif [[ "$HOSTNAME_LOWER" == *"9048"* || "$HOSTNAME_LOWER" == *"048"* ]]; then
    echo "▶ Desktop-9048 (NIM Branch) triggering local advance pre-crawl on :8769..."
    curl -s -X POST -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" -H 'Content-Type: application/json' http://127.0.0.1:8769/discovery/advance-precrawl -d '{}' || true
    echo ""
else
    echo "▶ Triggering local fleet pre-crawl orchestrator..."
    curl -s -X POST -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" -H 'Content-Type: application/json' http://127.0.0.1:8769/fleet/orchestrate-precrawl -d '{}' 2>/dev/null || \
    python3 -m common.fleet_orchestrator
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Advance Pre-Crawl dispatch complete."

# Secondary Daily Ingestion Safety Net: Ensure local sheet pipelines auto-start if idle
echo "▶ Running Secondary Daily Ingestion Safety Net check..."
if [[ "$HOSTNAME_LOWER" == *"508"* ]]; then
    for port in 8769 8770; do
        st=$(curl -s -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" http://127.0.0.1:$port/discovery/status 2>/dev/null | grep -o '"status":"[^"]*"' | cut -d':' -f2 | tr -d '"')
        if [[ "$st" == "idle" || "$st" == "stopped" || "$st" == "failed" || -z "$st" ]]; then
            echo "▶ Safety Net: Triggering /discovery/clear-and-fetch on port $port (current status=$st)..."
            curl -s -X POST -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" -H 'Content-Type: application/json' http://127.0.0.1:$port/discovery/clear-and-fetch -d '{}' || true
        fi
    done
elif [[ "$HOSTNAME_LOWER" == *"532"* ]]; then
    for port in 8771 8772; do
        st=$(curl -s -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" http://127.0.0.1:$port/discovery/status 2>/dev/null | grep -o '"status":"[^"]*"' | cut -d':' -f2 | tr -d '"')
        if [[ "$st" == "idle" || "$st" == "stopped" || "$st" == "failed" || -z "$st" ]]; then
            echo "▶ Safety Net: Triggering /discovery/clear-and-fetch on port $port (current status=$st)..."
            curl -s -X POST -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" -H 'Content-Type: application/json' http://127.0.0.1:$port/discovery/clear-and-fetch -d '{}' || true
        fi
    done
elif [[ "$HOSTNAME_LOWER" == *"9048"* || "$HOSTNAME_LOWER" == *"048"* ]]; then
    st=$(curl -s -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" http://127.0.0.1:8769/discovery/status 2>/dev/null | grep -o '"status":"[^"]*"' | cut -d':' -f2 | tr -d '"')
    if [[ "$st" == "idle" || "$st" == "stopped" || "$st" == "failed" || -z "$st" ]]; then
        echo "▶ Safety Net: Triggering /discovery/clear-and-fetch on port 8769 (current status=$st)..."
        curl -s -X POST -H "Authorization: Bearer ${SERVICE_AUTH_TOKEN}" -H 'Content-Type: application/json' http://127.0.0.1:8769/discovery/clear-and-fetch -d '{}' || true
    fi
fi

