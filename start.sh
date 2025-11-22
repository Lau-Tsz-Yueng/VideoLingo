#!/usr/bin/env bash
set -e

# Dual-mode entrypoint:
# - RUNPOD_MODE=serverless : start handler for Runpod serverless
# - otherwise             : keep container alive for debugging/SSH

if [ "$RUNPOD_MODE" = "serverless" ]; then
  echo "[start] running in serverless mode"
  exec python handler.py
fi

echo "[start] running in dev mode (sleep infinity). Set RUNPOD_MODE=serverless to start handler."
sleep infinity
