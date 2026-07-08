#!/bin/bash
# CML project build script — runs once on project creation.
# Installs Python deps + builds the React frontend.
set -e

echo "=== Cloudera AI Agents — CML Build ==="

# ── Python dependencies ──────────────────────────────────────────────────────
echo "[1/3] Installing Python dependencies..."
pip install --upgrade pip --quiet
pip install -r 02_backend/requirements.txt --quiet
echo "      Python deps installed."

# ── Node.js / npm ────────────────────────────────────────────────────────────
echo "[2/3] Installing Node.js dependencies..."
cd 03_frontend
npm install --silent
echo "      npm install done."

# ── Frontend build ────────────────────────────────────────────────────────────
echo "[3/3] Building React frontend..."
npm run build
echo "      Frontend built → 03_frontend/dist/"
cd ..

echo ""
echo "=== Build complete. Launch the 'Cloudera AI Agents' Application to start. ==="
