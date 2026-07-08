#!/bin/bash
# Quick start script for Cloudera AI Agents demo
# Run this to get everything set up quickly

set -e

echo "🚀 Cloudera AI Agents — Quick Start Demo"
echo "=========================================="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check prerequisites
echo "✓ Checking prerequisites..."

if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.9+"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "❌ npm not found. Please install Node.js + npm"
    exit 1
fi

echo "✓ Python and npm found"

# Setup environment
echo ""
echo "📋 Setting up environment..."

if [ ! -f ".env" ]; then
    echo "   Creating .env from template..."
    cp .env.example .env
    echo "   ⚠️  Edit .env with your Iceberg/Kafka/Ollama endpoints"
else
    echo "   .env already exists"
fi

# Install backend dependencies
echo ""
echo "📦 Installing backend dependencies..."
cd 02_backend
if [ ! -d "venv" ]; then
    python3 -m venv venv
    source venv/bin/activate
    pip install -q -r requirements.txt
else
    echo "   venv already exists"
fi
cd ..

# Install frontend dependencies
echo ""
echo "📦 Installing frontend dependencies..."
cd 03_frontend
if [ ! -d "node_modules" ]; then
    npm install --quiet
else
    echo "   node_modules already exists"
fi
cd ..

echo ""
echo "✅ Setup complete!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📺 NEXT STEPS:"
echo ""
echo "1️⃣  START THE APP:"
echo "   python launch.py"
echo ""
echo "2️⃣  OPEN IN BROWSER:"
echo "   http://localhost:5173"
echo ""
echo "3️⃣  WALK THROUGH DEMO:"
echo "   Click agents in sidebar (left nav)"
echo "   Follow DEMO_WORKFLOW.md for guided tour"
echo ""
echo "4️⃣  VIEW MOCK DATA (offline):"
echo "   python demo_script.py"
echo ""
echo "5️⃣  VIEW API EXAMPLES:"
echo "   python demo_script.py --api"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📚 DOCUMENTATION:"
echo "   • README.md          — Architecture & agent patterns"
echo "   • DEMO_WORKFLOW.md   — Full demo walkthrough (15-20 min)"
echo "   • demo_script.py     — Programmatic API examples"
echo ""
echo "🎯 KEY ENDPOINTS:"
echo "   API:  http://localhost:8000"
echo "   UI:   http://localhost:5173"
echo "   Docs: http://localhost:8000/docs"
echo ""
