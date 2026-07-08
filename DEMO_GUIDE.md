# 🎬 Cloudera AI Agents — Demo Guide

Complete guide for showcasing the entire workflow end-to-end.

---

## 📂 Demo Files Created

| File | Purpose |
|------|---------|
| **DEMO_WORKFLOW.md** | Full 15-20 min guided walkthrough (start here!) |
| **QUICK_START_DEMO.sh** | One-command setup for all dependencies |
| **demo_script.py** | Offline mock demo + API examples |
| **README.md** | Architecture & agent reference |

---

## ⚡ Quick Start (5 minutes)

```bash
# 1. Setup environment and install deps
bash QUICK_START_DEMO.sh

# 2. Start the application
python launch.py

# 3. Open browser
# http://localhost:5173

# 4. Click ORCHESTRATOR in left sidebar (primary entry point)

# 5. Follow DEMO_WORKFLOW.md for guided tour
```

---

## 🎯 Demo at a Glance

**Duration**: 15-20 minutes  
**Primary Entry Point**: 🎯 **ORCHESTRATOR** (coordinates all agents)

**6 Specialist Agents Demonstrated**:
1. ✅ **Source Scout** (ReAct) — Discover data assets
2. ✅ **Semantic Mapper** (Intelligence) — Map fields, detect conflicts
3. ✅ **Metadata Curator** (Policy Engine) — Classify sensitivity & PII
4. ✅ **Quality Guardian** (Evaluator) — Validate data quality
5. ✅ **Pipeline Builder** (Tool-Use) — Generate Flink/NiFi/Kafka Connect
6. ✅ **Pipeline Healer** (Reactive FSM) — Monitor & auto-heal

---

## 📺 Live Demo Walkthrough

### STEP 0: Orchestrator (2–3 min) — START HERE! 🎯
**UI Path**: Left sidebar → Orchestrator (⚡ icon)

```
Paste task: "I have 3 new data sources (Kafka topics). 
             Discover them, classify sensitivity, validate quality, 
             build pipeline, and monitor."

↓
Watch Orchestrator coordinate:
  [1/5] Source Scout       → Discover 3 topics
  [2/5] Metadata Curator   → Classify as RESTRICTED
  [3/5] Quality Guardian   → Score 88/100
  [4/5] Pipeline Builder   → Generate Flink SQL
  [5/5] Pipeline Healer    → Start monitoring

Show: 
- Agent Coordination DAG (visual flow)
- Data flowing between agents
- Metrics: Cost, latency, success rate
- Decision log (every action timestamped)
```

**Key talking point**:
> "One command orchestrates 5 agents end-to-end. No manual handoff. Full transparency."

---

### STEP 1: Source Scout (3–4 min)
**UI Path**: Left sidebar → Source Scout

```
Type: "Find all revenue-related tables and topics"
↓
Watch ReAct loop:
  1. Thought: "I need to search the catalog"
  2. Action: Query Iceberg + Kafka Schema Registry
  3. Observation: Returns 3 tables + 2 topics
  4. Thought: "Let me profile the top results"
  5. Action: Inspect schemas & metadata
  6. Final: Ranked list with quality scores

Show: 
- Transparency panel (System Prompt + LLM Log)
- Click table → 3-tab detail (Schema | Lineage | Recommendations)
```

**Key talking point**:
> "ReAct agents provide full chain-of-thought reasoning. Every step is auditable for compliance."

---

### STEP 2: Semantic Mapper (2–3 min)
**UI Path**: Left sidebar → Semantic Mapper (or click from Source Scout result)

```
Select a table from Source Scout results
↓
Agent auto-runs:
  - Generates embeddings for all field names
  - Finds similar fields across catalog
  - Flags conflicts: customer_id vs cust_id vs customer_uuid

Show:
- Similarity matrix (visual heatmap)
- Confidence scores (0–100%)
- Rename recommendations
```

**Key talking point**:
> "Semantic mapping prevents data silos. We detect naming conflicts before they become data quality issues."

---

### STEP 3: Metadata Curator (3–4 min)
**UI Path**: Left sidebar → Metadata Curator

```
Select same table
↓
See two classification approaches side-by-side:

LEFT (Hierarchical):
  - 1 system prompt
  - 1 LLM call
  - Result: RESTRICTED (because PII detected)
  - Fast: <2 seconds

RIGHT (ReAct):
  - 3 specialized prompts (Field Analysis → PII → Classification)
  - 4+ LLM calls with step-by-step reasoning
  - Final: RESTRICTED + owner + policy recommendations
  - Transparent: Full audit trail

Show:
- 📋 Prompt Comparison panel (toggle both prompts)
- LLM Call Log (every Ollama request/response at bottom)
- Confidence per field
```

**Key talking point**:
> "Two approaches, same result. Hierarchical is 10x faster for bulk classification. ReAct gives detailed reasoning for compliance audits."

---

### STEP 4: Quality Guardian (2–3 min)
**UI Path**: Left sidebar → Quality Guardian

```
Select same table
↓
Click "Generate Quality Checks"
  - Volume: 15M rows (INFO)
  - Completeness: 99.2% (PASS)
  - Uniqueness: 96.8% (WARN: duplicates in customer_id)
  - Freshness: 30 min old (PASS)

Overall Score: 88/100

Show:
- Each check shows generated SQL (click to expand)
- Why it failed/passed
- Auto-remediation suggestions
- Scoring formula (at bottom)
```

**Key talking point**:
> "Quality Guardian generates production SQL automatically. No manual query writing. Scores are consistent and reproducible."

---

### STEP 5: Pipeline Builder (3–4 min)
**UI Path**: Left sidebar → Pipeline Builder

```
Paste task: "Create Kafka → Iceberg → Gold table pipeline"
↓
Agent generates 3 options:

Option A: Flink SQL (real-time streaming)
  - Full SQL code
  - Watermarks + windowing configured
  - Click Deploy (demo: logs to console)

Option B: NiFi Flow (enterprise data integration)
Option C: Kafka Connect (simple replication)

Show:
- Generated code (fully production-ready)
- Architecture diagram
- Lineage (upstream topic → downstream table)
- Validation checks (schema compatible? watermarks OK?)
```

**Key talking point**:
> "Tool-Use patterns generate deterministic code. We use templates + LLM for validation only. 10x cheaper than ReAct."

---

### STEP 6: Pipeline Healer (2–3 min)
**UI Path**: Left sidebar → Pipeline Healer

```
Select pipeline from Step 5
↓
Healer monitors in real-time:
  ✅ Kafka lag: 250 messages (OK)
  ⚠️  Write latency: 850ms p75 (WARNING)
  ✅ Error rate: 0.02% (OK)

Auto-remediation:
  [14:35] → REDUCE_BATCH_SIZE (SUCCESS)
  [14:36] → NOTIFY_SLACK (RETRY queued)

Show:
- State machine diagram (RUNNING → DEGRADED → ESCALATED)
- Real-time metrics
- Action log with timestamps
- Retry queue (exponential backoff)
```

**Key talking point**:
> "Reactive agents don't need LLM calls — they're pure state machines. Perfect for 24/7 monitoring without token costs."

---

### STEP 7: Orchestrator (2–3 min)
**UI Path**: Left sidebar → Orchestrator

```
Paste high-level goal:
  "I have 3 new data sources. Discover, classify, 
   validate quality, build pipeline, monitor."

↓
Supervisor orchestrates:
  [1/5] Source Scout       → Discover 3 topics
  [2/5] Metadata Curator   → Classify as RESTRICTED
  [3/5] Quality Guardian   → Score 88/100
  [4/5] Pipeline Builder   → Generate Flink SQL
  [5/5] Pipeline Healer    → Start monitoring

Show:
- DAG of agent coordination
- Data flowing between agents
- Metrics:
  • Total latency: 12.4 sec
  • LLM calls: 8
  • Tokens: 4,521
  • Cost: ~$0.12
```

**Key talking point**:
> "One command triggers 5 agents in sequence. No manual handoff. Full cost visibility and audit trail."

---

## 🎤 Talking Points by Audience

### For Data Engineers
```
"These agents generate production-grade Flink/NiFi code instantly.
 No boilerplate, no trial-and-error. Just describe your goal."
```

### For Data Stewards / Governance
```
"Metadata Curator auto-detects PII and sensitivity levels.
 Every decision is logged and auditable. Export for compliance."
```

### For Ops/SRE
```
"Pipeline Healer monitors 24/7. Auto-heals common issues.
 Failed external calls (Slack, PagerDuty) are retried with backoff."
```

### For Data Science
```
"Source Scout finds your best datasets with full lineage.
 Semantic Mapper prevents naming conflicts. Quality Guardian validates."
```

### For Compliance/Legal
```
"Every LLM call is logged verbatim. Decision records include
 system prompt, reasoning chain, and confidence scores.
 Export decision logs for audit trails."
```

---

## 🔧 Offline Demo (if live services unavailable)

```bash
python demo_script.py
```

Shows:
- Mock responses from all 6 agents
- Complete workflow simulation
- No dependencies on Ollama/Iceberg/Kafka

---

## 📖 Offline API Examples

```bash
python demo_script.py --api
```

Shows:
- `GET /api/agents` — List configured agents
- `POST /api/agents/{agent}/run` — Run agent with prompt
- `GET /api/system/knox-status` — Check Iceberg connectivity
- Full curl examples

---

## 🎥 Troubleshooting

### Issue: Ollama not running
```bash
# Install Ollama (macOS):
brew install ollama

# Start Ollama in another terminal:
ollama serve

# Pull a model:
ollama pull llama3.2:latest
```

### Issue: Ports already in use
```bash
# Check what's running:
lsof -i :8000   # Backend
lsof -i :5173   # Frontend
lsof -i :11434  # Ollama

# Kill if needed:
kill -9 <PID>
```

### Issue: Frontend dev server slow
Pre-build React for production:
```bash
cd 03_frontend
npm run build
# Frontend served from dist/ via FastAPI
```

### Issue: Can't connect to Iceberg/Kafka
Edit `.env`:
```bash
ICEBERG_CATALOG_TYPE=hadoop
ICEBERG_WAREHOUSE=/tmp/iceberg-warehouse

# Create dummy warehouse if needed:
mkdir -p /tmp/iceberg-warehouse
```

---

## ✅ Pre-Demo Checklist

- [ ] Clone repo and `cd cloudera-ai-agents`
- [ ] Run `bash QUICK_START_DEMO.sh` (5 min setup)
- [ ] Test with `python demo_script.py` (2 min)
- [ ] Start app: `python launch.py`
- [ ] Open http://localhost:5173 in browser
- [ ] Verify all 6 agent icons in left sidebar are clickable
- [ ] Click each agent once to warm up caches
- [ ] Read DEMO_WORKFLOW.md one more time
- [ ] Open browser DevTools (F12) to show network requests if asked
- [ ] Have `.env` file handy to show configuration

---

## 🎬 Demo Timing

| Step | Duration | Agent |
|------|----------|-------|
| **0** | **2–3 min** | **Orchestrator (START HERE)** |
| 1 | 3–4 min | Source Scout |
| 2 | 2–3 min | Semantic Mapper |
| 3 | 3–4 min | Metadata Curator |
| 4 | 2–3 min | Quality Guardian |
| 5 | 3–4 min | Pipeline Builder |
| 6 | 2–3 min | Pipeline Healer |
| **Total** | **18–27 min** | Orchestrator + 6 Specialist Agents |

**Recommended flow**: Start with Orchestrator (2-3 min) to show end-to-end coordination, then optionally drill into each specialist agent for 3-4 min technical details.

(Can be shortened to 5 min if you only show Orchestrator; can be extended to 30+ min if doing deep code walkthroughs of each agent)

---

## 📚 Post-Demo: Deep Dives

If audience wants technical details, show these files:

```
02_backend/agents/source_scout/react_agent.py
  → ReAct loop implementation

02_backend/agents/metadata_curator/
  → Hierarchical vs ReAct comparison

02_backend/agents/quality_guardian/agent.py
  → Quality scoring rules

02_backend/agents/pipeline_builder/agent.py
  → Template-based code generation

02_backend/decision_store/logger.py
  → Compliance logging mechanism

03_frontend/components/AgentPanel.tsx
  → Transparency UI implementation
```

---

## 🚀 Next Steps After Demo

1. **Customize Prompts**: Edit system prompts per agent in UI
2. **Add Mock Data**: Use `scripts_demo/` folder
3. **Deploy to Cloudera AI (CML)**: Push to cluster for prod demo
4. **Export Decision Logs**: For compliance/audit
5. **Tune Agent Thresholds**: For your specific data

---

## 📞 Questions During Demo?

**Q: Why 6 agents and not just one big LLM?**
```
A: Specialized prompts + patterns are cheaper and more reliable.
   ReAct for exploration (high reasoning cost).
   Tool-Use for templates (low cost).
   Evaluators + FSMs never use LLM (real-time).
   This gives you 90% of the value at 1/10 the cost.
```

**Q: How much does this cost to run?**
```
A: Depends on LLM. With Ollama (local): $0 for models.
   With Claude API: ~$0.10–0.50 per workflow.
   With Cloudera AI Inference: Enterprise pricing.
   Decision logs let you track cost per agent.
```

**Q: Can we use different LLMs?**
```
A: Yes! Check config.py. Supports:
   - Ollama (local)
   - Cloudera AI Inference (via Gateway/Knox)
   - Claude API, OpenAI GPT, others (easily added)
```

**Q: How do we handle PII?**
```
A: Metadata Curator detects and classifies.
   Config supports encryption at rest + column-level access control.
   All decisions are logged for audit compliance.
```

---

## 📞 Support

- **Bugs/Issues**: Create GitHub issue
- **Feature Requests**: Submit PR with example
- **Questions**: Check README.md or IMPLEMENTATION_SUMMARY.md
