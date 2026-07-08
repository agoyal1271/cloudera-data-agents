# Cloudera AI Agents — Live Demo Workflow

## Overview
This demo showcases all **6 autonomous agents** operating in sequence over Iceberg, Kafka, and Ozone. Each agent demonstrates a different agentic pattern with full transparency into LLM calls and decisions.

**Duration**: ~15–20 minutes  
**Prerequisites**: Ollama running on `:11434`, `.env` configured with Iceberg/Kafka endpoints

---

## Demo Flow

### 1️⃣ **Source Scout** (ReAct Agent) — 3–4 min
**Goal**: Auto-discover and profile data assets  
**Pattern**: Multi-step reasoning with exploratory tool-use

#### Steps:
1. Open browser: http://localhost:5173
2. Click **Source Scout** in left sidebar
3. Type discovery query: `"Find all revenue-related tables and topics"`
4. Watch the ReAct loop unfold:
   - **Thought 1**: "I need to search for revenue tables"
   - **Action**: Query Iceberg catalog + Kafka Schema Registry
   - **Observation**: Returns list of tables with schemas
   - **Thought 2**: "Let me profile top 3 for data quality"
   - **Action**: Execute schema inspection
   - **Observation**: Returns field-level metadata
   - **Final Answer**: Ranked list with profiles

#### What to show:
- **Transparency Panel**: Click "📋 System Prompt" to see full ReAct chain-of-thought
- **Reasoning Viz**: Hover over each step to see LLM reasoning
- **Metadata**: Click table name → see 3-tab detail (Schema | Lineage | Recommendations)

---

### 2️⃣ **Semantic Mapper** (Intelligence Agent) — 2–3 min
**Goal**: Map fields, detect naming conflicts  
**Pattern**: Embeddings + semantic similarity

#### Steps:
1. From Source Scout results, click one of the discovered tables
2. Click **Semantic Mapper** tab in detail pane (or main nav)
3. It auto-runs on the selected table:
   - Generates embeddings for all field names
   - Finds similar fields across the catalog
   - Flags potential naming conflicts (e.g., `customer_id` vs `cust_id`)

#### What to show:
- **Similarity Matrix**: Visual heatmap of conflicting fields
- **Confidence Scores**: Each mapping shows 0–100% confidence
- **Recommendations**: "Rename X to Y for consistency"

---

### 3️⃣ **Metadata Curator** (Policy Engine + ReAct) — 3–4 min
**Goal**: Classify sensitivity, detect PII, assign owners  
**Pattern**: Hierarchical (fast 1 call) vs. ReAct (detailed 4+ calls)

#### Steps:
1. Click **Metadata Curator** in left nav
2. Select a table from previous discovery
3. Choose comparison mode (top of panel):
   - **Hierarchical** (left pane): Single system prompt, 1 LLM call → fast classification
   - **ReAct** (right pane): 3-step pipeline (Field Analysis → PII Detection → Classification) → transparent reasoning

#### What to show:
- **Hierarchical Result**: `RESTRICTED` (shows reasoning in 1 prompt)
- **ReAct Results**:
  - Field-by-field PII detection (confidence per field)
  - Aggregated sensitivity score
  - Recommended data owner + compliance policy
- **📋 Prompt Comparison**: Toggle between both system prompts side-by-side
- **LLM Call Log**: Bottom panel shows each Ollama call verbatim

#### Key insight:
> "Both classify the same table, but ReAct gives step-by-step reasoning for audit compliance, while Hierarchical scales to 100+ tables instantly."

---

### 4️⃣ **Quality Guardian** (Evaluator Agent) — 2–3 min
**Goal**: Real-time data quality scoring (completeness, uniqueness, freshness)  
**Pattern**: Rule-based evaluation + remediation suggestions

#### Steps:
1. Click **Quality Guardian** in main nav
2. Select the same table
3. Click **Generate Quality Checks**:
   - **Volume**: Row count (info-only)
   - **Completeness**: % null values per column (Pass/Warn/Fail)
   - **Uniqueness**: % duplicates in PK columns
   - **Freshness**: Last updated timestamp

#### What to show:
- **Scoring UI**: Pass (green) / Warn (amber) / Fail (red) breakdown
- **Generated SQL**: Click each check → see the exact Impala/Trino/Spark SQL generated
- **Remediation**: Auto-suggests fixes (e.g., "Add NOT NULL constraint to email_id")
- **Historical Trend**: If available, show how scores trended over time

#### Quality math:
- Overall Score = avg(non-info checks) × 100
- Pass = <5% null | Warn = 5–20% | Fail = ≥20%

---

### 5️⃣ **Pipeline Builder** (Tool-Use Agent) — 3–4 min
**Goal**: Auto-generate data integration pipelines  
**Pattern**: Deterministic tool-use (fast, low LLM cost)

#### Steps:
1. Click **Pipeline Builder** in main nav
2. Describe pipeline task: `"Create a Kafka → Iceberg → Gold table pipeline for real-time revenue processing"`
3. Agent generates 3 options:
   - **Option A**: Flink SQL (stream processing)
   - **Option B**: NiFi flow (enterprise integration)
   - **Option C**: Kafka Connect (simple replication)
4. Select Option A → shows Flink SQL code
5. Click **Deploy** (demo mode: logs to console, doesn't actually deploy)

#### What to show:
- **Generated Code**: Full Flink SQL with schema mapping, watermarks, windowing
- **Architecture Diagram**: Shows Kafka topic → Flink → Iceberg table
- **Lineage**: Click to see upstream (source topic) and downstream (gold table)
- **Validation**: "✅ Schema is compatible | ✅ Watermarking set to 60s | ✅ Iceberg table can be created"

#### Key insight:
> "Tool-Use patterns are 10–20x cheaper than ReAct. We generate deterministic code for templates."

---

### 6️⃣ **Pipeline Healer** (Reactive/FSM Agent) — 2–3 min
**Goal**: Monitor & auto-heal pipeline failures  
**Pattern**: State machine with escalation

#### Steps:
1. Click **Pipeline Healer** in left nav
2. Select the pipeline created above
3. Healer starts monitoring:
   - **Initial State**: `RUNNING`
   - **Check 1**: Kafka topic lag → ✅ OK
   - **Check 2**: Iceberg table write latency → ⚠️ WARNING (75th percentile slow)
   - **Action**: Auto-reduce batch size, retry
   - **Check 3**: Still slow? → Escalate to PagerDuty

#### What to show:
- **State Machine Diagram**: RUNNING → DEGRADED → ESCALATED (with transitions)
- **Metrics Panel**: Real-time lag, throughput, error rate
- **Action Log**: Every auto-heal attempt timestamped + action taken
- **Retry Queue**: Failed actions (e.g., Slack notify) queued with exponential backoff

#### Key insight:
> "Reactive agents don't need LLM calls — they're rule-driven FSMs. Perfect for 24/7 monitoring."

---

### 7️⃣ **Orchestrator** (Supervisor) — 2–3 min
**Goal**: Coordinate all 6 agents into a single workflow  
**Pattern**: LangGraph supervisor with tool delegation

#### Steps:
1. Click **Orchestrator** in left nav
2. Paste a high-level goal:
   ```
   "I just got 3 new data sources (Kafka topics). 
    Discover them, classify sensitivity, ensure quality, 
    build an ingestion pipeline, and monitor it."
   ```
3. Watch the Supervisor orchestrate:
   - **Step 1**: Delegate to Source Scout → discover the 3 topics
   - **Step 2**: Hand results to Metadata Curator → classify as `RESTRICTED` (PII detected)
   - **Step 3**: Forward to Quality Guardian → validate data quality
   - **Step 4**: Route to Pipeline Builder → generate ingest pipeline
   - **Step 5**: Activate Pipeline Healer → start monitoring

#### What to show:
- **Agent Coordination Graph**: Visual DAG of which agent called which
- **Data Flow**: Highlight how Source Scout output feeds into Metadata Curator input
- **Metrics**: Total cost (LLM tokens), total latency, success rate per agent
- **Decision Log**: Every decision timestamped + JSON record for compliance

#### Key insight:
> "One user command orchestrates 5 agents in parallel/sequence. No manual handoff."

---

## Visual Artifacts to Show

### UI Tour
1. **Left Sidebar**: 6 agent icons + Orchestrator + Dashboard
2. **Main Panel**: Agent-specific UI
3. **Transparency Panel** (top-right):
   - System Prompt (editable)
   - LLM Call Log (every Ollama request/response)
   - Decision Record (JSON export for compliance)
4. **Metrics** (bottom-right):
   - LLM tokens used
   - Latency per step
   - Confidence scores

### Code Artifacts
- **System Prompts**: Display side-by-side (Hierarchical vs. ReAct for Curator)
- **Generated Code**: Show Flink SQL, NiFi XML, Kafka Connect JSON
- **Decision Logs**: `decisions/metadata_curator/2024-11-22_14-32-50__classification.md`

---

## Demo Talking Points

| Agent | Pattern | Cost | Speed | When to use |
|-------|---------|------|-------|-------------|
| **Source Scout** | ReAct | High | Slow | Discovery, exploratory analysis |
| **Pipeline Builder** | Tool-Use | Low | Fast | Deterministic generation (NiFi, Flink) |
| **Quality Guardian** | Evaluator | None | RT | Scoring, no LLM needed |
| **Pipeline Healer** | Reactive FSM | None | RT | 24/7 monitoring, auto-remediation |
| **Semantic Mapper** | Intelligence | Med | Med | Field deduplication, conflict detection |
| **Metadata Curator** | Policy Engine | Low | Fast | Compliance, sensitivity classification |

---

## Talking Points for Stakeholders

### For Data Engineers
> "These agents generate production-grade Flink/NiFi code instantly. No boilerplate, just describe your goal."

### For Data Stewards
> "Metadata Curator auto-detects PII and assigns sensitivity levels. Every decision is auditable."

### For Ops/SRE
> "Pipeline Healer monitors 24/7 and auto-heals. Failed external calls (Slack, PagerDuty) are retried with backoff."

### For Data Science
> "Source Scout + Semantic Mapper find your best datasets. Lineage tracing prevents data leaks."

### For Compliance
> "Every LLM call is logged. Decision records include system prompt, reasoning, and confidence. Export for audit trails."

---

## Fallback / Demo Failure Scenarios

### If Ollama is down
- Show pre-recorded screenshots of LLM calls
- Explain: "Ollama would run locally, but API calls are identical on cloud"

### If Iceberg/Kafka unavailable
- Use **mock data**:
  ```bash
  curl -X POST http://localhost:8000/api/agents/demo-data/ingest
  ```
  Loads sample tables + topics

### If Vite dev server is slow
- Pre-build frontend: `npm run build` → serve `dist/` with FastAPI
- Explain: "On CML, this runs on prod machine with pre-built React"

---

## Quick Start (for you)

```bash
# 1. Setup environment
cp .env.example .env
# Edit .env with your Iceberg/Kafka/Ollama endpoints

# 2. Start Ollama (if local)
ollama pull llama3.2:latest
ollama serve

# 3. Launch app (new terminal)
python launch.py
# Opens http://localhost:5173 automatically

# 4. Test connectivity
curl http://localhost:8000/api/system/knox-status
# Should return Knox config + Iceberg catalog info

# 5. Open frontend
# Click through each agent in sidebar
# Follow demo flow above
```

---

## Post-Demo: Show Code

Open these files for technical deep-dive:

```
02_backend/
├── agents/
│   ├── source_scout/react_agent.py          (ReAct loop)
│   ├── pipeline_builder/agent.py            (Tool-use templates)
│   ├── quality_guardian/agent.py            (Evaluator rules)
│   ├── pipeline_healer/agent.py             (FSM states)
│   ├── semantic_mapper/agent.py             (Embeddings)
│   └── metadata_curator/
│       ├── agent.py                         (ReAct pipeline)
│       └── hierarchical_supervisor.py       (1-prompt classification)
├── decision_store/logger.py                 (Compliance logging)
└── routers/agents.py                        (API surface)

03_frontend/
├── components/
│   ├── AgentPanel.tsx                       (Transparency view)
│   ├── SourceScout/SourceScout.tsx          (Discovery UI)
│   └── Orchestrator/Orchestrator.tsx        (Workflow DAG)
```

Each file is ~200–300 lines, heavily commented for walkthrough.

---

## Next Steps After Demo

1. **Customize Prompts**: Edit system prompts per agent in UI (top-right panel)
2. **Tune Thresholds**: Quality Guardian scoring → edit config
3. **Add Connectors**: NiFi processors, Flink UDFs → modify code
4. **Deploy to Prod**: Push to Cloudera AI (CML) on your cluster
5. **Monitor Agents**: Export decision logs to compliance system
