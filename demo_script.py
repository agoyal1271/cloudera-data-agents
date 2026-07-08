#!/usr/bin/env python3
"""
Interactive demo script for Cloudera AI Agents.
Showcases all 6 agents with mock data and guided workflow.

Run: python demo_script.py
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import AsyncIterator

# Mock agent responses for when real services aren't available
MOCK_RESPONSES = {
    "source_scout": {
        "type": "discovery_complete",
        "content": {
            "tables": [
                {
                    "name": "gold.revenue_daily",
                    "catalog": "iceberg",
                    "rows": 15_000_000,
                    "columns": 12,
                    "size_gb": 42.5,
                    "last_updated": "2024-11-22T14:32:00Z",
                    "schema": [
                        {"name": "date", "type": "DATE"},
                        {"name": "customer_id", "type": "BIGINT"},
                        {"name": "amount", "type": "DECIMAL(18,2)"},
                        {"name": "region", "type": "STRING"},
                    ]
                },
                {
                    "name": "gold.orders",
                    "catalog": "iceberg",
                    "rows": 50_000_000,
                    "columns": 18,
                    "size_gb": 125.3,
                    "last_updated": "2024-11-22T14:00:00Z",
                }
            ],
            "topics": [
                {
                    "name": "revenue-events-v1",
                    "partitions": 8,
                    "consumer_lag": 250,
                    "messages_per_sec": 12000,
                    "schema": "avro"
                }
            ]
        }
    },
    "semantic_mapper": {
        "type": "mapping_complete",
        "content": {
            "conflicts": [
                {
                    "field_group": ["customer_id", "cust_id", "customer_uuid"],
                    "confidence": 0.95,
                    "recommendation": "Standardize to 'customer_id'"
                },
                {
                    "field_group": ["revenue", "amount", "total"],
                    "confidence": 0.87,
                    "recommendation": "Rename 'total' to 'amount' for consistency"
                }
            ]
        }
    },
    "metadata_curator": {
        "hierarchical": {
            "sensitivity": "RESTRICTED",
            "pii_fields": ["customer_id", "email"],
            "confidence": 0.94,
            "reasoning": "Contains customer identifiers and contact info"
        },
        "react": {
            "step_1": {
                "phase": "Field Analysis",
                "analysis": "Analyzed 12 fields for PII indicators",
                "findings": "2 identifying fields detected (customer_id, email)"
            },
            "step_2": {
                "phase": "PII Detection",
                "high_risk": ["customer_id (95% confidence)", "email (98% confidence)"],
                "medium_risk": ["phone_number (72% confidence)"],
            },
            "step_3": {
                "phase": "Classification",
                "sensitivity": "RESTRICTED",
                "owner": "data-governance@company.com",
                "policy": "PII_PROTECTION_V2",
                "recommendation": "Require encryption at rest + column-level access control"
            }
        }
    },
    "quality_guardian": {
        "checks": {
            "volume": {
                "status": "INFO",
                "rows": 15_000_000,
                "message": "15M rows — good volume"
            },
            "completeness": {
                "status": "PASS",
                "null_percentage": 0.8,
                "message": "0.8% null — excellent"
            },
            "uniqueness": {
                "status": "WARN",
                "duplicate_percentage": 3.2,
                "message": "3.2% duplicates in customer_id — investigate PK constraint"
            },
            "freshness": {
                "status": "PASS",
                "age_hours": 0.5,
                "message": "Updated 30 min ago — fresh"
            }
        },
        "overall_score": 88,
        "generated_sql": """
        -- Completeness check
        SELECT
          COUNT(*) as total_rows,
          COUNT(customer_id) as non_null_customer_id,
          ROUND(100 * COUNT(customer_id) / COUNT(*), 2) as completeness_pct
        FROM gold.revenue_daily;
        """
    },
    "pipeline_builder": {
        "type": "pipeline_generated",
        "options": [
            {
                "name": "Flink SQL (Real-time)",
                "code": """
                CREATE TABLE revenue_kafka (
                  event_time TIMESTAMP(3),
                  customer_id BIGINT,
                  amount DECIMAL(18,2),
                  region STRING,
                  WATERMARK FOR event_time AS event_time - INTERVAL '60' SECOND
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'revenue-events-v1',
                  'properties.bootstrap.servers' = 'localhost:9092'
                );

                CREATE TABLE revenue_gold (
                  date_id DATE,
                  customer_id BIGINT,
                  daily_amount DECIMAL(18,2),
                  region STRING,
                  created_at TIMESTAMP(3)
                ) WITH (
                  'connector' = 'iceberg',
                  'catalog-name' = 'iceberg_prod',
                  'database-name' = 'gold',
                  'table-name' = 'revenue_daily'
                );

                INSERT INTO revenue_gold
                SELECT
                  CAST(event_time AS DATE),
                  customer_id,
                  SUM(amount),
                  region,
                  CURRENT_TIMESTAMP
                FROM revenue_kafka
                GROUP BY CAST(event_time AS DATE), customer_id, region;
                """
            },
            {
                "name": "NiFi Flow (Enterprise)",
                "description": "ConsumeKafka → ValidateRecord → PublishIceberg"
            },
            {
                "name": "Kafka Connect (Simple)",
                "description": "Kafka → Iceberg Sink Connector"
            }
        ]
    },
    "pipeline_healer": {
        "monitoring": {
            "status": "RUNNING",
            "checks": [
                {
                    "check": "Kafka Topic Lag",
                    "result": "OK",
                    "value": "250 messages",
                    "threshold": "10000 messages"
                },
                {
                    "check": "Iceberg Write Latency",
                    "result": "WARNING",
                    "value": "850ms (p75)",
                    "threshold": "500ms"
                },
                {
                    "check": "Error Rate (5min window)",
                    "result": "OK",
                    "value": "0.02%",
                    "threshold": "1%"
                }
            ],
            "actions": [
                {
                    "timestamp": "2024-11-22T14:35:00Z",
                    "action": "REDUCE_BATCH_SIZE",
                    "reason": "High write latency detected",
                    "result": "✅ SUCCESS"
                },
                {
                    "timestamp": "2024-11-22T14:36:15Z",
                    "action": "NOTIFY_SLACK",
                    "reason": "Warning threshold exceeded",
                    "result": "⏳ RETRY (queued for next attempt)"
                }
            ]
        }
    },
    "orchestrator": {
        "workflow": {
            "goal": "Discover, classify, validate quality, build pipeline, and monitor 3 new data sources",
            "steps": [
                {
                    "step": 1,
                    "agent": "Source Scout",
                    "status": "✅ COMPLETE",
                    "result": "Discovered 3 Kafka topics + 2 Iceberg tables"
                },
                {
                    "step": 2,
                    "agent": "Metadata Curator",
                    "status": "✅ COMPLETE",
                    "result": "Classified as RESTRICTED (PII detected in 4 fields)"
                },
                {
                    "step": 3,
                    "agent": "Quality Guardian",
                    "status": "✅ COMPLETE",
                    "result": "Quality score: 88/100 (1 warning on uniqueness)"
                },
                {
                    "step": 4,
                    "agent": "Pipeline Builder",
                    "status": "✅ COMPLETE",
                    "result": "Generated Flink SQL + NiFi backup"
                },
                {
                    "step": 5,
                    "agent": "Pipeline Healer",
                    "status": "🔄 MONITORING",
                    "result": "Active monitoring, 1 auto-remediation applied"
                }
            ],
            "metrics": {
                "total_latency_sec": 12.4,
                "llm_calls": 8,
                "tokens_used": 4521,
                "estimated_cost": "$0.12"
            }
        }
    }
}


class MockAgent:
    """Mock agent that simulates LLM responses."""

    def __init__(self, name: str, is_live: bool = False):
        self.name = name
        self.is_live = is_live

    async def run(self, prompt: str) -> AsyncIterator[dict]:
        """Simulate agent execution with streaming responses."""
        print(f"\n🤖 [{self.name}] Running...")
        print(f"   Prompt: {prompt[:60]}...")

        if self.name in MOCK_RESPONSES:
            response = MOCK_RESPONSES[self.name]

            # Emit step-by-step responses
            yield {"type": "started", "agent": self.name, "timestamp": datetime.now().isoformat()}

            await asyncio.sleep(0.5)  # Simulate processing

            yield {
                "type": "thinking",
                "content": f"Processing query for {self.name}...",
                "timestamp": datetime.now().isoformat()
            }

            await asyncio.sleep(1)  # Simulate LLM call

            yield {
                "type": "result",
                "content": response,
                "timestamp": datetime.now().isoformat()
            }
        else:
            yield {"type": "error", "content": f"No mock response for {self.name}"}


async def demo_workflow():
    """Run the full demo workflow."""

    print("\n" + "="*80)
    print("🚀 CLOUDERA AI AGENTS — LIVE DEMO")
    print("="*80)

    print("\n📋 SCENARIO: New data source ingestion + governance + monitoring")
    print("   Goal: Discover, classify, validate, pipeline, and monitor 3 new Kafka topics\n")

    agents = [
        ("Orchestrator", "Supervisor", "Coordinate end-to-end workflow across all agents"),
        ("Source Scout", "ReAct", "Discover revenue-related tables and topics"),
        ("Semantic Mapper", "Intelligence", "Detect field naming conflicts"),
        ("Metadata Curator", "Policy Engine", "Classify sensitivity + detect PII"),
        ("Quality Guardian", "Evaluator", "Validate data quality"),
        ("Pipeline Builder", "Tool-Use", "Generate Flink SQL pipeline"),
        ("Pipeline Healer", "Reactive FSM", "Monitor and auto-heal"),
    ]

    for i, (name, pattern, task) in enumerate(agents, 0):
        print(f"\n{'─'*80}")
        print(f"STEP {i}: {name.upper()} ({pattern})")
        print(f"{'─'*80}")
        print(f"📌 Task: {task}")
        print()

        agent = MockAgent(name)

        async for event in agent.run(task):
            if event["type"] == "result":
                print(f"✅ Result received:")
                result_json = json.dumps(event["content"], indent=2)
                # Print first 500 chars
                preview = result_json[:500]
                if len(result_json) > 500:
                    preview += "\n   ... (truncated)"
                print("\n".join(f"   {line}" for line in preview.split("\n")))
            elif event["type"] == "thinking":
                print(f"💭 {event['content']}")

        # For CI/automated runs, skip input; for interactive use, uncomment:
        # input("\n▶️  Press ENTER to continue to next agent...")
        print("\n" + "─"*40)


    print("\n" + "="*80)
    print("✨ DEMO COMPLETE!")
    print("="*80)
    print("\n📺 To see the live UI:")
    print("   1. Start the app: python launch.py")
    print("   2. Open http://localhost:5173 in browser")
    print("   3. Click through each agent in the sidebar")
    print("\n📖 Full demo walkthrough: See DEMO_WORKFLOW.md")
    print("\n")


async def show_api_examples():
    """Show example API calls for programmatic access."""

    print("\n" + "="*80)
    print("💻 PROGRAMMATIC API EXAMPLES")
    print("="*80)

    examples = [
        {
            "endpoint": "GET /api/agents",
            "description": "List all configured agents",
            "curl": 'curl http://localhost:8000/api/agents | jq .',
            "response": {
                "agents": [
                    {"id": "source_scout", "pattern": "ReAct", "status": "ready"},
                    {"id": "pipeline_builder", "pattern": "Tool-Use", "status": "ready"},
                ]
            }
        },
        {
            "endpoint": "POST /api/agents/{agent}/run",
            "description": "Run an agent with a prompt",
            "curl": '''curl -X POST http://localhost:8000/api/agents/source_scout/run \\
  -H "Content-Type: application/json" \\
  -d '{"prompt": "Find revenue tables"}' \\
  --stream''',
            "response": "Server-Sent Events stream of agent reasoning"
        },
        {
            "endpoint": "GET /api/system/knox-status",
            "description": "Check Iceberg catalog connectivity",
            "curl": "curl http://localhost:8000/api/system/knox-status",
            "response": {
                "status": "connected",
                "catalog": "iceberg",
                "tables": 142,
                "knox_jwt_expires": "2024-11-23T14:32:00Z"
            }
        }
    ]

    for example in examples:
        print(f"\n📍 {example['endpoint']}")
        print(f"   {example['description']}")
        print(f"\n   $ {example['curl']}")
        print(f"\n   Response:")
        if isinstance(example['response'], dict):
            print("\n".join(f"   {line}" for line in json.dumps(example['response'], indent=2).split("\n")))
        else:
            print(f"   {example['response']}")


async def main():
    """Main entry point."""

    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--api":
        await show_api_examples()
    else:
        try:
            await demo_workflow()
        except KeyboardInterrupt:
            print("\n\n👋 Demo interrupted. Goodbye!")
            sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
