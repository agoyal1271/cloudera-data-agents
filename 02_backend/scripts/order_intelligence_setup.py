"""
Order Intelligence Demo — OpenMetadata Setup Script

Registers the full Order Intelligence pipeline in OpenMetadata and builds
a 5-hop lineage chain so the AI agents can traverse it end-to-end:

  [Kafka] order-events                          hop -4  (raw stream, PII)
      ↓  NiFi ReadyFlow: nifi-order-ingest
  [Iceberg] demo.raw_orders                     hop -3  (landing zone, PII, Tier3)
      ↓  SQL Transform 1: enrich_orders_job
  [Iceberg] demo.products           ← side ref  hop -3  (catalogue, Tier3)
  [Iceberg] demo.customers          ← side ref  hop -3  (CRM, PII, Tier2)
      ↓  (all three feed enriched_orders)
  [Iceberg] demo.enriched_orders               hop -2  (joined, PII, Tier2)
      ↓  SQL Transform 2: build_mart_job
  [Iceberg] demo.order_analytics_mart          hop -1  (aggregated, Tier1)
      ↓  Looker connection
  [Dashboard] operations_kpi_dashboard         hop  0  (consumer)

Run from 02_backend/:
    python -m scripts.order_intelligence_setup
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger("order_intelligence_setup")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _step(msg: str):
    logger.info(f"▶  {msg}")


def _ok(msg: str):
    logger.info(f"   ✓  {msg}")


def _skip(msg: str):
    logger.info(f"   –  {msg} (skipped, non-fatal)")


# ── 1. Ensure services exist ──────────────────────────────────────────────────

def setup_services():
    _step("Ensuring OpenMetadata services exist")
    from tools.openmetadata.client import ensure_messaging_service, ensure_database_service

    kafka_svc = ensure_messaging_service("cdp_kafka")
    _ok(f"Kafka messaging service  id={kafka_svc}")

    hive_svc = ensure_database_service("cdp_hive")
    _ok(f"Hive/Iceberg DB service  id={hive_svc}")


# ── 2. Register Kafka topic ───────────────────────────────────────────────────

ORDER_EVENTS_SCHEMA = [
    {"name": "order_id",        "type": "string"},
    {"name": "customer_id",     "type": "string"},
    {"name": "product_id",      "type": "string"},
    {"name": "quantity",        "type": "int"},
    {"name": "unit_price",      "type": "double"},
    {"name": "discount_pct",    "type": ["null", "double"]},
    {"name": "order_status",    "type": "string"},
    {"name": "channel",         "type": "string"},
    {"name": "region",          "type": "string"},
    {"name": "payment_method",  "type": "string"},
    {"name": "is_first_order",  "type": "boolean"},
    {"name": "event_timestamp", "type": "long"},
]


def register_topic():
    _step("Registering Kafka topic: order-events")
    from tools.openmetadata.client import register_topic

    result = register_topic(
        topic_name="order-events",
        schema_fields=ORDER_EVENTS_SCHEMA,
        description=(
            "Real-time e-commerce order lifecycle events. "
            "Carries PII (customer_id) and financial data. "
            "Produced by the storefront service; partitioned 12-way by region hash."
        ),
    )
    if result:
        _ok(f"topic registered  fqn={result.get('fullyQualifiedName')}")
    else:
        _skip("topic may already exist or OM unreachable")
    return result


# ── 3. Register Iceberg tables ────────────────────────────────────────────────

ICEBERG_TABLES = [
    {
        "name": "demo.raw_orders",
        "description": (
            "Append-only landing zone for order-events Kafka topic. "
            "No transformations — exact mirror of Kafka payload plus ingestion metadata. "
            "Contains PII (customer_id, payment_method). Retention: 90 days."
        ),
        "fields": [
            {"name": "order_id",        "type": "string"},
            {"name": "customer_id",     "type": "string"},
            {"name": "product_id",      "type": "string"},
            {"name": "quantity",        "type": "int"},
            {"name": "unit_price",      "type": "double"},
            {"name": "discount_pct",    "type": "double"},
            {"name": "order_status",    "type": "string"},
            {"name": "channel",         "type": "string"},
            {"name": "region",          "type": "string"},
            {"name": "payment_method",  "type": "string"},
            {"name": "is_first_order",  "type": "boolean"},
            {"name": "event_timestamp", "type": "long"},
            {"name": "ingested_at",     "type": "timestamptz"},
            {"name": "kafka_partition", "type": "int"},
            {"name": "kafka_offset",    "type": "long"},
        ],
    },
    {
        "name": "demo.products",
        "description": (
            "Product catalogue reference table. Joined into enriched_orders to add "
            "category, sub-category, cost price, and list price. Updated daily from ERP."
        ),
        "fields": [
            {"name": "product_id",   "type": "string"},
            {"name": "product_name", "type": "string"},
            {"name": "category",     "type": "string"},
            {"name": "sub_category", "type": "string"},
            {"name": "cost_price",   "type": "double"},
            {"name": "list_price",   "type": "double"},
            {"name": "supplier_id",  "type": "string"},
            {"name": "is_active",    "type": "boolean"},
            {"name": "created_at",   "type": "timestamptz"},
        ],
    },
    {
        "name": "demo.enriched_orders",
        "description": (
            "Orders enriched with customer CRM data and product catalogue. "
            "Adds computed revenue columns (gross/net/tax), discount tier classification, "
            "gross margin, and date dimensions. Contains PII inherited from raw_orders."
        ),
        "fields": [
            {"name": "order_id",          "type": "string"},
            {"name": "customer_id",       "type": "string"},
            {"name": "customer_name",     "type": "string"},
            {"name": "customer_email",    "type": "string"},
            {"name": "customer_segment",  "type": "string"},
            {"name": "product_id",        "type": "string"},
            {"name": "product_name",      "type": "string"},
            {"name": "category",          "type": "string"},
            {"name": "sub_category",      "type": "string"},
            {"name": "quantity",          "type": "int"},
            {"name": "unit_price",        "type": "double"},
            {"name": "discount_pct",      "type": "double"},
            {"name": "discount_tier",     "type": "string"},
            {"name": "gross_amount",      "type": "double"},
            {"name": "discount_amount",   "type": "double"},
            {"name": "net_amount",        "type": "double"},
            {"name": "tax_amount",        "type": "double"},
            {"name": "total_amount",      "type": "double"},
            {"name": "cost_of_goods",     "type": "double"},
            {"name": "gross_margin",      "type": "double"},
            {"name": "order_status",      "type": "string"},
            {"name": "channel",           "type": "string"},
            {"name": "region",            "type": "string"},
            {"name": "payment_method",    "type": "string"},
            {"name": "is_first_order",    "type": "boolean"},
            {"name": "order_date",        "type": "date"},
            {"name": "order_hour",        "type": "int"},
            {"name": "order_day_of_week", "type": "int"},
            {"name": "event_timestamp",   "type": "long"},
            {"name": "enriched_at",       "type": "timestamptz"},
        ],
    },
    {
        "name": "demo.order_analytics_mart",
        "description": (
            "Daily × region × category aggregate mart. Computed via windowed SQL from enriched_orders. "
            "Carries running totals, 7-day moving averages, period-over-period deltas, and "
            "region/category revenue ranks. Primary source for Operations KPI dashboard."
        ),
        "fields": [
            {"name": "report_date",           "type": "date"},
            {"name": "region",                "type": "string"},
            {"name": "category",              "type": "string"},
            {"name": "channel",               "type": "string"},
            {"name": "order_count",           "type": "long"},
            {"name": "unique_customers",      "type": "long"},
            {"name": "new_customer_orders",   "type": "long"},
            {"name": "gross_revenue",         "type": "double"},
            {"name": "net_revenue",           "type": "double"},
            {"name": "total_discount",        "type": "double"},
            {"name": "avg_order_value",       "type": "double"},
            {"name": "avg_gross_margin",      "type": "double"},
            {"name": "cumulative_revenue",    "type": "double"},
            {"name": "revenue_7d_moving_avg", "type": "double"},
            {"name": "revenue_vs_prev_day",   "type": "double"},
            {"name": "revenue_vs_prev_week",  "type": "double"},
            {"name": "region_revenue_rank",   "type": "int"},
            {"name": "category_revenue_rank", "type": "int"},
            {"name": "computed_at",           "type": "timestamptz"},
        ],
    },
]


def register_tables():
    _step("Registering Iceberg tables")
    from tools.openmetadata.client import register_table

    registered = {}
    for tbl in ICEBERG_TABLES:
        result = register_table(
            table_name=tbl["name"],
            fields=tbl["fields"],
            description=tbl["description"],
        )
        short = tbl["name"].split(".")[-1]
        if result:
            _ok(f"{tbl['name']}  id={result.get('id')}")
            registered[short] = result
        else:
            _skip(f"{tbl['name']} may already exist")
            registered[short] = {"name": tbl["name"]}
    return registered


# ── 4. Create N-hop lineage chain ─────────────────────────────────────────────
#
# Edge map (source → sink, type, pipeline label):
#
#   order-events (topic)  →  raw_orders (table)      via nifi-order-ingest
#   raw_orders            →  enriched_orders          via enrich-orders-job
#   customers             →  enriched_orders          via enrich-orders-job
#   products              →  enriched_orders          via enrich-orders-job
#   enriched_orders       →  order_analytics_mart     via build-mart-job
#
# This gives order_analytics_mart a 4-hop upstream depth:
#   order-events → raw_orders → enriched_orders → order_analytics_mart

LINEAGE_EDGES = [
    {
        "from_fqn":  "cdp_kafka.order-events",
        "from_type": "topic",
        "to_fqn":    "cdp_hive.demo.default.raw_orders",
        "to_type":   "table",
        "pipeline":  "nifi-order-ingest",
        "label":     "order-events → raw_orders",
    },
    {
        "from_fqn":  "cdp_hive.demo.default.raw_orders",
        "from_type": "table",
        "to_fqn":    "cdp_hive.demo.default.enriched_orders",
        "to_type":   "table",
        "pipeline":  "enrich-orders-job",
        "label":     "raw_orders → enriched_orders",
    },
    {
        "from_fqn":  "cdp_hive.demo.default.customers",
        "from_type": "table",
        "to_fqn":    "cdp_hive.demo.default.enriched_orders",
        "to_type":   "table",
        "pipeline":  "enrich-orders-job",
        "label":     "customers → enriched_orders (dimension join)",
    },
    {
        "from_fqn":  "cdp_hive.demo.default.products",
        "from_type": "table",
        "to_fqn":    "cdp_hive.demo.default.enriched_orders",
        "to_type":   "table",
        "pipeline":  "enrich-orders-job",
        "label":     "products → enriched_orders (dimension join)",
    },
    {
        "from_fqn":  "cdp_hive.demo.default.enriched_orders",
        "from_type": "table",
        "to_fqn":    "cdp_hive.demo.default.order_analytics_mart",
        "to_type":   "table",
        "pipeline":  "build-mart-job",
        "label":     "enriched_orders → order_analytics_mart",
    },
]


def create_lineage():
    _step("Creating lineage edges")
    from tools.openmetadata.client import create_lineage_edge

    for edge in LINEAGE_EDGES:
        result = create_lineage_edge(
            from_fqn=edge["from_fqn"], from_type=edge["from_type"],
            to_fqn=edge["to_fqn"],   to_type=edge["to_type"],
            pipeline_name=edge["pipeline"],
        )
        if result:
            _ok(edge["label"])
        else:
            _skip(edge["label"])


# ── 5. Schema Registry — register Avro schema ────────────────────────────────

ORDER_EVENTS_AVRO = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.cloudera.demo",
    "doc": "Real-time order lifecycle event from the e-commerce storefront",
    "fields": [
        {"name": "order_id",        "type": "string"},
        {"name": "customer_id",     "type": "string"},
        {"name": "product_id",      "type": "string"},
        {"name": "quantity",        "type": "int"},
        {"name": "unit_price",      "type": "double"},
        {"name": "discount_pct",    "type": ["null", "double"], "default": None},
        {"name": "order_status",    "type": "string"},
        {"name": "channel",         "type": "string"},
        {"name": "region",          "type": "string"},
        {"name": "payment_method",  "type": "string"},
        {"name": "is_first_order",  "type": "boolean"},
        {"name": "event_timestamp", "type": "long",   "logicalType": "timestamp-millis"},
    ],
}


def register_schema_registry():
    _step("Registering Avro schema in Schema Registry")
    from config import SCHEMA_REGISTRY_URL
    if not SCHEMA_REGISTRY_URL:
        _skip("SCHEMA_REGISTRY_URL not set")
        return
    try:
        from tools.kafka.schema_registry import register_schema
        schema_id = register_schema(
            name="order-events",
            avro_schema=ORDER_EVENTS_AVRO,
            description="Real-time order lifecycle events — PII (customer_id, payment_method)",
        )
        if schema_id:
            _ok(f"schema registered  name=order-events  id={schema_id}")
        else:
            _ok("schema already exists  name=order-events")
    except Exception as e:
        _skip(f"Schema Registry unreachable: {e}")


# ── 6. Impala — DDL + sample data ─────────────────────────────────────────────

def _impala_conn():
    """Open an Impala connection via Knox. Returns (conn, None) or (None, error_msg)."""
    import os
    from config import KNOX_USERNAME, KNOX_PASSWORD
    knox_host = os.getenv("KNOX_HOST", "cdp-utility.cdp.local")
    try:
        from impala.dbapi import connect
        conn = connect(
            host=knox_host, port=8443, use_http_transport=True,
            http_path="gateway/cdp-proxy-api/impala/",
            auth_mechanism="LDAP", user=KNOX_USERNAME, password=KNOX_PASSWORD,
        )
        return conn, None
    except Exception as e:
        return None, str(e)


def _run_impala(statements: list[str], label: str = ""):
    """Execute a list of SQL statements via Impala. Each statement runs independently."""
    conn, err = _impala_conn()
    if conn is None:
        _skip(f"Impala unavailable ({err}) — skipping {label}")
        return False
    cur = conn.cursor()
    ok_count = 0
    for sql in statements:
        sql = sql.strip().rstrip(";")
        if not sql:
            continue
        try:
            cur.execute(sql)
            ok_count += 1
        except Exception as e:
            _skip(f"  SQL failed: {str(e)[:120]}")
    conn.close()
    _ok(f"{label}: {ok_count}/{len(statements)} statements OK")
    return ok_count == len(statements)


# Sample data designed so the mart aggregates are consistent with the enriched rows.
# Dates span 2026-06-22 to 2026-06-24 across 3 regions and 3 categories.

_PRODUCTS_INSERT = """
INSERT INTO demo.products VALUES
  ('PROD-1101', 'Laptop Pro 15"',       'Electronics', 'Laptops',   650.00, 1299.00, 'SUP-001', TRUE,  CAST('2025-01-10 00:00:00' AS TIMESTAMP)),
  ('PROD-1102', 'Wireless Headphones',  'Electronics', 'Audio',      45.00,  149.00, 'SUP-002', TRUE,  CAST('2025-01-10 00:00:00' AS TIMESTAMP)),
  ('PROD-2201', 'Running Shoes',        'Apparel',     'Footwear',   28.00,   89.00, 'SUP-003', TRUE,  CAST('2025-02-01 00:00:00' AS TIMESTAMP)),
  ('PROD-2202', 'Yoga Mat',             'Sports',      'Fitness',    12.00,   39.00, 'SUP-004', TRUE,  CAST('2025-02-01 00:00:00' AS TIMESTAMP)),
  ('PROD-3301', 'Smart TV 55"',         'Electronics', 'TV',        280.00,  649.00, 'SUP-001', TRUE,  CAST('2025-03-15 00:00:00' AS TIMESTAMP)),
  ('PROD-3302', 'Coffee Maker',         'Home',        'Kitchen',    35.00,   89.00, 'SUP-005', TRUE,  CAST('2025-03-15 00:00:00' AS TIMESTAMP)),
  ('PROD-4401', 'Backpack',             'Apparel',     'Bags',       22.00,   65.00, 'SUP-003', TRUE,  CAST('2025-04-01 00:00:00' AS TIMESTAMP)),
  ('PROD-5501', 'Gaming Console',       'Electronics', 'Gaming',    320.00,  499.00, 'SUP-006', TRUE,  CAST('2025-05-01 00:00:00' AS TIMESTAMP)),
  ('PROD-5502', 'Desk Chair',           'Home',        'Furniture',  95.00,  249.00, 'SUP-007', TRUE,  CAST('2025-05-01 00:00:00' AS TIMESTAMP)),
  ('PROD-7712', 'Bluetooth Speaker',    'Electronics', 'Audio',      28.00,   79.00, 'SUP-002', TRUE,  CAST('2025-06-01 00:00:00' AS TIMESTAMP))
"""

_RAW_ORDERS_INSERT = """
INSERT INTO demo.raw_orders VALUES
  ('ORD-001', 'C-10293', 'PROD-1101', 1, 1299.00, NULL,  'DELIVERED', 'WEB',        'US_EAST', 'CARD',          FALSE, 1750550400000, CAST('2026-06-22 08:01:00' AS TIMESTAMP), 0, 100001),
  ('ORD-002', 'C-30021', 'PROD-1102', 2,  149.00, 0.10,  'DELIVERED', 'MOBILE',     'EU',      'WALLET',         TRUE, 1750553100000, CAST('2026-06-22 08:45:00' AS TIMESTAMP), 1, 100002),
  ('ORD-003', 'C-85441', 'PROD-2201', 1,   89.00, NULL,  'SHIPPED',   'WEB',        'APAC',    'CARD',            TRUE, 1750557600000, CAST('2026-06-22 10:00:00' AS TIMESTAMP), 2, 100003),
  ('ORD-004', 'C-72019', 'PROD-3301', 1,  649.00, 0.15,  'DELIVERED', 'WEB',        'US_WEST', 'BANK_TRANSFER', FALSE, 1750561200000, CAST('2026-06-22 11:00:00' AS TIMESTAMP), 3, 100004),
  ('ORD-005', 'C-10293', 'PROD-5501', 1,  499.00, NULL,  'DELIVERED', 'PARTNER_API','US_EAST', 'CARD',           FALSE, 1750566600000, CAST('2026-06-22 12:30:00' AS TIMESTAMP), 0, 100005),
  ('ORD-006', 'C-30021', 'PROD-3302', 2,   89.00, 0.05,  'CONFIRMED', 'MOBILE',     'EU',      'WALLET',         FALSE, 1750636800000, CAST('2026-06-23 08:00:00' AS TIMESTAMP), 1, 100006),
  ('ORD-007', 'C-85441', 'PROD-2202', 3,   39.00, NULL,  'SHIPPED',   'WEB',        'APAC',    'CARD',           FALSE, 1750640400000, CAST('2026-06-23 09:00:00' AS TIMESTAMP), 2, 100007),
  ('ORD-008', 'C-72019', 'PROD-4401', 2,   65.00, 0.10,  'CONFIRMED', 'MOBILE',     'US_WEST', 'CARD',           FALSE, 1750644000000, CAST('2026-06-23 10:00:00' AS TIMESTAMP), 3, 100008),
  ('ORD-009', 'C-11001', 'PROD-1102', 1,  149.00, NULL,  'DELIVERED', 'WEB',        'US_EAST', 'CARD',            TRUE, 1750648800000, CAST('2026-06-23 11:20:00' AS TIMESTAMP), 0, 100009),
  ('ORD-010', 'C-11002', 'PROD-5502', 1,  249.00, 0.20,  'SHIPPED',   'WEB',        'EU',      'BANK_TRANSFER',   TRUE, 1750654200000, CAST('2026-06-23 12:50:00' AS TIMESTAMP), 1, 100010),
  ('ORD-011', 'C-10293', 'PROD-7712', 2,   79.00, NULL,  'CONFIRMED', 'MOBILE',     'US_EAST', 'WALLET',         FALSE, 1750723200000, CAST('2026-06-24 08:00:00' AS TIMESTAMP), 0, 100011),
  ('ORD-012', 'C-30021', 'PROD-1101', 1, 1299.00, 0.10,  'PENDING',   'WEB',        'EU',      'CARD',           FALSE, 1750726800000, CAST('2026-06-24 09:00:00' AS TIMESTAMP), 1, 100012),
  ('ORD-013', 'C-85441', 'PROD-3301', 1,  649.00, NULL,  'CONFIRMED', 'PARTNER_API','APAC',    'BANK_TRANSFER',  FALSE, 1750730400000, CAST('2026-06-24 10:00:00' AS TIMESTAMP), 2, 100013),
  ('ORD-014', 'C-72019', 'PROD-2201', 2,   89.00, 0.05,  'SHIPPED',   'WEB',        'US_WEST', 'CARD',           FALSE, 1750734000000, CAST('2026-06-24 11:00:00' AS TIMESTAMP), 3, 100014),
  ('ORD-015', 'C-11003', 'PROD-5501', 1,  499.00, 0.25,  'CONFIRMED', 'MOBILE',     'EU',      'WALLET',          TRUE, 1750737600000, CAST('2026-06-24 12:00:00' AS TIMESTAMP), 4, 100015)
"""

_ENRICHED_ORDERS_INSERT = """
INSERT INTO demo.enriched_orders VALUES
  ('ORD-001','C-10293','Alice Johnson','alice.j@example.com','RETURNING','PROD-1101','Laptop Pro 15"','Electronics','Laptops',  1,1299.00,0.00,'NONE',   1299.00,  0.00,1299.00,110.42,1409.42, 650.00,0.499,'DELIVERED','WEB',       'US_EAST','CARD',         FALSE,CAST('2026-06-22' AS DATE),8, 7,1750550400000,CAST('2026-06-22 08:05:00' AS TIMESTAMP)),
  ('ORD-002','C-30021','Bob Martinez', 'bob.m@example.com', 'NEW',      'PROD-1102','Wireless Headphones','Electronics','Audio',2, 149.00,0.10,'SMALL',  298.00, 29.80, 268.20, 53.64, 321.84,  90.00,0.664,'DELIVERED','MOBILE',    'EU',     'WALLET',        TRUE, CAST('2026-06-22' AS DATE),8, 7,1750553100000,CAST('2026-06-22 08:50:00' AS TIMESTAMP)),
  ('ORD-003','C-85441','Chen Wei',     'chen.w@example.com','NEW',      'PROD-2201','Running Shoes',     'Apparel',  'Footwear',1,  89.00,0.00,'NONE',    89.00,  0.00,  89.00,  8.90,  97.90,  28.00,0.685,'SHIPPED',  'WEB',       'APAC',   'CARD',          TRUE, CAST('2026-06-22' AS DATE),10,7,1750557600000,CAST('2026-06-22 10:05:00' AS TIMESTAMP)),
  ('ORD-004','C-72019','Diana Lee',    'diana.l@example.com','RETURNING','PROD-3301','Smart TV 55"',     'Electronics','TV',  1, 649.00,0.15,'MEDIUM', 649.00, 97.35, 551.65, 46.89, 598.54, 280.00,0.492,'DELIVERED','WEB',       'US_WEST','BANK_TRANSFER',FALSE,CAST('2026-06-22' AS DATE),11,7,1750561200000,CAST('2026-06-22 11:05:00' AS TIMESTAMP)),
  ('ORD-005','C-10293','Alice Johnson','alice.j@example.com','RETURNING','PROD-5501','Gaming Console',   'Electronics','Gaming',1,499.00,0.00,'NONE',  499.00,  0.00, 499.00, 42.42, 541.42, 320.00,0.359,'DELIVERED','PARTNER_API','US_EAST','CARD',         FALSE,CAST('2026-06-22' AS DATE),12,7,1750566600000,CAST('2026-06-22 12:35:00' AS TIMESTAMP)),
  ('ORD-006','C-30021','Bob Martinez', 'bob.m@example.com', 'RETURNING','PROD-3302','Coffee Maker',     'Home',     'Kitchen', 2,  89.00,0.05,'SMALL',  178.00,  8.90, 169.10, 33.82, 202.92,  70.00,0.586,'CONFIRMED','MOBILE',    'EU',     'WALLET',        FALSE,CAST('2026-06-23' AS DATE),8, 1,1750636800000,CAST('2026-06-23 08:05:00' AS TIMESTAMP)),
  ('ORD-007','C-85441','Chen Wei',     'chen.w@example.com','RETURNING','PROD-2202','Yoga Mat',         'Sports',   'Fitness', 3,  39.00,0.00,'NONE',  117.00,  0.00, 117.00, 11.70, 128.70,  36.00,0.692,'SHIPPED',  'WEB',       'APAC',   'CARD',          FALSE,CAST('2026-06-23' AS DATE),9, 1,1750640400000,CAST('2026-06-23 09:05:00' AS TIMESTAMP)),
  ('ORD-008','C-72019','Diana Lee',    'diana.l@example.com','RETURNING','PROD-4401','Backpack',         'Apparel',  'Bags',    2,  65.00,0.10,'SMALL',  130.00, 13.00, 117.00,  9.95, 126.95,  44.00,0.624,'CONFIRMED','MOBILE',    'US_WEST','CARD',          FALSE,CAST('2026-06-23' AS DATE),10,1,1750644000000,CAST('2026-06-23 10:05:00' AS TIMESTAMP)),
  ('ORD-009','C-11001','Ethan Park',   'ethan.p@example.com','NEW',     'PROD-1102','Wireless Headphones','Electronics','Audio',1,149.00,0.00,'NONE',  149.00,  0.00, 149.00, 12.67, 161.67,  45.00,0.698,'DELIVERED','WEB',       'US_EAST','CARD',          TRUE, CAST('2026-06-23' AS DATE),11,1,1750648800000,CAST('2026-06-23 11:25:00' AS TIMESTAMP)),
  ('ORD-010','C-11002','Fatima Ali',   'fatima.a@example.com','NEW',    'PROD-5502','Desk Chair',        'Home',     'Furniture',1,249.00,0.20,'MEDIUM',249.00, 49.80, 199.20, 39.84, 239.04,  95.00,0.523,'SHIPPED',  'WEB',       'EU',     'BANK_TRANSFER', TRUE, CAST('2026-06-23' AS DATE),12,1,1750654200000,CAST('2026-06-23 12:55:00' AS TIMESTAMP)),
  ('ORD-011','C-10293','Alice Johnson','alice.j@example.com','RETURNING','PROD-7712','Bluetooth Speaker','Electronics','Audio',2,  79.00,0.00,'NONE',  158.00,  0.00, 158.00, 13.43, 171.43,  56.00,0.646,'CONFIRMED','MOBILE',    'US_EAST','WALLET',        FALSE,CAST('2026-06-24' AS DATE),8, 2,1750723200000,CAST('2026-06-24 08:05:00' AS TIMESTAMP)),
  ('ORD-012','C-30021','Bob Martinez', 'bob.m@example.com', 'RETURNING','PROD-1101','Laptop Pro 15"','Electronics','Laptops',1,1299.00,0.10,'SMALL',1299.00,129.90,1169.10,233.82,1402.92, 650.00,0.444,'PENDING',  'WEB',       'EU',     'CARD',          FALSE,CAST('2026-06-24' AS DATE),9, 2,1750726800000,CAST('2026-06-24 09:05:00' AS TIMESTAMP)),
  ('ORD-013','C-85441','Chen Wei',     'chen.w@example.com','RETURNING','PROD-3301','Smart TV 55"',     'Electronics','TV',  1, 649.00,0.00,'NONE',  649.00,  0.00, 649.00, 64.90, 713.90, 280.00,0.569,'CONFIRMED','PARTNER_API','APAC',  'BANK_TRANSFER', FALSE,CAST('2026-06-24' AS DATE),10,2,1750730400000,CAST('2026-06-24 10:05:00' AS TIMESTAMP)),
  ('ORD-014','C-72019','Diana Lee',    'diana.l@example.com','RETURNING','PROD-2201','Running Shoes',    'Apparel',  'Footwear',2,  89.00,0.05,'SMALL', 178.00,  8.90, 169.10, 14.37, 183.47,  56.00,0.669,'SHIPPED',  'WEB',       'US_WEST','CARD',          FALSE,CAST('2026-06-24' AS DATE),11,2,1750734000000,CAST('2026-06-24 11:05:00' AS TIMESTAMP)),
  ('ORD-015','C-11003','Grace Kim',    'grace.k@example.com','NEW',     'PROD-5501','Gaming Console',   'Electronics','Gaming',1,499.00,0.25,'LARGE', 499.00,124.75, 374.25, 74.85, 449.10, 320.00,0.145,'CONFIRMED','MOBILE',    'EU',     'WALLET',         TRUE, CAST('2026-06-24' AS DATE),12,2,1750737600000,CAST('2026-06-24 12:05:00' AS TIMESTAMP))
"""

_MART_INSERT = """
INSERT INTO demo.order_analytics_mart VALUES
  (CAST('2026-06-22' AS DATE),'US_EAST','Electronics','WEB',        2, 1, 0, 1798.00,1798.00,  0.00, 899.00,0.429,1798.00,1798.00,   NULL,   NULL,  1, 1, CAST('2026-06-22 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-22' AS DATE),'US_WEST','Electronics','WEB',        1, 1, 0,  649.00, 551.65, 97.35, 551.65,0.492, 551.65, 551.65,   NULL,   NULL,  3, 2, CAST('2026-06-22 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-22' AS DATE),'EU',     'Electronics','MOBILE',     1, 1, 1,  298.00, 268.20, 29.80, 268.20,0.664, 268.20, 268.20,   NULL,   NULL,  4, 3, CAST('2026-06-22 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-22' AS DATE),'APAC',   'Apparel',    'WEB',        1, 1, 1,   89.00,  89.00,  0.00,  89.00,0.685,  89.00,  89.00,   NULL,   NULL,  5, 1, CAST('2026-06-22 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-23' AS DATE),'EU',     'Electronics','WEB',        1, 1, 1,  249.00, 199.20, 49.80, 199.20,0.523, 467.40, 399.00,   NULL,-68.80,   2, 2, CAST('2026-06-23 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-23' AS DATE),'EU',     'Home',       'MOBILE',     1, 1, 0,  178.00, 169.10,  8.90, 169.10,0.586, 437.30, 309.35,   NULL,   NULL,  2, 1, CAST('2026-06-23 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-23' AS DATE),'US_EAST','Electronics','WEB',        1, 1, 1,  149.00, 149.00,  0.00, 149.00,0.698,1947.00,1267.53,   NULL,   NULL,  1, 1, CAST('2026-06-23 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-23' AS DATE),'APAC',   'Sports',     'WEB',        1, 1, 0,  117.00, 117.00,  0.00, 117.00,0.692, 206.00, 103.00,   NULL,   NULL,  3, 1, CAST('2026-06-23 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-24' AS DATE),'EU',     'Electronics','WEB',        2, 2, 1, 1948.00,1712.35,235.65, 856.18,0.500,2180.30,1309.28,1513.15,1443.55, 1, 1, CAST('2026-06-24 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-24' AS DATE),'US_EAST','Electronics','MOBILE',     1, 1, 0,  158.00, 158.00,  0.00, 158.00,0.646,2105.00,1125.67,   9.00,   NULL,  2, 2, CAST('2026-06-24 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-24' AS DATE),'APAC',   'Electronics','PARTNER_API',1, 1, 0,  649.00, 649.00,  0.00, 649.00,0.569, 855.00, 355.00,560.00,   NULL,  1, 1, CAST('2026-06-24 23:00:00' AS TIMESTAMP)),
  (CAST('2026-06-24' AS DATE),'US_WEST','Apparel',    'WEB',        1, 1, 0,  178.00, 169.10,  8.90, 169.10,0.669, 720.75, 369.03,   NULL,  80.10,  3, 1, CAST('2026-06-24 23:00:00' AS TIMESTAMP))
"""


def _impala_type(t: str) -> str:
    t = t.lower()
    if t in ("timestamptz", "timestamp"):   return "TIMESTAMP"
    if t == "date":                          return "DATE"
    if t in ("long", "bigint"):              return "BIGINT"
    if t == "int":                           return "INT"
    if t == "double":                        return "DOUBLE"
    if t == "boolean":                       return "BOOLEAN"
    return "STRING"


def _make_create_table(tbl: dict) -> str:
    """Generate an Impala-native CREATE TABLE IF NOT EXISTS … STORED AS ICEBERG."""
    parts  = tbl["name"].split(".")
    db, name = (parts[0], parts[-1]) if len(parts) > 1 else ("demo", parts[0])
    cols   = ",\n  ".join(
        f"`{f['name']}` {_impala_type(f['type'])}"
        for f in tbl["fields"]
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {db}.{name} (\n  {cols}\n)\n"
        f"STORED AS ICEBERG"
    )


def create_tables_and_insert():
    _step("Creating Iceberg tables in Impala (DDL)")
    ddl_stmts = [_make_create_table(t) for t in ICEBERG_TABLES]
    _run_impala(ddl_stmts, "DDL (CREATE TABLE IF NOT EXISTS)")

    _step("Inserting sample data (products: 10 rows, raw_orders: 15, enriched_orders: 15, mart: 12)")
    for label, sql in [
        ("products",             _PRODUCTS_INSERT),
        ("raw_orders",           _RAW_ORDERS_INSERT),
        ("enriched_orders",      _ENRICHED_ORDERS_INSERT),
        ("order_analytics_mart", _MART_INSERT),
    ]:
        _run_impala([sql], label)


# ── 7. Refresh catalog index (Qdrant) ────────────────────────────────────────

def refresh_catalog_index():
    _step("Refreshing Qdrant catalog index so new tables appear in search")
    try:
        from tools.iceberg.iceberg_tools import list_iceberg_tables, invalidate_iceberg_list_cache
        from tools.catalog import catalog_store
        invalidate_iceberg_list_cache()
        tables = list_iceberg_tables(force_refresh=True)
        catalog_store.index_iceberg_tables_bulk(tables)
        _ok(f"Qdrant index refreshed  ({len(tables)} tables)")
    except Exception as e:
        _skip(f"catalog refresh failed: {e}")


# ── 8. Verify traversal ───────────────────────────────────────────────────────

def verify_lineage():
    _step("Verifying lineage traversal from order_analytics_mart (depth=3)")
    from tools.openmetadata.client import get_lineage_by_name, format_lineage_for_llm

    result = get_lineage_by_name("order_analytics_mart", "table", upstream_depth=3, downstream_depth=1)
    if not result:
        _skip("OM not reachable — lineage verification skipped")
        return

    nodes = (result.get("graph") or {}).get("nodes", [])
    edges = result.get("edge_count", 0)
    depths = sorted(set(n.get("depth", 0) for n in nodes))
    _ok(f"{len(nodes)} nodes  |  {edges} edges  |  depths={depths}")

    summary = format_lineage_for_llm(result, "order_analytics_mart")
    print("\n" + summary)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Order Intelligence Demo — OM Setup")
    print("=" * 60)

    setup_services()
    register_topic()
    register_schema_registry()
    register_tables()
    create_lineage()
    create_tables_and_insert()
    refresh_catalog_index()
    verify_lineage()

    print("\n" + "=" * 60)
    print("  Setup complete.")
    print("  Ask the agent: 'show full lineage for order_analytics_mart'")
    print("  or: 'what breaks if order-events schema changes?'")
    print("=" * 60)
