-- =============================================================================
-- Order Intelligence Pipeline — SQL Transformation Script
--
-- Pipeline:
--   [Kafka] order-events
--       ↓  Section 1: Flink SQL — stream landing into raw_orders (Iceberg)
--   [Iceberg] demo.raw_orders
--       ↓  Section 2: Impala SQL — enrich with customer + product dimensions
--   [Iceberg] demo.enriched_orders  ← joins: raw_orders × customers × products
--       ↓  Section 3: Impala SQL — window aggregations → analytics mart
--   [Iceberg] demo.order_analytics_mart
--       ↓  Section 4: Validation queries — data quality checks
--
-- Engines:
--   Section 1  →  Flink SQL Client  (streaming, continuous)
--   Section 2+ →  Impala / Spark SQL on Cloudera CDP
-- =============================================================================


-- =============================================================================
-- SECTION 0: ICEBERG TABLE DDL  (run once — Impala or Spark)
-- =============================================================================

-- Raw landing zone — mirrors Kafka schema exactly, plus ingestion metadata
CREATE TABLE IF NOT EXISTS demo.raw_orders (
    order_id        STRING        COMMENT 'UUID from producer — natural PK',
    customer_id     STRING        COMMENT 'PII: links to customers table',
    product_id      STRING,
    quantity        INT,
    unit_price      DOUBLE,
    discount_pct    DOUBLE,
    order_status    STRING        COMMENT 'PENDING|CONFIRMED|SHIPPED|DELIVERED|CANCELLED',
    channel         STRING        COMMENT 'WEB|MOBILE|PARTNER_API',
    region          STRING        COMMENT 'US_EAST|US_WEST|EU|APAC',
    payment_method  STRING        COMMENT 'PII: CARD|BANK_TRANSFER|WALLET',
    is_first_order  BOOLEAN,
    event_timestamp BIGINT        COMMENT 'Epoch milliseconds from producer',
    ingested_at     TIMESTAMP,
    kafka_partition INT,
    kafka_offset    BIGINT
)
USING iceberg
PARTITIONED BY (days(ingested_at))
TBLPROPERTIES (
    'write.format.default'       = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'history.expire.max-snapshot-age-ms' = '7776000000'  -- 90 days
);

-- Product catalogue reference
CREATE TABLE IF NOT EXISTS demo.products (
    product_id   STRING,
    product_name STRING,
    category     STRING,
    sub_category STRING,
    cost_price   DOUBLE,
    list_price   DOUBLE,
    supplier_id  STRING,
    is_active    BOOLEAN,
    created_at   TIMESTAMP
)
USING iceberg
PARTITIONED BY (identity(category))
TBLPROPERTIES ('write.format.default' = 'parquet');

-- Enriched orders — result of transform 1
CREATE TABLE IF NOT EXISTS demo.enriched_orders (
    order_id          STRING,
    customer_id       STRING,
    customer_name     STRING,
    customer_email    STRING        COMMENT 'PII',
    customer_segment  STRING        COMMENT 'NEW|RETURNING|VIP|CHURNED',
    product_id        STRING,
    product_name      STRING,
    category          STRING,
    sub_category      STRING,
    quantity          INT,
    unit_price        DOUBLE,
    discount_pct      DOUBLE,
    discount_tier     STRING        COMMENT 'NONE|SMALL(<10%)|MEDIUM(10-20%)|LARGE(>20%)',
    gross_amount      DOUBLE        COMMENT 'quantity * unit_price',
    discount_amount   DOUBLE        COMMENT 'gross_amount * discount_pct',
    net_amount        DOUBLE        COMMENT 'gross_amount - discount_amount',
    tax_amount        DOUBLE        COMMENT 'net_amount * regional_tax_rate',
    total_amount      DOUBLE        COMMENT 'net_amount + tax_amount',
    cost_of_goods     DOUBLE        COMMENT 'quantity * products.cost_price',
    gross_margin      DOUBLE        COMMENT '(net_amount - cost_of_goods) / net_amount',
    order_status      STRING,
    channel           STRING,
    region            STRING,
    payment_method    STRING,
    is_first_order    BOOLEAN,
    order_date        DATE,
    order_hour        INT,
    order_day_of_week INT           COMMENT '1=Monday … 7=Sunday',
    event_timestamp   BIGINT,
    enriched_at       TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(order_date), identity(region))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.delete.mode'    = 'merge-on-read'   -- allows upserts for status changes
);

-- Analytics mart — daily × region × category aggregate
CREATE TABLE IF NOT EXISTS demo.order_analytics_mart (
    report_date             DATE,
    region                  STRING,
    category                STRING,
    channel                 STRING,
    order_count             BIGINT,
    unique_customers        BIGINT,
    new_customer_orders     BIGINT,
    gross_revenue           DOUBLE,
    net_revenue             DOUBLE,
    total_discount          DOUBLE,
    avg_order_value         DOUBLE,
    avg_gross_margin        DOUBLE,
    cumulative_revenue      DOUBLE   COMMENT 'Running total by region × category',
    revenue_7d_moving_avg   DOUBLE   COMMENT '7-day moving average of net_revenue',
    revenue_vs_prev_day     DOUBLE   COMMENT 'net_revenue - LAG(net_revenue,1)',
    revenue_vs_prev_week    DOUBLE   COMMENT 'net_revenue - LAG(net_revenue,7)',
    region_revenue_rank     INT      COMMENT 'RANK of this region on this date by net_revenue',
    category_revenue_rank   INT      COMMENT 'RANK of this category on this date by net_revenue',
    computed_at             TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('write.format.default' = 'parquet');


-- =============================================================================
-- SECTION 1: FLINK SQL — Kafka → raw_orders  (streaming, continuous)
-- Submit via: flink sql-client -f order_intelligence_transforms.sql
-- =============================================================================

-- Source: Kafka topic with Avro schema from Confluent Schema Registry
CREATE TABLE IF NOT EXISTS kafka_order_events (
    order_id        STRING,
    customer_id     STRING,
    product_id      STRING,
    quantity        INT,
    unit_price      DOUBLE,
    discount_pct    DOUBLE,
    order_status    STRING,
    channel         STRING,
    region          STRING,
    payment_method  STRING,
    is_first_order  BOOLEAN,
    event_timestamp BIGINT,
    -- Flink metadata columns
    `__kafka_partition` INT     METADATA FROM 'partition',
    `__kafka_offset`    BIGINT  METADATA FROM 'offset',
    `__event_time`      TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR `__event_time` AS `__event_time` - INTERVAL '10' SECOND
) WITH (
    'connector'                          = 'kafka',
    'topic'                              = 'order-events',
    'properties.bootstrap.servers'       = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'                = 'flink-order-ingest-consumer',
    'format'                             = 'avro-confluent',
    'avro-confluent.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
    'scan.startup.mode'                  = 'earliest-offset'
);

-- Sink: Iceberg table via REST catalog
CREATE TABLE IF NOT EXISTS iceberg_raw_orders (
    order_id        STRING,
    customer_id     STRING,
    product_id      STRING,
    quantity        INT,
    unit_price      DOUBLE,
    discount_pct    DOUBLE,
    order_status    STRING,
    channel         STRING,
    region          STRING,
    payment_method  STRING,
    is_first_order  BOOLEAN,
    event_timestamp BIGINT,
    ingested_at     TIMESTAMP(3),
    kafka_partition INT,
    kafka_offset    BIGINT
) WITH (
    'connector'      = 'iceberg',
    'catalog-name'   = 'default_catalog',
    'catalog-type'   = 'rest',
    'uri'            = '${ICEBERG_CATALOG_URI}',
    'warehouse'      = '${ICEBERG_WAREHOUSE}',
    'database-name'  = 'demo',
    'table-name'     = 'raw_orders',
    'write.format.default' = 'parquet'
)
PARTITIONED BY (DAY(ingested_at));

-- Continuous streaming insert — runs until cancelled
INSERT INTO iceberg_raw_orders
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    COALESCE(discount_pct, 0.0)   AS discount_pct,
    order_status,
    channel,
    region,
    payment_method,
    is_first_order,
    event_timestamp,
    CURRENT_TIMESTAMP              AS ingested_at,
    `__kafka_partition`            AS kafka_partition,
    `__kafka_offset`               AS kafka_offset
FROM kafka_order_events
-- Drop clearly malformed records at the stream boundary
WHERE order_id    IS NOT NULL
  AND customer_id IS NOT NULL
  AND quantity    > 0
  AND unit_price  > 0;


-- =============================================================================
-- SECTION 2: IMPALA SQL — Transform 1: raw_orders → enriched_orders
-- Scheduled as a CDP Data Engineering job (Oozie / Airflow) — runs hourly.
-- Joins: raw_orders × customers × products
-- Adds:  revenue columns, discount tier, gross margin, date dimensions
-- =============================================================================

INSERT INTO demo.enriched_orders
SELECT
    r.order_id,
    r.customer_id,
    c.customer_name,
    c.email                                            AS customer_email,

    -- Customer segment: derived from order history
    CASE
        WHEN r.is_first_order                          THEN 'NEW'
        WHEN c.lifetime_order_count >= 20              THEN 'VIP'
        WHEN c.days_since_last_order > 180             THEN 'CHURNED'
        ELSE                                                'RETURNING'
    END                                                AS customer_segment,

    r.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    r.quantity,
    r.unit_price,
    COALESCE(r.discount_pct, 0.0)                      AS discount_pct,

    -- Discount tier classification
    CASE
        WHEN COALESCE(r.discount_pct, 0) = 0          THEN 'NONE'
        WHEN r.discount_pct < 0.10                     THEN 'SMALL'
        WHEN r.discount_pct BETWEEN 0.10 AND 0.20      THEN 'MEDIUM'
        ELSE                                                'LARGE'
    END                                                AS discount_tier,

    -- Revenue columns
    r.quantity * r.unit_price                          AS gross_amount,
    r.quantity * r.unit_price * COALESCE(r.discount_pct, 0)
                                                       AS discount_amount,
    r.quantity * r.unit_price * (1 - COALESCE(r.discount_pct, 0))
                                                       AS net_amount,

    -- Regional tax rates: US 8.5%, EU 20% (VAT), APAC 10%, default 0%
    r.quantity * r.unit_price * (1 - COALESCE(r.discount_pct, 0)) *
        CASE r.region
            WHEN 'US_EAST'  THEN 0.085
            WHEN 'US_WEST'  THEN 0.085
            WHEN 'EU'       THEN 0.20
            WHEN 'APAC'     THEN 0.10
            ELSE 0.0
        END                                            AS tax_amount,

    r.quantity * r.unit_price * (1 - COALESCE(r.discount_pct, 0)) *
        (1 + CASE r.region
                WHEN 'US_EAST'  THEN 0.085
                WHEN 'US_WEST'  THEN 0.085
                WHEN 'EU'       THEN 0.20
                WHEN 'APAC'     THEN 0.10
                ELSE 0.0
             END)                                      AS total_amount,

    -- Margin: requires product cost
    r.quantity * COALESCE(p.cost_price, 0)             AS cost_of_goods,
    CASE
        WHEN r.quantity * r.unit_price * (1 - COALESCE(r.discount_pct, 0)) = 0
        THEN 0.0
        ELSE (
            r.quantity * r.unit_price * (1 - COALESCE(r.discount_pct, 0))
            - r.quantity * COALESCE(p.cost_price, 0)
        ) / (r.quantity * r.unit_price * (1 - COALESCE(r.discount_pct, 0)))
    END                                                AS gross_margin,

    r.order_status,
    r.channel,
    r.region,
    r.payment_method,
    r.is_first_order,

    -- Date dimensions for partition + BI slicing
    CAST(FROM_UNIXTIME(r.event_timestamp / 1000) AS DATE)  AS order_date,
    HOUR(FROM_UNIXTIME(r.event_timestamp / 1000))          AS order_hour,
    DAYOFWEEK(FROM_UNIXTIME(r.event_timestamp / 1000))     AS order_day_of_week,

    r.event_timestamp,
    NOW()                                              AS enriched_at

FROM demo.raw_orders r

-- Left join: retain orders with unknown customers (data quality signal, not a filter)
LEFT JOIN demo.customers c
    ON r.customer_id = c.id

-- Left join: retain orders for discontinued products
LEFT JOIN demo.products p
    ON r.product_id = p.product_id

WHERE r.ingested_at >= TIMESTAMPADD(HOUR, -1, NOW())   -- incremental: last hour only
  AND r.order_id NOT IN (
      SELECT order_id FROM demo.enriched_orders        -- idempotent: skip already-enriched
      WHERE order_date >= DATE_SUB(NOW(), INTERVAL 2 DAY)
  );


-- =============================================================================
-- SECTION 3: IMPALA SQL — Transform 2: enriched_orders → order_analytics_mart
-- Scheduled daily after Transform 1 completes (T+1 hour SLA).
-- Uses CTEs, window functions, LAG, running totals, rolling averages.
-- =============================================================================

-- Materialise yesterday's mart rows (DELETE + INSERT for full daily refresh)
DELETE FROM demo.order_analytics_mart
WHERE report_date = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY);

INSERT INTO demo.order_analytics_mart
WITH

-- CTE 1: Daily base metrics per grain (date × region × category × channel)
daily_base AS (
    SELECT
        order_date                              AS report_date,
        region,
        category,
        channel,
        COUNT(*)                                AS order_count,
        COUNT(DISTINCT customer_id)             AS unique_customers,
        SUM(CASE WHEN is_first_order THEN 1 ELSE 0 END)
                                                AS new_customer_orders,
        SUM(gross_amount)                       AS gross_revenue,
        SUM(net_amount)                         AS net_revenue,
        SUM(discount_amount)                    AS total_discount,
        AVG(net_amount)                         AS avg_order_value,
        AVG(gross_margin)                       AS avg_gross_margin
    FROM demo.enriched_orders
    WHERE order_status != 'CANCELLED'           -- exclude cancellations from revenue
      AND order_date = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    GROUP BY order_date, region, category, channel
),

-- CTE 2: Historical daily net revenue for the same grain (needed for LAG/window)
-- Pull 30 days to support 7-day moving average + week-ago comparison
history AS (
    SELECT
        report_date,
        region,
        category,
        channel,
        net_revenue
    FROM demo.order_analytics_mart
    WHERE report_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
      AND report_date <  DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
),

-- CTE 3: Union current day with history so window functions see the full series
full_series AS (
    SELECT report_date, region, category, channel, net_revenue,
           TRUE  AS is_current
    FROM daily_base
    UNION ALL
    SELECT report_date, region, category, channel, net_revenue,
           FALSE AS is_current
    FROM history
),

-- CTE 4: Window functions applied over the full series
windowed AS (
    SELECT
        report_date,
        region,
        category,
        channel,
        net_revenue,
        is_current,

        -- Running cumulative revenue (within region × category, ordered by date)
        SUM(net_revenue) OVER (
            PARTITION BY region, category
            ORDER BY report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                           AS cumulative_revenue,

        -- 7-day moving average (current day + 6 preceding)
        AVG(net_revenue) OVER (
            PARTITION BY region, category
            ORDER BY report_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                           AS revenue_7d_moving_avg,

        -- Day-over-day delta
        net_revenue - LAG(net_revenue, 1) OVER (
            PARTITION BY region, category
            ORDER BY report_date
        )                                           AS revenue_vs_prev_day,

        -- Week-over-week delta
        net_revenue - LAG(net_revenue, 7) OVER (
            PARTITION BY region, category
            ORDER BY report_date
        )                                           AS revenue_vs_prev_week
    FROM full_series
),

-- CTE 5: Region rank on this date (which region generated most net revenue?)
region_ranks AS (
    SELECT
        report_date,
        region,
        RANK() OVER (
            PARTITION BY report_date
            ORDER BY SUM(net_revenue) DESC
        )                                           AS region_revenue_rank
    FROM windowed
    WHERE is_current
    GROUP BY report_date, region
),

-- CTE 6: Category rank on this date within each region
category_ranks AS (
    SELECT
        report_date,
        region,
        category,
        RANK() OVER (
            PARTITION BY report_date, region
            ORDER BY SUM(net_revenue) DESC
        )                                           AS category_revenue_rank
    FROM windowed
    WHERE is_current
    GROUP BY report_date, region, category
)

-- Final SELECT: join everything back to the daily base grain
SELECT
    b.report_date,
    b.region,
    b.category,
    b.channel,
    b.order_count,
    b.unique_customers,
    b.new_customer_orders,
    b.gross_revenue,
    b.net_revenue,
    b.total_discount,
    b.avg_order_value,
    b.avg_gross_margin,
    w.cumulative_revenue,
    w.revenue_7d_moving_avg,
    w.revenue_vs_prev_day,
    w.revenue_vs_prev_week,
    rr.region_revenue_rank,
    cr.category_revenue_rank,
    NOW()                                           AS computed_at

FROM daily_base b

JOIN windowed w
    ON  b.report_date = w.report_date
    AND b.region      = w.region
    AND b.category    = w.category
    AND b.channel     = w.channel
    AND w.is_current  = TRUE

JOIN region_ranks rr
    ON  b.report_date = rr.report_date
    AND b.region      = rr.region

JOIN category_ranks cr
    ON  b.report_date = cr.report_date
    AND b.region      = cr.region
    AND b.category    = cr.category;


-- =============================================================================
-- SECTION 4: VALIDATION QUERIES — Data Quality Checks
-- Run after each transform to surface issues before the mart goes to Looker.
-- =============================================================================

-- QC-1: Raw orders null rate on key PII/join columns
SELECT
    COUNT(*)                                            AS total_rows,
    SUM(CASE WHEN order_id    IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN product_id  IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN quantity <= 0       THEN 1 ELSE 0 END) AS invalid_quantity,
    SUM(CASE WHEN unit_price <= 0     THEN 1 ELSE 0 END) AS invalid_price,
    ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS customer_id_null_pct
FROM demo.raw_orders
WHERE ingested_at >= TIMESTAMPADD(HOUR, -1, NOW());


-- QC-2: Enrichment join hit rate — how many orders matched a customer / product?
SELECT
    COUNT(*)                                            AS total_enriched,
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END)
                                                        AS customer_join_misses,
    SUM(CASE WHEN product_name  IS NULL THEN 1 ELSE 0 END)
                                                        AS product_join_misses,
    ROUND(100.0 * SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS customer_miss_pct,
    ROUND(100.0 * SUM(CASE WHEN product_name  IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS product_miss_pct
FROM demo.enriched_orders
WHERE enriched_at >= TIMESTAMPADD(HOUR, -1, NOW());


-- QC-3: Margin sanity — no row should have gross_margin < -1 or > 1
SELECT COUNT(*) AS margin_anomalies
FROM demo.enriched_orders
WHERE gross_margin < -1.0
   OR gross_margin >  1.0;


-- QC-4: Mart completeness — every active region × top-5 categories should have a row
SELECT
    m.region,
    m.category,
    m.report_date,
    m.net_revenue,
    m.order_count,
    m.region_revenue_rank,
    m.category_revenue_rank
FROM demo.order_analytics_mart m
WHERE m.report_date = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
ORDER BY m.region_revenue_rank, m.category_revenue_rank
LIMIT 40;


-- QC-5: Period-over-period revenue anomaly detector
-- Flags regions where revenue dropped more than 30% vs. prior week
SELECT
    report_date,
    region,
    SUM(net_revenue)                                    AS net_revenue,
    SUM(revenue_vs_prev_week)                           AS delta_vs_last_week,
    ROUND(
        100.0 * SUM(revenue_vs_prev_week)
              / NULLIF(SUM(net_revenue) - SUM(revenue_vs_prev_week), 0),
        1
    )                                                   AS pct_change_vs_last_week
FROM demo.order_analytics_mart
WHERE report_date = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
GROUP BY report_date, region
HAVING ABS(pct_change_vs_last_week) > 30
ORDER BY pct_change_vs_last_week ASC;
