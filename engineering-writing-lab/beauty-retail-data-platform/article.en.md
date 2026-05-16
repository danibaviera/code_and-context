# Architecting a Data Platform for Beauty Retail: personalization, demand forecasting, and omnichannel experience

> **Domain:** Retail / Cosmetics  
> **Stack:** Python · Apache Airflow · dbt · PostgreSQL · Apache Kafka · React  
> **Focus:** Personalized recommendation, demand forecasting, stockout prevention, omnichannel view

---

## 1. Business Context

A cosmetics company with an omnichannel operation — e-commerce, physical stores, and a mobile app — grew rapidly over the past two years. That growth brought a structural problem: data from different channels lived in silos, and operational decisions were still being made based on manually consolidated reports, often two or three days late.

The most critical pain points identified:

- **Recurring stockouts** on the highest-turnover SKUs, especially skincare items with high seasonal demand. The supply chain team only identified a stockout after the product had already hit zero in the system.
- **Generic recommendations** on e-commerce and the app, with no consideration of purchase history, declared skin type, or category preferences. The "best sellers" algorithm was the same for every customer.
- **Low-ROI marketing campaigns** because customer segments were defined manually and statically — no dynamic update based on recent behavior.
- **No unified customer view**: a customer who shopped in the physical store and also on e-commerce was treated as two different profiles. The history from one did not feed the experience of the other.
- **Overstocking of seasonal items**, causing high working capital costs and the need for clearance sales that eroded margins.

The diagnosis was clear: the company had enough data to solve these problems, but lacked the architecture to turn it into operational intelligence. The solution required a data platform that integrated sources, organized data into layers, and made actionable insights available to different teams.

---

## 2. Functional and Non-Functional Requirements

### Functional Requirements

- Consolidation of data from all channels (e-commerce, physical POS, mobile app) into a unified customer and product view
- Personalized recommendation pipeline by customer profile, updated based on recent behavior
- Predictive demand model with 30-, 60-, and 90-day horizons per SKU and region
- Dynamic customer segmentation for marketing campaigns
- Operational dashboard for the supply chain team with near-real-time inventory visibility
- Automatic imminent stockout alerts before inventory reaches zero
- Serving API for consumption of recommendations by the React frontend

### Non-Functional Requirements

| Dimension | Requirement |
|---|---|
| **Scale** | 500k active customers, ~50k transactions/day, 3x peaks during seasonal dates |
| **Latency** | Behavioral events: ingestion in < 5 minutes / Forecasting: daily update |
| **Availability** | 99.9% for supply chain pipelines and stockout alerts |
| **Data Retention** | Transactional: 3 years / Behavioral logs: 12 months |
| **Cost** | Analytical budget isolated from transactional budget — no resource contention |
| **Governance** | LGPD compliance for behavioral data, profiles, and purchase history |
| **Quality** | SLA of >= 98% valid records in serving layer tables |
| **Observability** | Automatic alerts for pipeline failures and quality deviations |

---

## 3. Architectural Decisions

### 3.1 Event-driven architecture for behavioral capture

**Decision:** use Apache Kafka for near-real-time capture of digital behavioral events — browsing, add-to-cart, abandonment, category search.

**Justification:** a customer's in-session behavior has an extremely short relevance window. A customer who puts a specific serum in her cart and abandons the purchase needs a remarketing action within minutes for her to still have that product in mind — not hours later. With a traditional batch approach, that signal would arrive with a multi-hour delay or only in the next day's report, making any action ineffective.

Additionally, Kafka acts as a decoupled event bus: the e-commerce system publishes events without needing to know who will consume them. This allows adding new consumers (a fraud detection pipeline, for example) without changing the source.

**Alternative considered and discarded:** direct batch polling on the e-commerce database every 30 minutes. Discarded due to latency and the risk of overloading the transactional database with extraction queries.

---

### 3.2 Feature store for consistency between training and production

**Decision:** implement a centralized feature store for recommendation and segmentation features.

**Justification:** a classic problem in production ML systems is *training-serving skew*: the model is trained with features calculated one way and served in production with features calculated another way — small differences in logic, time window, or null handling that seem irrelevant but silently degrade prediction quality.

For beauty recommendations, where the model needs features like "proportion of skincare purchases in the last 90 days", "average time between purchases", and "SKUs viewed but not purchased in the last session", consistency between training and production is critical. The feature store ensures the same calculation logic is used in both environments.

**Alternative considered and discarded:** calculating features directly in the inference query. Discarded due to the risk of skew and the inability to reuse the same features across multiple models.

---

### 3.3 Separation between transactional and analytical layers via CDC

**Decision:** keep the transactional PostgreSQL completely isolated from the analytical environment, with synchronization via CDC (Change Data Capture) managed by Airflow.

**Justification:** complex analytical queries — LTV calculation, cohort analysis, product ranking by margin — executed directly on the transactional database would cause performance degradation for sales operations. On peak dates like Black Friday, this would be critical.

Separation allows each layer to scale independently: the transactional database is optimized for writes and single-record reads; the analytical database is optimized for wide scans and aggregations.

**Accepted trade-off:** controlled data duplication and a latency of minutes between a POS transaction and analytical availability. For the use cases in this project (supply chain, segmentation, forecasting), this latency is completely acceptable.

---

### 3.4 Layered modeling with dbt

**Decision:** adopt dbt for layered modeling (raw → trusted → business → serving), with automatic quality tests and generated documentation.

**Justification:** without a transformation framework, business logic tends to be scattered across multiple undocumented, untested, and untracked SQL queries. dbt solves this by treating SQL models as code: versioned in Git, with declaratively defined quality tests, and documentation automatically generated from the models.

This creates an explicit contract between the data engineering team and analytical consumers: any analyst can open dbt docs and understand where each field comes from, what transformations were applied, and which validations guarantee the integrity of that data.

---

## 4. Trade-offs

| Decision | Gain | Loss |
|---|---|---|
| Kafka for behavioral events | Near-real-time reactivity; decoupling between producer and consumer | Higher operational complexity; need to manage topics and consumer groups |
| Feature store | Full consistency between training and production; feature reuse across models | Additional operational cost; overhead of maintaining up-to-date features |
| CDC + separate analytical layer | Analytical scalability without impacting the transactional database | Data duplication; minutes of latency between transaction and analytical availability |
| dbt for layered modeling | Governance, automatic tests, documentation, Git versioning | Learning curve for the team; initial configuration overhead |
| Airflow for orchestration | Pipeline visibility; controlled reprocessing; explicit dependencies between DAGs | DAG maintenance overhead; additional infrastructure for the scheduler |

---

## 5. Technical Implementation

### Stack

```
Batch ingestion:    Apache Airflow + Python (connectors for POS and app)
Stream ingestion:   Apache Kafka + Python consumers
Storage:            Transactional PostgreSQL + Analytical PostgreSQL (separate)
Transformation:     dbt (SQL models with tests and documentation)
Orchestration:      Apache Airflow
Feature store:      PostgreSQL + Redis (low-latency feature cache)
ML serving:         FastAPI (Python) for recommendations API
Frontend:           React (internal supply chain dashboards)
```

### Data Flow

```
Sources                   Ingestion                  Raw Storage
───────                   ─────────                  ───────────
E-commerce          ──── Kafka (events) ──────────► raw.ecommerce_events
Physical POS        ──── Airflow CDC    ──────────► raw.pos_transactions
Mobile App          ──── Airflow API    ──────────► raw.mobile_sessions
Inventory/ERP       ──── Airflow batch  ──────────► raw.inventory_snapshots
Campaigns/CRM       ──── Airflow API    ──────────► raw.campaign_events

                              │
                         [dbt models]
                              │
              ┌───────────────┼───────────────┐
         [trusted.*]    [business.*]      [serving.*]
              │               │                │
         Cleaned and     Business entities  Models for
         validated data  (dims and facts)   ML & dashboards
                               │
              ┌────────────────┼───────────────┐
        [Feature Store]  [Forecasting]  [Dashboards]
        Recommendation   Demand         Supply Chain
        RFM Segmentation Stockout       Marketing
```

### Modeling Layers

**Raw layer** — raw data with no transformation, preserved exactly as received from the source. Each record has `_ingested_at` (timestamp of arrival in the platform) and `_source_system` (source identifier). Nothing is deleted from this layer.

**Trusted layer** — clean, correctly typed data, with deduplication by business key and referential integrity validation. Records that fail validation are moved to quarantine tables (`_quarantine`) with the error reason for analysis. This layer is the platform's quality contract.

**Business layer** — consolidated and enriched business entities: `dim_customer` (unified omnichannel profile), `dim_product` (attributes, category, margin), `fct_sales` (sales facts by channel), `fct_cart_events` (cart events). Cross-source enrichment happens here.

**Serving layer** — models optimized for final consumption: `customer_360` (consolidated customer view for ML), `product_demand_features` (forecasting features per SKU), `campaign_performance` (campaign performance for marketing), `inventory_risk` (products at stockout risk for supply chain).

### Customer Segmentation Model

Segmentation uses RFM (Recency, Frequency, Monetary) enriched with category preference attributes and preferred purchase channel. The model is recalculated weekly via an Airflow pipeline, and updated segments are synced with the CRM platform for campaign activation.

```sql
-- Illustrative RFM score calculation for customer segmentation
WITH rfm_base AS (
  SELECT
    customer_id,
    DATE_DIFF(CURRENT_DATE, MAX(order_date), DAY)  AS recency_days,
    COUNT(DISTINCT order_id)                        AS frequency,
    SUM(order_value)                                AS monetary_value
  FROM business.fct_sales
  WHERE order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH)
  GROUP BY customer_id
),

rfm_scored AS (
  SELECT
    customer_id,
    NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,
    NTILE(5) OVER (ORDER BY frequency)         AS frequency_score,
    NTILE(5) OVER (ORDER BY monetary_value)    AS monetary_score
  FROM rfm_base
)

SELECT
  customer_id,
  recency_score,
  frequency_score,
  monetary_score,
  (recency_score + frequency_score + monetary_score) AS rfm_total,
  CASE
    WHEN recency_score >= 4 AND frequency_score >= 4 THEN 'champions'
    WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'loyal'
    WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'new_customers'
    WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'at_risk'
    ELSE 'standard'
  END AS customer_segment
FROM rfm_scored
```

### Demand Forecasting Pipeline

The per-SKU forecasting model uses:

- **Historical sales series**: 24 months per SKU and distribution region
- **Seasonality**: holidays (Mother's Day, Christmas, Valentine's Day), seasons, monthly cycle
- **Campaign signals**: active coupons, scheduled promotions, product launches
- **Stockout history**: periods when the product was unavailable (to correct the historical series)
- **Category trends**: growth or decline of subcategories (e.g., growth of SPF skincare)

The Airflow pipeline triggers weekly model retraining. Before promoting a new model to production, an automatic validation step checks MAPE (Mean Absolute Percentage Error) for the 30-, 60-, and 90-day horizons. Models that do not meet the defined threshold are held in quarantine for manual analysis.

```python
# Illustrative structure of the forecasting DAG
from airflow import DAG
from airflow.operators.python import PythonOperator

def validate_model_performance(model_path: str, threshold_mape: float) -> bool:
    """
    Evaluates the MAPE of the candidate model on the validation set.
    Returns True only if performance meets the defined threshold.
    """
    metrics = evaluate_model(model_path)
    return all(
        metrics[f"mape_{horizon}d"] <= threshold_mape
        for horizon in [30, 60, 90]
    )

with DAG("demand_forecasting_weekly", schedule_interval="@weekly") as dag:
    extract_features   = PythonOperator(task_id="extract_features",   ...)
    train_model        = PythonOperator(task_id="train_model",         ...)
    validate_model     = PythonOperator(task_id="validate_model",      ...)
    promote_model      = PythonOperator(task_id="promote_to_prod",     ...)
    generate_forecasts = PythonOperator(task_id="generate_forecasts",  ...)

    extract_features >> train_model >> validate_model >> promote_model >> generate_forecasts
```

### Feature Engineering for Recommendation

Recommendation features are calculated in batch (daily update) and stored in the feature store. Low-latency features — such as "products viewed in the current session" — are calculated in real time and stored in Redis for serving with latency < 50ms.

Examples of features persisted in the feature store:

| Feature | Window | Granularity |
|---|---|---|
| `category_affinity_skincare` | 90 days | Per customer |
| `avg_ticket_value` | 12 months | Per customer |
| `purchase_channel_preference` | 6 months | Per customer |
| `days_since_last_purchase` | — | Per customer |
| `repurchase_rate_by_sku` | 12 months | Per SKU |
| `cross_sell_affinity` | 12 months | SKU pair |

---

## 6. Observability and Governance

### Quality Monitoring with dbt

Each layer has automatic tests executed at the end of each transformation pipeline. Failures block promotion to the next layer.

| Test | Layer | What it validates |
|---|---|---|
| `not_null` on key fields | trusted, business | No records without ID or date |
| `unique` on PKs | trusted, business | No duplicates on primary keys |
| `accepted_values` | trusted | Valid enums (order status, channel, category) |
| `relationships` | business | Referential integrity between facts and dimensions |
| `row_count_anomaly` | serving | Volume within expected range (deviation > 20% = alert) |

### Lineage

dbt automatically generates the lineage graph for all models, making it visible to any engineer or analyst:

- Which raw source each field in the serving layer comes from
- Which downstream models will be affected by an upstream change
- The execution history and test status of each model

### Operational Alerts

| Alert | Trigger | Channel |
|---|---|---|
| Pipeline failure | Airflow DAG in `failed` status after retries | Slack #data-ops + PagerDuty |
| Quality below SLA | % of valid records < 98% in serving table | Slack #data-quality |
| Imminent stockout | Projected inventory < 15% of reorder point in 7 days | Supply chain dashboard + email |
| Volume anomaly | Event volume < 50% of 7-day average | Slack #data-ops |
| Feature skew | Deviation > 15% between training and production features | Slack #ml-ops |

### Versioning

- dbt models versioned in Git with pull request review
- ML models versioned with MLflow (experiments, parameters, metrics, artifacts)
- Transactional database schema versioned with Alembic
- All changes to serving models go through a downstream impact validation process

### LGPD and Privacy

Behavioral and preference data is stored with pseudonymization: the personal identification key is held in a separate table with role-restricted access. Consent logs are tracked, and any deletion request triggers an automatic purge pipeline that removes customer data across all layers, with a confirmation log.

---

## 7. Stakeholder Impact

### Supply Chain

- **Stockout reduction**: from 18% to 4% on high-turnover SKUs after 3 months of operation with automated alerts
- **Emergency replenishment response time**: reduced from 72h to 24h based on predictive alerts
- **Reduced seasonal overstock**: demand simulations enabled adjustments to seasonal collection orders, reducing leftover inventory by 30%

### Marketing

- **34% increase in conversion rate** for RFM-segmented campaigns compared to broadcast campaigns to the entire base
- **CAC (Customer Acquisition Cost) reduced by 22%** with dynamic segmentation by preferred purchase channel and journey stage
- **Increased return on reactivation campaigns**: "at_risk" segment with personalized actions increased repurchase by 28%

### Product / E-commerce

- **Personalized recommendation CTR 2.8x higher** than generic "best sellers" recommendations
- **15% increase in average order value** via category-affinity-driven cross-sell
- **Real omnichannel experience**: the unified customer view allowed coupons generated in the physical store to be recognized in the app and vice versa

### Executive Leadership

- Consolidated dashboard with near-real-time omnichannel view, accessible without dependence on the data team
- Ability to simulate demand scenarios for collection planning and supplier negotiation
- Structured data foundation for strategic analyses of geographic and portfolio expansion

---

## 8. Next Steps

### Short Term (0–3 months)

- **Migrate feature store to a managed solution** (Feast or Tecton) to reduce the operational overhead of maintaining the custom PostgreSQL + Redis setup
- **Implement structured A/B testing** in the serving layer: controlled experiments to compare recommendation model variations using business metrics (CTR, conversion, average order value)

### Medium Term (3–6 months)

- **Expand forecasting model with external signals**: integrate Google Trends data by product category and weather forecasts to capture trends before they appear in sales
- **Reverse ETL for segment synchronization**: automatically sync segments calculated on the platform with the CRM and paid media platforms (Meta, Google Ads) for direct activation without manual exports

### Long Term (6–12 months)

- **Evaluate migration of analytical PostgreSQL to a cloud warehouse** (BigQuery or Snowflake) to scale ad hoc analyses without computational resource constraints
- **Build a self-service experimentation platform** so product and marketing teams can create and track data experiments without depending on engineering for each analysis
- **Real-time churn prediction model** with automatic intervention for customers with high abandonment probability based on session patterns

---

*Diagrams available at [diagrams/](./diagrams/)*  
*References at [references.md](./references.md)*
