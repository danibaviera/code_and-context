# Building Data Architectures for Renewable Energy Markets: real-time ingestion, operational forecasting, and analytical governance

> **Domain:** Renewable Energy / Regulated Market  
> **Stack:** Python · Apache Kafka · Apache Spark · Apache Airflow · Grafana · PostgreSQL  
> **Focus:** Telemetry streaming, generation forecasting, regulatory governance, operational observability

---

## 1. Business Context

Renewable energy operators — solar and wind — face a structural challenge that does not exist in conventional sources: generation is intermittent and directly dependent on weather conditions outside the operator's control. The wind stops. Cloud cover increases. A cold front completely changes the generation profile of a wind farm within hours.

At the same time, the energy market operates with dispatch commitments scheduled with the National Electric System Operator (ONS). Deviations between what was programmed and what was actually generated result in financial penalties proportional to the deviation — placing generation forecasting at the center of the company's operational and financial decisions.

The Brazilian regulatory context (ANEEL, ONS, CCEE) imposes additional traceability requirements: measurements must be auditable, reports must be delivered within defined time windows, and any inconsistency in measurement data can result in challenges during regulatory inspections.

The pain points identified at the operator before the platform was implemented:

- **No consolidated visibility** across generation at multiple geographically distributed farms. Each farm had its own telemetry system with distinct interfaces and formats.
- **Manual integration with weather forecasts**: analysts manually downloaded forecast files and fed spreadsheets to estimate next-day generation.
- **Unstandardized sensor data**: turbines from different manufacturers generated telemetry in different protocols (MODBUS, OPC-DA, IEC 61850), with no translation to a canonical model.
- **No governance layer**: official measurement data was stored in local directories with no version control, integrity hashing, or access logging.
- **Reactive dispatch decisions**: the Control Center operated with a view of the recent past, with no structured predictive capability. Equipment anomalies were detected after they had already impacted generation, not before.

The platform needed to transform this fragmented environment into a data infrastructure reliable enough to support both real-time operational decisions and retroactive regulatory auditing.

---

## 2. Functional and Non-Functional Requirements

### Functional Requirements

- Real-time ingestion of sensor telemetry: generated power, voltage, equipment temperature, turbine vibration, solar irradiance
- Automated integration with weather forecast APIs (INMET, Open-Meteo, private services) with hourly updates
- Predictive generation model per farm with 24h and 72h horizons, updated every 15 minutes
- Real-time operational dashboard for the Control Center with visibility across all farms
- Automatic regulatory reporting pipeline (ONS/ANEEL) delivered by T+2h of the settlement period
- Equipment anomaly alerts for the maintenance team before impact on generation
- Full traceability of measurement data for regulatory auditing
- Detection of sensor gaps and failures with automatic data quality qualification

### Non-Functional Requirements

| Dimension | Requirement |
|---|---|
| **Scale** | 500+ sensors per farm; multiple simultaneous farms; ~10 million readings/day |
| **Latency** | Operational telemetry: < 30 seconds / Forecast update: every 15 minutes |
| **Availability** | 99.95% for operational dispatch pipeline and critical alerts |
| **Retention** | Telemetry: 5 years (ANEEL regulatory requirement) / Audit logs: 7 years |
| **Cost** | Automatic storage tiering (hot/warm/cold) based on access frequency |
| **Governance** | Full traceability for auditing; immutability of regulatory data |
| **Quality** | Zero tolerance for losses in official measurement data; mandatory flags for estimated data |
| **Regulatory SLA** | Report available by T+2h of the end of each settlement period |
| **Security** | Isolation of OT (Operational Technology) and IT networks; role-audited access |

---

## 3. Architectural Decisions

### 3.1 Lambda hybrid architecture to balance reactivity and reliability

**Decision:** adopt a Lambda architecture with two parallel processing layers: a *speed layer* with Kafka + Spark Structured Streaming for low-latency operational data, and a *batch layer* with Spark + Airflow for historical processing, regulatory reports, and model retraining.

**Justification:** the requirements are fundamentally contradictory if approached with a single technology. Sensor telemetry demands processing within seconds for equipment alerts — latency that only streaming delivers. Official measurement data for regulatory reports demands full consistency, reliable reprocessing, and immutability — properties that purely streaming architectures rarely guarantee without prohibitive operational cost.

The Lambda architecture resolves this tension by dedicating distinct technologies to distinct requirements, with a unified serving layer that abstracts the origin from end consumers.

**Alternative considered:** Kappa Architecture (unified streaming). Discarded because the requirement for auditable historical reprocessing with idempotency guarantees on regulatory data is simpler to implement with explicit batch processing.

---

### 3.2 Lakehouse as the central storage layer

**Decision:** implement the Lakehouse pattern with Delta Lake on object storage (S3-compatible), rather than a pure relational warehouse.

**Justification:** data arrives from sources with radically different levels of format maturity. Modern turbines generate Protobuf; legacy equipment generates CSV over FTP; weather APIs return JSON with schemas that change without notice. A pure warehouse would require rigid schema enforcement at ingestion, making the platform brittle to any source variation.

The Lakehouse allows flexible ingestion in the raw zone with progressive schema evolution, and adds capabilities critical for the regulatory context: *time travel* (access to historical data versions for auditing), automatic compaction of small files, and ACID transactions that ensure regulatory reads never see inconsistent states.

**Accepted trade-off:** greater management complexity compared to a managed warehouse. Justified by the gain in ingestion flexibility and the auditing capabilities via time travel.

---

### 3.3 Near-real-time predictive inference with a 15-minute window

**Decision:** serve the generation forecasting model via a Python microservice with feature and prediction updates every 15 minutes.

**Justification:** the Control Center makes dispatch adjustment decisions in 1-hour windows. For those decisions to be based on relevant forecasts, the forecast needs to incorporate the most recent weather conditions — a cold front that appears in the forecast at 2pm must be reflected in the generation forecast from 3pm onwards, not just in the next daily update.

**Accepted trade-off:** 15-minute latency between a new weather reading and the forecast update, versus the complexity of sub-minute real-time serving. The trade-off is acceptable: the operational dispatch horizon is measured in hours, and a 15-minute window does not create a significant gap for the decisions the system needs to support.

---

### 3.4 Separation between operational and regulatory domains

**Decision:** maintain explicitly separate pipelines and storage zones for operational data (high speed, corrections allowed) and regulatory data (audited, immutable after validation).

**Justification:** operational data needs flexibility — a sensor reading outside the valid range can be corrected once the cause is identified. Regulatory data, once validated, cannot be altered: any subsequent correction must be recorded as a new version with an auditable justification, not as a silent replacement of the original record.

The separation makes this contract explicit in the architecture, preventing operational processes from inadvertently contaminating the integrity of data that will be presented in audits.

---

## 4. Trade-offs

| Decision | Gain | Loss |
|---|---|---|
| Lambda Architecture (hybrid) | Low latency for operations + historical reliability for regulatory | Duplicate processing logic; larger maintenance surface |
| Lakehouse vs pure Warehouse | Flexible ingestion from heterogeneous sources; time travel for auditing | Higher operational complexity; need to manage compaction and vacuum |
| Forecast every 15 minutes | Forecast always updated with recent weather data | Computational cost of frequent inference; complexity of feature freshness management |
| Isolated regulatory pipelines | Guaranteed immutability; independent traceability | Maintenance overhead of two parallel pipelines for some sources |
| Kafka for IoT telemetry | High throughput; message durability; replay for reprocessing | Need to manage Kafka cluster; complexity of per-farm topic configuration |
| OT/IT separation | Isolation of sensitive industrial networks; compliance with IEC 62443 | Additional latency on the OT→IT bridge; need for data gateways |

---

## 5. Technical Implementation

### Stack

```
Stream ingestion:    Apache Kafka (topics per farm/sensor type)
Batch ingestion:     Apache Airflow + Python (weather APIs, SFTP)
Stream processing:   Apache Spark Structured Streaming
Batch processing:    Apache Spark (PySpark) + Apache Airflow
Storage:             Delta Lake (Lakehouse) + PostgreSQL (aggregates serving)
Transformation:      dbt (analytical layer) + PySpark (operational layer)
ML serving:          FastAPI (Python) for forecasting service
Monitoring:          Grafana + Prometheus + AlertManager
Critical alerts:     PagerDuty
ML versioning:       MLflow
```

### Data Flow

```
Operational Sources            Ingestion                  Processing
───────────────────            ─────────                  ──────────

IoT Sensors           ─ MQTT ─► Kafka Broker
(turbines/panels)               [per topic]  ──► Spark Structured Streaming ──► Delta Lake
                                              └──► Real-time alerts               /raw/

Weather APIs          ─ HTTP ─► Airflow DAG ───────────────────────────────────► Delta Lake
(INMET, Open-Meteo)                                                                /raw/meteo/

SCADA / ONS           ─ SFTP ─► Airflow DAG ───────────────────────────────────► Delta Lake
(operational data)                                                                 /raw/scada/

Maintenance Events    ─ API ──► Kafka Broker ──► Spark Streaming ───────────────► Delta Lake
(CMMS)                                                                             /raw/events/

                                    │
                              [Spark Batch +]
                              [dbt models   ]
                                    │
                     ┌──────────────┼──────────────┐
              Delta Lake        Delta Lake      Delta Lake
              /trusted/         /business/      /regulatory/
                                    │
               ┌────────────────────┼──────────────────────┐
        [Forecasting     [Control Center        [Regulatory
         Service]         Grafana Dashboard]     Reports
         FastAPI]                                ANEEL/ONS]
```

### Lakehouse Zones

**raw/** — immutable raw data partitioned by `farm_id / year / month / day`. Each file has its SHA-256 hash registered in the data catalog for integrity verification. Nothing is deleted from this zone.

**trusted/** — data normalized to standard units (MW, kV, °C, m/s), with outliers identified and flagged with `quality_flag` (valid / estimated / suspect / missing). Telemetry gaps filled by linear interpolation with a mandatory flag. Per-batch checksums for auditing.

**business/** — time series aggregated by turbine/panel, by farm, and by period (15 min, hourly, daily). Enriched with weather forecasts and installed capacity data. Foundation for the forecasting model and the operational dashboard.

**regulatory/** — subset of data with additional immutability controls: write-once per settlement period, per-record integrity hash, full access log, and explicit correction versioning with auditable justification. Official source for all ANEEL/ONS reports.

### Generation Forecasting Pipeline

The per-farm prediction model uses:

- **Generation history**: 18–24-month series per farm, cleaned and with maintenance periods marked
- **Weather features**: solar irradiance (W/m²), wind speed and direction by height, ambient temperature, cloud cover, atmospheric pressure
- **Operational calendar**: scheduled maintenance, available capacity per turbine/panel
- **Regional seasonal patterns**: monthly wind seasonality, seasonal irradiance variation

The Airflow pipeline is triggered hourly to collect the updated weather forecast and generate the 24h and 72h forecasts per farm. The FastAPI service exposes the updated forecasts and publishes them to the Kafka topic `forecast.generation` for consumption by the Control Center.

```python
# Illustrative structure of the forecasting pipeline
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

def fetch_meteorological_forecast(farm_id: str) -> dict:
    """Collects updated weather forecast via API and persists it to the trusted zone."""
    ...

def compute_generation_features(farm_id: str, horizon_hours: int) -> pd.DataFrame:
    """Builds the feature vector for the requested horizon from the business zone."""
    ...

def run_forecast_inference(features: pd.DataFrame, model_version: str) -> pd.DataFrame:
    """Runs inference with the MLflow model registered as Production."""
    ...

def validate_forecast_sanity(forecast: pd.DataFrame) -> bool:
    """
    Validates that the forecast is within physical bounds:
    - Generation cannot exceed installed capacity
    - Generation cannot be negative
    - Variation between consecutive periods must be physically plausible
    """
    ...

with DAG("generation_forecast_hourly", schedule_interval="@hourly") as dag:
    fetch_meteo    = PythonOperator(task_id="fetch_meteorological_data", ...)
    build_features = PythonOperator(task_id="compute_features",          ...)
    run_inference  = PythonOperator(task_id="run_forecast_inference",    ...)
    validate       = ShortCircuitOperator(task_id="validate_forecast",   ...)
    publish        = PythonOperator(task_id="publish_to_kafka",          ...)

    fetch_meteo >> build_features >> run_inference >> validate >> publish
```

### Equipment Anomaly Detection

The anomaly detector processes the telemetry stream in real time via Spark Structured Streaming. It uses a sliding z-score calculated over a 7-day time window for each sensor of each piece of equipment, comparing the current value against the equipment's own historical behavior (rather than a global threshold, which would be insensitive to differences between equipment from different manufacturers and generations).

```python
# Illustrative anomaly detection logic in streaming
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def compute_rolling_zscore(df, sensor_col: str, window_days: int = 7):
    """
    Calculates the rolling z-score for a specific sensor,
    using an N-day window per equipment_id.
    Returns the dataframe with z-score and anomaly flag columns.
    """
    window_spec = (
        Window
        .partitionBy("equipment_id", "sensor_type")
        .orderBy(F.col("event_timestamp").cast("long"))
        .rangeBetween(-window_days * 86400, 0)
    )

    df = df.withColumn("rolling_mean", F.avg(sensor_col).over(window_spec))
    df = df.withColumn("rolling_std",  F.stddev(sensor_col).over(window_spec))
    df = df.withColumn(
        "zscore",
        (F.col(sensor_col) - F.col("rolling_mean")) / F.col("rolling_std")
    )
    df = df.withColumn(
        "is_anomaly",
        F.abs(F.col("zscore")) > F.lit(3.0)
    )
    return df
```

Detected anomalies are published to the Kafka topic `alerts.equipment` and consumed by AlertManager for notification of the maintenance team.

---

## 6. Observability and Governance

### Operational Monitoring with Grafana + Prometheus

Separate dashboards by audience:

**Control Center (operational)**
- Current generation per farm (MW) vs forecast (MW) — real-time gauge
- Accumulated deviation from scheduled dispatch in the current period
- Sensor connectivity status per farm (green / yellow / red)
- Active equipment alerts with severity and time since detection

**Data Team (infrastructure)**
- Kafka message throughput per topic (messages/second)
- Spark Streaming consumer lag per topic
- End-to-end telemetry latency (sensor → dashboard)
- Rate of records with `quality_flag` ≠ valid per farm
- Airflow DAG status and latency of critical pipelines

**Leadership / Regulatory**
- Generation performance per farm (MWh actual vs scheduled)
- Historical equipment availability per farm
- Regulatory report delivery status (delivered within SLA / delayed / pending)

### Data Quality

| Check | Layer | Frequency | Action on Failure |
|---|---|---|---|
| Sensor completeness per farm | trusted | Every 15 min | Immediate alert + interpolation with flag |
| Physical value range | trusted | Real-time (stream) | Quarantine + maintenance notification |
| Temporal consistency (gaps) | trusted | Every 15 min | Documented fill + `estimated` flag |
| Consistency between redundant sensors | trusted | Every hour | `suspect` flag + operational alert |
| Hash integrity (regulatory data) | regulatory | Per batch | Full block + critical alert + PagerDuty |
| Regulatory report completeness | regulatory | Per period | Critical alert + automatic escalation |

### Regulatory Traceability

Each record in the `regulatory/` zone has the following mandatory audit fields:

| Field | Description |
|---|---|
| `record_hash` | SHA-256 of the record's content before any transformation |
| `ingestion_timestamp` | Exact UTC timestamp of receipt by the pipeline |
| `source_system` | Identifier of the originating measurement system |
| `pipeline_version` | Pipeline version (git commit hash) that processed the record |
| `regulatory_period` | Regulatory settlement period (YYYY-MM-DD HH:MM) |
| `quality_flag` | `valid` / `estimated` / `suspect` — never omitted in regulatory data |
| `audit_user` | Identity of the process or user that wrote the record |

Any attempt to modify data in already-validated periods triggers a critical alert and is blocked by the access control layer. Legitimate corrections are recorded as new records with `corrects_record_hash` pointing to the original.

### Forecasting Model Versioning

Models are managed by MLflow with the following lifecycle:

1. **Staging**: newly trained model is registered with validation metrics
2. **Champion/Challenger**: candidate model is compared with the production model for 7 days in shadow mode
3. **Production**: promotion only if MAPE < 8% (24h) and < 15% (72h) — validated by the Airflow pipeline
4. **Archived**: previous model is archived (not deleted) for regulatory traceability

Automatic rollback is triggered if production performance degrades > 20% relative to the baseline measured at the time of promotion.

### Fault Tolerance

| Scenario | Resilience Mechanism |
|---|---|
| Individual sensor failure | Gap detection; interpolation with flag; maintenance alert |
| Kafka broker failure | 3-broker replication (RF=3); consumer groups with auto-recovery; offset replay |
| Weather API failure | Cache of last valid forecast with expiration timestamp; fallback to secondary provider |
| Airflow pipeline failure | Configurable auto-retry per DAG (3 attempts); alert after exhaustion |
| Forecasting service unavailability | Last valid forecast served with `forecast_age_minutes` field; alert if > 30 min |
| OT→IT connectivity loss | Local buffer at the data gateway with 24h capacity; automatic sync on reconnection |

---

## 7. Stakeholder Impact

### Operational Control Center

- **Consolidated visibility** across all farms in a single Grafana dashboard with < 30 second latency, replacing multiple fragmented monitoring systems
- **Dispatch deviation reduction**: forecast with MAPE < 8% at the 24h horizon enabled proactive scheduling adjustments, reducing deviation penalties by 60% in the first 6 months
- **Proactive alerts** allowed the Control Center to initiate contingency protocols before equipment anomalies impacted generation

### Maintenance Team

- **Predictive fault detection** through vibration and temperature pattern analysis reduced unplanned downtime by 40%, allowing 70% of interventions to be performed in planned maintenance windows
- **Complete per-equipment reading history** with 1-minute resolution, eliminating dependence on local turbine logs for fault diagnosis

### Regulatory Compliance

- **ANEEL/ONS reports generated automatically** by T+2h, eliminating a manual process that frequently missed the deadline
- **Full traceability** with integrity hashing and access logs eliminating the risk of regulatory challenges due to data inconsistency
- **Structured audit trail** reduced the response time to audit requests from weeks to hours

### Executive Leadership

- **60% reduction in dispatch deviation penalties** as a direct result of forecasting improvement
- **Structured historical baseline** enabling capacity expansion feasibility analyses with actual vs projected generation data
- **Consolidated per-farm performance view** for investment prioritization decisions in maintenance and upgrades

---

## 8. Next Steps

### Short Term (0–3 months)

- **Per-equipment predictive maintenance model**: expand the anomaly detector to classify the likely fault type (bearing wear, electrical issue, misalignment) based on historical vibration and temperature patterns, enabling more precise prioritization by the maintenance team
- **OPC-UA protocol for next-generation equipment**: standardize data collection from new turbines and inverters with OPC-UA instead of proprietary adapters, reducing the integration cost of new equipment

### Medium Term (3–6 months)

- **Data catalog with Apache Atlas or OpenMetadata**: implement a centralized metadata catalog for lineage traceability, sensitivity-based access control, and auto-documentation of data sources
- **Spot market features (PLD)**: integrate Settlement Price of Differences data to correlate dispatch decisions with market price, enabling financial optimization beyond operational
- **Migration of batch transformations to dbt + Spark**: standardize the analytical transformation layer with dbt for the governance, testing, and documentation gains already consolidated in the retail article

### Long Term (6–12 months)

- **Portfolio optimization model**: build a model that simultaneously balances forecasted generation, market prices (PLD), regulatory dispatch constraints, and equipment availability to maximize risk-adjusted net revenue
- **Renewable Energy Certificates (REC/I-REC)**: expand regulatory traceability to support the issuance and auditing of renewable energy certificates, enabling the company to participate in voluntary carbon credit markets
- **Cloud-native migration evaluation**: analyze the feasibility of migrating to Azure Synapse Analytics + Event Hubs or AWS MSK + Redshift to reduce the operational overhead of managing Kafka and Spark infrastructure on-premises

---

*Diagrams available at [diagrams/](./diagrams/)*  
*References at [references.md](./references.md)*
