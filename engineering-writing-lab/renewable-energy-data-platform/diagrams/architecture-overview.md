# Arquitetura Geral — Plataforma de Dados para Energia Renovável

Visão de alto nível da arquitetura Lambda híbrida, com separação entre speed layer (streaming) e batch layer, e zona regulatória isolada.

---

## Diagrama de Arquitetura

```mermaid
graph TB
    subgraph OT["Rede OT — Operational Technology"]
        SENSORS[Sensores IoT<br/>Turbinas & Painéis]
        SCADA[SCADA<br/>Supervisório]
        CMMS[CMMS<br/>Manutenção]
    end

    subgraph IT_INGESTION["Camada de Ingestão — Rede IT"]
        GW[Gateway OT→IT<br/>Buffer 24h]
        KAFKA[Apache Kafka<br/>Tópicos por parque/tipo]
        AIRFLOW_I[Airflow DAGs<br/>APIs & SFTP]
    end

    subgraph METEO["Fontes Externas"]
        INMET[INMET API]
        OPENMETEO[Open-Meteo API]
        ONS_SFTP[ONS / ANEEL<br/>SFTP]
    end

    subgraph SPEED["Speed Layer — Spark Structured Streaming"]
        SPARK_S[Spark Streaming<br/>Processamento em tempo real]
        ANOMALY[Detector de Anomalias<br/>Z-score deslizante]
        ALERT_STREAM[Tópico Kafka<br/>alerts.equipment]
    end

    subgraph BATCH["Batch Layer — Spark + Airflow"]
        SPARK_B[Spark Batch<br/>Histórico e regulatório]
        DBT[dbt<br/>Camada analítica]
    end

    subgraph LAKEHOUSE["Delta Lakehouse"]
        RAW[(raw/<br/>Dados brutos imutáveis)]
        TRUSTED[(trusted/<br/>Validados e normalizados)]
        BUSINESS[(business/<br/>Séries temporais agregadas)]
        REGULATORY[(regulatory/<br/>Dados imutáveis auditáveis)]
    end

    subgraph SERVING_L["Serving & Consumidores"]
        FORECAST_SVC[Forecasting Service<br/>FastAPI - atualização 15 min]
        GRAFANA[Grafana Dashboard<br/>Centro de Controle]
        REPORTS[Relatórios Regulatórios<br/>ANEEL / ONS]
        PAGERDUTY[AlertManager<br/>PagerDuty]
    end

    SENSORS -->|MQTT / OPC| GW
    SCADA -->|OPC-DA / IEC 61850| GW
    CMMS -->|API REST| KAFKA
    GW --> KAFKA

    INMET & OPENMETEO -->|HTTP API horária| AIRFLOW_I
    ONS_SFTP -->|SFTP batch| AIRFLOW_I

    KAFKA --> SPARK_S
    SPARK_S --> RAW
    SPARK_S --> ANOMALY
    ANOMALY --> ALERT_STREAM
    ALERT_STREAM --> PAGERDUTY

    AIRFLOW_I --> RAW

    RAW --> SPARK_B
    RAW --> DBT
    SPARK_B --> TRUSTED
    DBT --> TRUSTED
    TRUSTED --> DBT
    DBT --> BUSINESS
    DBT --> REGULATORY

    BUSINESS --> FORECAST_SVC
    BUSINESS --> GRAFANA
    REGULATORY --> REPORTS
```

---

## Separação OT / IT e Controles de Segurança

```mermaid
graph LR
    subgraph OT_NET["Rede OT (Isolada)"]
        S1[Turbina 01<br/>Sensor cluster]
        S2[Turbina 02<br/>Sensor cluster]
        S3[Painel Solar<br/>Inversor]
    end

    subgraph DMZ["DMZ — Gateway"]
        GW[Data Gateway<br/>Buffer local 24h<br/>Validação de protocolo]
    end

    subgraph IT_NET["Rede IT"]
        KAFKA[Kafka Broker<br/>Cluster 3 nós]
        SPARK[Spark Cluster]
        LAKE[Delta Lake]
    end

    S1 & S2 & S3 -->|MQTT / Modbus TCP| GW
    GW -->|Protocolo validado<br/>One-way data diode| KAFKA
    KAFKA --> SPARK
    SPARK --> LAKE

    style OT_NET fill:#fff3cd
    style DMZ fill:#f8d7da
    style IT_NET fill:#d4edda
```

---

## Responsabilidades por Componente

| Componente | Responsabilidade | Tecnologia |
|---|---|---|
| **Kafka** | Barramento de telemetria de alta frequência com replay e durabilidade | Apache Kafka (RF=3) |
| **Spark Streaming** | Processamento de telemetria em tempo real; detecção de anomalias | Spark Structured Streaming |
| **Spark Batch** | Processamento histórico; agregações; reprocessamento regulatório | Apache Spark (PySpark) |
| **Airflow** | Orquestração de ingestão de APIs externas, retreinamento de modelos, relatórios | Apache Airflow |
| **Delta Lake** | Armazenamento com ACID transactions, time travel e schema evolution | Delta Lake / S3-compatible |
| **dbt** | Modelagem analítica com testes de qualidade e lineage | dbt (Spark adapter) |
| **Forecasting Service** | Serving de previsões de geração com atualização a cada 15 min | Python / FastAPI + MLflow |
| **Grafana** | Dashboard operacional do Centro de Controle | Grafana + Prometheus |
| **AlertManager** | Roteamento de alertas por severidade e equipe responsável | AlertManager + PagerDuty |
