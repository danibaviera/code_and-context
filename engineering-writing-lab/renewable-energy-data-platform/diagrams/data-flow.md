# Fluxo de Dados — Plataforma de Dados para Energia Renovável

Detalhe do fluxo de dados separado por velocidade de ingestão, com SLAs e destinos para cada tipo de dado.

---

## Diagrama de Fluxo — Speed Layer (Streaming)

```mermaid
flowchart LR
    subgraph SENSORS["Sensores IoT (~500/parque)"]
        T1([Potência gerada\nMW])
        T2([Tensão / Corrente\nkV / A])
        T3([Temperatura\n°C])
        T4([Vibração\nm/s²])
        T5([Irradiância\nW/m²])
    end

    KAFKA_TELEM[Kafka\ntopic: telemetry.{parque_id}]
    KAFKA_ALERTS[Kafka\ntopic: alerts.equipment]

    SPARK_STREAM[Spark Structured Streaming\nJanela deslizante 7 dias]

    ANOMALY{Z-score\n> 3.0?}

    RAW_TELEM[(Delta Lake\nraw/telemetry/)]
    TRUSTED_TELEM[(Delta Lake\ntrusted/telemetry/\nnormalizado + flags)]

    GRAFANA[Grafana\nCentro de Controle\n< 30s latência]
    PAGERDUTY[AlertManager\nPagerDuty\nEquipe Manutenção]

    T1 & T2 & T3 & T4 & T5 --> KAFKA_TELEM
    KAFKA_TELEM --> SPARK_STREAM
    SPARK_STREAM --> RAW_TELEM
    SPARK_STREAM --> ANOMALY
    ANOMALY -->|sim| KAFKA_ALERTS
    ANOMALY -->|não| TRUSTED_TELEM
    KAFKA_ALERTS --> PAGERDUTY
    TRUSTED_TELEM --> GRAFANA
```

---

## Diagrama de Fluxo — Batch Layer

```mermaid
flowchart TD
    subgraph BATCH_SOURCES["Fontes Batch"]
        METEO([APIs Meteorológicas\nINMET / Open-Meteo])
        SCADA_SRC([SCADA / ONS\nSFTP diário])
        MANUT([CMMS\nEventos de manutenção])
    end

    AIRFLOW[Airflow DAGs\nOrquestração batch]

    subgraph RAW_ZONE["Delta Lake — raw/"]
        RAW_M[(raw/meteorological/\nParticionado por parque/data)]
        RAW_S[(raw/scada/\nDados operacionais oficiais)]
        RAW_E[(raw/maintenance_events/)]
    end

    subgraph TRUSTED_ZONE["Delta Lake — trusted/"]
        TR_M[(trusted/meteorological/\nNormalizado, gaps preenchidos)]
        TR_S[(trusted/operational/\nValidado + checksums)]
        TR_E[(trusted/maintenance/\nEventos estruturados)]
    end

    subgraph BUSINESS_ZONE["Delta Lake — business/"]
        BUS_GEN[(business/generation_series/\nSérie temporal por parque)]
        BUS_EQUIP[(business/equipment_history/\nHistórico por equipamento)]
        BUS_FORE[(business/forecast_features/\nFeatures para forecasting)]
    end

    subgraph REGULATORY_ZONE["Delta Lake — regulatory/\n⚠️ Write-once • Hash SHA-256 • Audit log"]
        REG[(regulatory/metering/\nMedição oficial por período)]
        REG_LOG[(regulatory/audit_log/\nLog de acesso completo)]
    end

    subgraph CONSUMERS["Consumidores Finais"]
        FORE_SVC[Forecasting Service\nFastAPI]
        REPORTS[Relatórios ANEEL/ONS\nAté T+2h]
        DASH[Grafana\nPerformance histórica]
    end

    METEO --> AIRFLOW
    SCADA_SRC --> AIRFLOW
    MANUT --> AIRFLOW

    AIRFLOW --> RAW_M & RAW_S & RAW_E

    RAW_M -->|dbt trusted models| TR_M
    RAW_S -->|dbt trusted models| TR_S
    RAW_E -->|dbt trusted models| TR_E

    TR_M & TR_S -->|dbt business models| BUS_GEN
    TR_S & TR_E -->|dbt business models| BUS_EQUIP
    BUS_GEN & TR_M -->|Spark feature engineering| BUS_FORE

    TR_S -->|pipeline regulatório isolado| REG
    REG -->|log automático de acesso| REG_LOG

    BUS_FORE --> FORE_SVC
    REG --> REPORTS
    BUS_GEN & BUS_EQUIP --> DASH
```

---

## SLAs por Tipo de Dado

| Dado | Frequência de Ingestão | SLA de Disponibilidade | Zona de Destino |
|---|---|---|---|
| Telemetria de sensores | Contínua (1 leitura/min por sensor) | < 30 segundos no dashboard | trusted/telemetry/ |
| Alertas de anomalia | Em tempo real | < 60 segundos para PagerDuty | Kafka topic → AlertManager |
| Previsão meteorológica | A cada hora | < 5 min após publicação da API | raw/meteorological/ |
| Dados SCADA operacionais | A cada 15 min | < 30 min | raw/scada/ |
| Features de forecasting | A cada 15 min (pipeline Airflow) | T + 15 min | business/forecast_features/ |
| Forecasts de geração publicados | A cada 15 min | T + 5 min após atualização de features | Kafka + Grafana |
| Dados de medição oficial | Por período de competência | Disponível para regulatory/ imediatamente | regulatory/metering/ |
| Relatórios regulatórios | Por período de competência | T + 2 horas do fim do período | Output ANEEL/ONS |
