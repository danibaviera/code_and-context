# Fluxo de Dados — Plataforma de Dados para Retail de Beleza

Detalhe do fluxo de dados desde as fontes até os consumidores finais, com tipos de ingestão e latências.

---

## Diagrama de Fluxo

```mermaid
flowchart LR
    subgraph Eventos_Stream["⚡ Stream (< 5 min)"]
        E1([Navegação])
        E2([Carrinho])
        E3([Abandono])
        E4([Busca])
    end

    subgraph Batch_Diario["🕐 Batch Diário"]
        B1([Transações PDV])
        B2([Snapshot Estoque])
        B3([Campanhas CRM])
        B4([Sessões App])
    end

    KAFKA[Kafka\nTópicos por domínio]
    AIRFLOW[Airflow\nDAGs batch]

    RAW_E[(raw.ecommerce_events)]
    RAW_P[(raw.pos_transactions)]
    RAW_I[(raw.inventory_snapshots)]
    RAW_C[(raw.campaign_events)]

    TRUSTED[(trusted.*\nDados limpos)]
    QUARANTINE[(quarantine.*\nErros isolados)]

    BUSINESS_C[(dim_customer\ncustomer_360)]
    BUSINESS_P[(dim_product\nproduct_attributes)]
    BUSINESS_S[(fct_sales\nomnichannel)]
    BUSINESS_CE[(fct_cart_events)]

    SERVING_REC[(customer_360\nfeatures ML)]
    SERVING_FORE[(product_demand\nfeatures)]
    SERVING_INV[(inventory_risk\nalertas)]

    FS[Feature Store]
    MODEL_REC[Modelo Recomendação]
    MODEL_FORE[Modelo Forecasting]

    API_REC[API Recomendações\nFastAPI]
    DASH_SC[Dashboard\nSupply Chain]
    CRM_OUT[CRM / Ads\nSegmentos]

    E1 & E2 & E3 & E4 --> KAFKA
    B1 --> AIRFLOW
    B2 --> AIRFLOW
    B3 --> AIRFLOW
    B4 --> AIRFLOW

    KAFKA --> RAW_E
    AIRFLOW --> RAW_P
    AIRFLOW --> RAW_I
    AIRFLOW --> RAW_C

    RAW_E & RAW_P & RAW_I & RAW_C -->|dbt trusted models| TRUSTED
    TRUSTED -->|registros inválidos| QUARANTINE

    TRUSTED -->|dbt business models| BUSINESS_C
    TRUSTED -->|dbt business models| BUSINESS_P
    TRUSTED -->|dbt business models| BUSINESS_S
    TRUSTED -->|dbt business models| BUSINESS_CE

    BUSINESS_C & BUSINESS_P & BUSINESS_S & BUSINESS_CE -->|dbt serving models| SERVING_REC
    BUSINESS_S & BUSINESS_P -->|dbt serving models| SERVING_FORE
    BUSINESS_S & SERVING_FORE -->|dbt serving models| SERVING_INV

    SERVING_REC --> FS
    FS --> MODEL_REC
    SERVING_FORE --> MODEL_FORE

    MODEL_REC --> API_REC
    MODEL_FORE --> DASH_SC
    SERVING_INV --> DASH_SC
    BUSINESS_C --> CRM_OUT
```

---

## Latências por Tipo de Dado

| Dado | Tipo de Ingestão | Latência Alvo |
|---|---|---|
| Eventos de navegação e carrinho | Stream (Kafka) | < 5 minutos |
| Transações de e-commerce | Stream (Kafka) | < 5 minutos |
| Transações PDV físico | Batch CDC (Airflow) | < 4 horas |
| Snapshot de estoque | Batch (Airflow) | < 2 horas |
| Campanhas CRM | Batch (Airflow) | Diário |
| Sessões app mobile | Batch API (Airflow) | < 4 horas |
| Features de ML (serving) | dbt após ingestão | T + 30 min |
| Segmentos RFM | Airflow semanal | Semanal |
| Forecasts de demanda | Airflow semanal | Semanal |
