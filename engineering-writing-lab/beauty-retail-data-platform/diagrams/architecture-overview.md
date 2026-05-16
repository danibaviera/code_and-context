# Arquitetura Geral — Plataforma de Dados para Retail de Beleza

Visão de alto nível dos componentes e suas responsabilidades na plataforma.

---

## Diagrama de Arquitetura

```mermaid
graph TB
    subgraph Sources["Fontes de Dados"]
        EC[E-commerce<br/>Web & Mobile]
        PDV[PDV Físico<br/>Sistema de Caixa]
        APP[App Mobile]
        ERP[ERP / Estoque]
        CRM[CRM / Campanhas]
    end

    subgraph Ingestion["Camada de Ingestão"]
        KAFKA[Apache Kafka<br/>Eventos comportamentais]
        AIRFLOW_I[Airflow DAGs<br/>Batch & CDC]
    end

    subgraph Storage["Armazenamento"]
        RAW[(raw.*<br/>Dados brutos)]
        TRUSTED[(trusted.*<br/>Dados validados)]
        BUSINESS[(business.*<br/>Entidades de negócio)]
        SERVING[(serving.*<br/>Pronto para consumo)]
    end

    subgraph Transform["Transformação"]
        DBT[dbt<br/>SQL models + testes]
    end

    subgraph ML["Camada de ML"]
        FS[Feature Store<br/>PostgreSQL + Redis]
        REC[Modelo de<br/>Recomendação]
        FORE[Modelo de<br/>Forecasting]
        SEG[Segmentação<br/>RFM]
    end

    subgraph Serving_Layer["Serving"]
        API[FastAPI<br/>Recomendações]
        DASH[React Dashboard<br/>Supply Chain]
        ALERTS[AlertManager<br/>Ruptura & Qualidade]
    end

    EC -->|eventos stream| KAFKA
    APP -->|eventos stream| KAFKA
    PDV -->|CDC| AIRFLOW_I
    ERP -->|batch diário| AIRFLOW_I
    CRM -->|batch diário| AIRFLOW_I

    KAFKA --> RAW
    AIRFLOW_I --> RAW

    RAW --> DBT
    DBT --> TRUSTED
    TRUSTED --> DBT
    DBT --> BUSINESS
    BUSINESS --> DBT
    DBT --> SERVING

    SERVING --> FS
    FS --> REC
    FS --> FORE
    FS --> SEG

    REC --> API
    FORE --> DASH
    SEG --> ALERTS
    DASH --> ALERTS
```

---

## Responsabilidades por Componente

| Componente | Responsabilidade | Tecnologia |
|---|---|---|
| **Kafka** | Barramento de eventos comportamentais em tempo quase real | Apache Kafka |
| **Airflow DAGs** | Orquestração de pipelines batch, CDC e retreinamento de modelos | Apache Airflow |
| **raw.*** | Persistência imutável dos dados brutos por fonte | PostgreSQL |
| **trusted.*** | Dados limpos, tipados e deduplicated | PostgreSQL + dbt |
| **business.*** | Dimensões e fatos de negócio consolidados | PostgreSQL + dbt |
| **serving.*** | Modelos otimizados para ML e dashboards | PostgreSQL + dbt |
| **Feature Store** | Centralização de features com consistência treino/produção | PostgreSQL + Redis |
| **FastAPI** | Serving de recomendações para frontend com baixa latência | Python / FastAPI |
| **React Dashboard** | Visibilidade operacional para supply chain e marketing | React |
