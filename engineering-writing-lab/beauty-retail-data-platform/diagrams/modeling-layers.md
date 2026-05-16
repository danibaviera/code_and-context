# Camadas de Modelagem dbt — Plataforma de Dados para Retail de Beleza

Detalhe das camadas de transformação gerenciadas pelo dbt, com responsabilidades e exemplos de modelos.

---

## Diagrama de Camadas

```mermaid
flowchart TD
    subgraph RAW["Camada Raw — Dados brutos, imutáveis"]
        R1[raw.ecommerce_events]
        R2[raw.pos_transactions]
        R3[raw.inventory_snapshots]
        R4[raw.campaign_events]
        R5[raw.mobile_sessions]
    end

    subgraph TRUSTED["Camada Trusted — Dados validados e limpos"]
        T1[trusted.ecommerce_events]
        T2[trusted.pos_transactions]
        T3[trusted.inventory]
        T4[trusted.campaign_events]
        T5[trusted.mobile_sessions]
        TQ[quarantine.*\n⚠️ Registros inválidos]
    end

    subgraph BUSINESS["Camada Business — Entidades de negócio"]
        B1[dim_customer\nPerfil unificado omnichannel]
        B2[dim_product\nAtributos, categoria, margem]
        B3[fct_sales\nVendas por canal e SKU]
        B4[fct_cart_events\nCarrinho e abandono]
        B5[dim_campaign\nCampanhas e segmentos]
    end

    subgraph SERVING["Camada Serving — Pronto para consumo"]
        S1[customer_360\nFeatures ML por cliente]
        S2[product_demand_features\nFeatures forecasting por SKU]
        S3[campaign_performance\nPerformance por campanha]
        S4[inventory_risk\nRisco de ruptura por SKU]
        S5[rfm_segments\nSegmentação de clientes]
    end

    subgraph CONSUMERS["Consumidores"]
        C1[Feature Store\nRecomendação]
        C2[Modelo Forecasting\nDemanda]
        C3[Dashboard\nMarketing]
        C4[Dashboard\nSupply Chain]
        C5[CRM / Ads\nSegmentos]
    end

    R1 & R2 & R3 & R4 & R5 -->|limpeza, tipagem\ndeduplicação| TRUSTED
    TRUSTED -->|registros inválidos| TQ

    T1 & T2 & T5 -->|unificação por customer_key| B1
    T1 & T2 -->|atributos de produto| B2
    T1 & T2 -->|fatos de venda| B3
    T1 -->|eventos de carrinho| B4
    T4 -->|atributos de campanha| B5

    B1 & B3 & B4 -->|features de cliente| S1
    B2 & B3 & T3 -->|features de produto| S2
    B3 & B5 -->|métricas por campanha| S3
    T3 & S2 -->|risco por SKU| S4
    B1 & B3 -->|scores RFM| S5

    S1 --> C1
    S2 --> C2
    S3 --> C3
    S4 --> C4
    S5 --> C5
```

---

## Contrato de Qualidade por Camada

| Camada | Testes Obrigatórios | Ação em Falha |
|---|---|---|
| **trusted** | `not_null` em PKs e datas; `unique` em PKs; `accepted_values` em enums | Mover para quarentena; não promover para business |
| **business** | `relationships` entre fatos e dims; `not_null` em FKs; `unique` em PKs | Falhar o pipeline; alertar #data-quality |
| **serving** | `row_count_anomaly`; `not_null` em todas as features; range de valores | Falhar o pipeline; bloquear update na feature store |

---

## Exemplos de Modelos dbt

### `dim_customer` — Perfil unificado omnichannel

```sql
-- models/business/dim_customer.sql
-- Unifica o cliente entre e-commerce, PDV e app com resolução de entidade por e-mail/CPF
SELECT
    COALESCE(ec.customer_id, pos.customer_id)           AS customer_id,
    COALESCE(ec.email, pos.email)                       AS email,
    COALESCE(ec.cpf_hash, pos.cpf_hash)                 AS cpf_hash,
    COALESCE(ec.first_name, pos.first_name)             AS first_name,
    MIN(COALESCE(ec.created_at, pos.created_at))        AS customer_since,
    CASE
        WHEN ec.customer_id IS NOT NULL
         AND pos.customer_id IS NOT NULL THEN 'omnichannel'
        WHEN ec.customer_id IS NOT NULL  THEN 'digital_only'
        ELSE 'physical_only'
    END                                                  AS channel_profile
FROM trusted.ecommerce_customers ec
FULL OUTER JOIN trusted.pos_customers pos
    ON ec.cpf_hash = pos.cpf_hash
    OR ec.email = pos.email
```

### `inventory_risk` — Risco de ruptura

```sql
-- models/serving/inventory_risk.sql
-- Identifica SKUs com risco de ruptura baseado em estoque atual e previsão de demanda
SELECT
    i.sku_id,
    i.current_stock,
    f.forecast_30d,
    i.reorder_point,
    CASE
        WHEN i.current_stock < f.forecast_30d * 0.15 THEN 'critical'
        WHEN i.current_stock < f.forecast_30d * 0.30 THEN 'warning'
        ELSE 'ok'
    END                                   AS risk_level,
    ROUND(i.current_stock / NULLIF(f.daily_avg_demand, 0)) AS days_of_stock
FROM trusted.inventory i
LEFT JOIN serving.product_demand_features f USING (sku_id)
```
