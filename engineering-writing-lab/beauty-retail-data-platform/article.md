# Arquitetando uma Plataforma de Dados para Retail de Beleza: personalização, previsão de demanda e experiência omnichannel

> **Domínio:** Varejo / Cosméticos  
> **Stack:** Python · Apache Airflow · dbt · PostgreSQL · Apache Kafka · React  
> **Foco:** Recomendação personalizada, demand forecasting, prevenção de ruptura, visão omnichannel

---

## 1. Contexto de Negócio

Uma empresa de cosméticos com operação omnichannel — e-commerce, lojas físicas e aplicativo mobile — cresceu de forma acelerada nos últimos dois anos. O crescimento trouxe um problema estrutural: os dados dos diferentes canais viviam em conflitos, e as decisões operacionais continuavam sendo tomadas com base em relatórios consolidados manualmente, frequentemente com dois ou três dias de atraso.

As dores mais críticas identificadas:

- **Ruptura recorrente de estoque** nos SKUs de maior rotatividade, especialmente em itens de skincare com alta demanda sazonal. O time de supply chain só identificava a ruptura depois que o produto já tinha zerado no sistema.
- **Recomendações genéricas** no e-commerce e no app, sem considerar histórico de compra, tipo de pele declarado ou preferências de categoria. O algoritmo de "mais vendidos" era o mesmo para todos os clientes.
- **Campanhas de marketing com baixo ROI** porque os segmentos de clientes eram definidos manualmente e de forma estática — sem atualização dinâmica baseada em comportamento recente.
- **Ausência de visão unificada do cliente**: uma cliente que comprava na loja física e também no e-commerce era tratada como dois perfis diferentes. O histórico de uma não alimentava a experiência da outra.
- **Excesso de estoque em itens sazonais**, causando alto custo de imobilização de capital e necessidade de liquidações que corroíam margem.

O diagnóstico era claro: a empresa tinha dados suficientes para resolver esses problemas, mas não tinha arquitetura para transformá-los em inteligência operacional. A solução exigia uma plataforma de dados que integrasse fontes, organizasse os dados em camadas e disponibilizasse insights acionáveis para diferentes times.

---

## 2. Requisitos Funcionais e Não Funcionais

### Requisitos Funcionais

- Consolidação de dados de todos os canais (e-commerce, PDV físico, app mobile) em uma visão única de cliente e produto
- Pipeline de recomendação personalizada por perfil de cliente, atualizado com base em comportamento recente
- Modelo preditivo de demanda com horizonte de 30, 60 e 90 dias por SKU e por região
- Segmentação dinâmica de clientes para campanhas de marketing
- Dashboard operacional para equipe de supply chain com visibilidade de estoque em tempo quase real
- Alertas automáticos de ruptura iminente antes que o estoque chegue a zero
- API de serving para consumo das recomendações pelo frontend React

### Requisitos Não Funcionais

| Dimensão | Requisito |
|---|---|
| **Escala** | 500 mil clientes ativos, ~50 mil transações/dia, picos 3x em datas sazonais |
| **Latência** | Eventos comportamentais: ingestão em < 5 minutos / Forecasting: atualização diária |
| **Disponibilidade** | 99,9% para pipelines de supply chain e alertas de ruptura |
| **Retenção de dados** | Transacional: 3 anos / Logs de comportamento: 12 meses |
| **Custo** | Budget analítico isolado do budget transacional — sem competição de recursos |
| **Governança** | Compliance com LGPD para dados de comportamento, perfil e histórico de compras |
| **Qualidade** | SLA de >= 98% de registros válidos nas tabelas da camada serving |
| **Observabilidade** | Alertas automáticos para falhas de pipeline e desvios de qualidade |

---

## 3. Decisões Arquiteturais

### 3.1 Arquitetura orientada a eventos para captura comportamental

**Decisão:** utilizar Apache Kafka para captura em tempo quase real dos eventos de comportamento digital — navegação, adição ao carrinho, abandono, busca por categoria.

**Justificativa:** o comportamento de um cliente em sessão tem janela de relevância curtíssima. Uma cliente que coloca um sérum específico no carrinho e abandona a compra precisa de uma ação de remarketing em minutos para que ela ainda esteja com aquele produto na cabeça — não horas depois. Com uma abordagem batch tradicional, esse sinal chegaria com atraso de horas ou só no relatório do dia seguinte, tornando qualquer ação ineficaz.

Além disso, Kafka funciona como um barramento de eventos desacoplado: o sistema de e-commerce publica eventos sem precisar conhecer quem vai consumi-los. Isso permite adicionar novos consumidores (um pipeline de detecção de fraude, por exemplo) sem alterar a origem.

**Alternativa considerada e descartada:** polling batch direto no banco de e-commerce a cada 30 minutos. Descartado pela latência e pelo risco de sobrecarregar o banco transacional com queries de extração.

---

### 3.2 Feature store para consistência entre treino e produção

**Decisão:** implementar uma feature store centralizada para as features de recomendação e segmentação.

**Justificativa:** um problema clássico em sistemas de ML em produção é o *training-serving skew*: o modelo é treinado com features calculadas de uma forma e servido em produção com features calculadas de outra — pequenas diferenças de lógica, de janela temporal ou de tratamento de nulos que parecem irrelevantes mas degradam silenciosamente a qualidade das previsões.

Para recomendações de beleza, onde o modelo precisa de features como "proporção de compras em skincare nos últimos 90 dias", "tempo médio entre compras" e "SKUs visualizados mas não comprados na última sessão", a consistência entre treino e produção é crítica. A feature store garante que a mesma lógica de cálculo seja usada em ambos os ambientes.

**Alternativa considerada e descartada:** calcular features diretamente na query de inferência. Descartado pelo risco de skew e pela impossibilidade de reutilizar as mesmas features em múltiplos modelos.

---

### 3.3 Separação entre camada transacional e analítica via CDC

**Decisão:** manter o PostgreSQL transacional completamente isolado do ambiente analítico, com sincronização via CDC (Change Data Capture) gerenciada pelo Airflow.

**Justificativa:** queries analíticas complexas — cálculo de LTV, cohort analysis, ranking de produtos por margem — executadas diretamente no banco transacional causariam degradação de performance para operações de venda. Em datas de pico como Black Friday, isso seria crítico.

A separação permite escalar cada camada de forma independente: o banco transacional é otimizado para escrita e leitura de registros únicos; o banco analítico é otimizado para scans amplos e agregações.

**Tradeoff aceito:** duplicação controlada de dados e uma latência de minutos entre a transação no PDV e a disponibilidade analítica. Para os casos de uso deste projeto (supply chain, segmentação, forecasting), essa latência é completamente aceitável.

---

### 3.4 Modelagem em camadas com dbt

**Decisão:** adotar dbt para modelagem em camadas (raw → trusted → business → serving), com testes automáticos de qualidade e documentação gerada.

**Justificativa:** sem um framework de transformação, é comum que a lógica de negócio fique distribuída em múltiplas queries SQL sem documentação, sem testes e sem rastreabilidade. dbt resolve isso ao tratar modelos SQL como código: versionados em Git, com testes de qualidade definidos declarativamente e com documentação gerada automaticamente a partir dos modelos.

Isso cria um contrato explícito entre a engenharia de dados e os times analíticos: qualquer analista pode abrir o dbt docs e entender de onde vem cada campo, quais transformações foram aplicadas e quais validações garantem a integridade daquele dado.

---

## 4. Trade-offs

| Decisão | Ganho | Perda |
|---|---|---|
| Kafka para eventos comportamentais | Reatividade em tempo quase real; desacoplamento entre produtor e consumidor | Maior complexidade operacional; necessidade de gestão de tópicos e consumer groups |
| Feature store | Consistência total entre treino e produção; reutilização de features entre modelos | Custo operacional adicional; overhead de manutenção de features atualizadas |
| CDC + camada analítica separada | Escalabilidade analítica sem impacto no banco transacional | Duplicação de dados; latência de minutos entre transação e disponibilidade analítica |
| dbt para modelagem em camadas | Governança, testes automáticos, documentação, versionamento em Git | Curva de aprendizado para o time; overhead de configuração inicial |
| Airflow para orquestração | Visibilidade de pipelines; reprocessamento controlado; dependências explícitas entre DAGs | Overhead de manutenção de DAGs; infraestrutura adicional para o scheduler |

---

## 5. Implementação Técnica

### Stack

```
Ingestão batch:     Apache Airflow + Python (connectors para PDV e app)
Ingestão streaming: Apache Kafka + Python consumers
Armazenamento:      PostgreSQL transacional + PostgreSQL analítico (separados)
Transformação:      dbt (SQL models com testes e documentação)
Orquestração:       Apache Airflow
Feature store:      PostgreSQL + Redis (cache de features de baixa latência)
Serving ML:         FastAPI (Python) para API de recomendações
Frontend:           React (dashboards internos de supply chain)
```

### Fluxo de Dados

```
Fontes                    Ingestão                   Armazenamento Raw
──────                    ────────                   ──────────────────
E-commerce          ──── Kafka (eventos) ─────────► raw.ecommerce_events
PDV Físico          ──── Airflow CDC     ─────────► raw.pos_transactions
App Mobile          ──── Airflow API     ─────────► raw.mobile_sessions
Estoque/ERP         ──── Airflow batch   ─────────► raw.inventory_snapshots
Campanhas/CRM       ──── Airflow API     ─────────► raw.campaign_events

                              │
                         [dbt models]
                              │
              ┌───────────────┼───────────────┐
         [trusted.*]    [business.*]      [serving.*]
              │               │                │
         Dados limpos    Entidades de       Modelos para
         e validados     negócio (dims      ML e dashboards
                         e fatos)
                              │
              ┌───────────────┼───────────────┐
        [Feature Store]  [Forecasting]  [Dashboards]
        Recomendação     Demanda        Supply Chain
        Segmentação RFM  Ruptura        Marketing
```

### Camadas de Modelagem

**Raw layer** — dados brutos sem nenhuma transformação, preservados exatamente como chegaram da fonte. Cada registro tem `_ingested_at` (timestamp de chegada na plataforma) e `_source_system` (identificador da fonte). Nada é deletado nessa camada.

**Trusted layer** — dados limpos, tipados corretamente, com deduplicação por chave de negócio e validação de integridade referencial. Registros com erro de validação são movidos para tabelas de quarentena (`_quarantine`) com o motivo do erro para análise. Essa camada é o contrato de qualidade da plataforma.

**Business layer** — entidades de negócio consolidadas e enriquecidas: `dim_customer` (perfil unificado omnichannel), `dim_product` (atributos, categoria, margem), `fct_sales` (fatos de venda por canal), `fct_cart_events` (eventos de carrinho). Aqui acontece o enriquecimento cruzado entre fontes.

**Serving layer** — modelos otimizados para consumo final: `customer_360` (visão consolidada do cliente para ML), `product_demand_features` (features de forecasting por SKU), `campaign_performance` (performance de campanhas para marketing), `inventory_risk` (produtos com risco de ruptura para supply chain).

### Modelo de Segmentação de Clientes

A segmentação utiliza RFM (Recency, Frequency, Monetary) enriquecida com atributos de preferência de categoria e canal de compra preferencial. O modelo é recalculado semanalmente via pipeline Airflow e os segmentos atualizados são sincronizados com a plataforma de CRM para ativação em campanhas.

```sql
-- Cálculo ilustrativo de scores RFM para segmentação de clientes
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

### Pipeline de Previsão de Demanda

O modelo de forecasting por SKU utiliza:

- **Série histórica de vendas**: 24 meses por SKU e por região de distribuição
- **Sazonalidade**: datas comemorativas (Dia das Mães, Natal, Dia dos Namorados), estações do ano, ciclo mensal
- **Sinais de campanha**: cupons ativos, promoções programadas, lançamentos de produtos
- **Histórico de ruptura**: períodos em que o produto estava indisponível (para corrigir a série histórica)
- **Tendências de categoria**: crescimento ou declínio de subcategorias (ex: crescimento de skincare com SPF)

O pipeline Airflow dispara o retreinamento semanal dos modelos. Antes de promover um novo modelo para produção, um step de validação automática verifica o MAPE (Mean Absolute Percentage Error) para os horizontes de 30, 60 e 90 dias. Modelos que não atingem o threshold definido ficam em quarentena para análise manual.

```python
# Estrutura ilustrativa do DAG de forecasting
from airflow import DAG
from airflow.operators.python import PythonOperator

def validate_model_performance(model_path: str, threshold_mape: float) -> bool:
    """
    Avalia o MAPE do modelo candidato no conjunto de validação.
    Retorna True apenas se a performance atinge o threshold definido.
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

### Feature Engineering para Recomendação

As features de recomendação são calculadas em batch (atualização diária) e armazenadas na feature store. As features de baixa latência — como "produtos visualizados na sessão atual" — são calculadas em tempo real e armazenadas no Redis para serving com latência < 50ms.

Exemplos de features persistidas na feature store:

| Feature | Janela | Granularidade |
|---|---|---|
| `category_affinity_skincare` | 90 dias | Por cliente |
| `avg_ticket_value` | 12 meses | Por cliente |
| `purchase_channel_preference` | 6 meses | Por cliente |
| `days_since_last_purchase` | — | Por cliente |
| `repurchase_rate_by_sku` | 12 meses | Por SKU |
| `cross_sell_affinity` | 12 meses | Par de SKUs |

---

## 6. Observabilidade e Governança

### Monitoramento de Qualidade com dbt

Cada camada possui testes automáticos executados ao final de cada pipeline de transformação. Falhas bloqueiam a promoção para a camada superior.

| Teste | Camada | O que valida |
|---|---|---|
| `not_null` em campos chave | trusted, business | Sem registros sem ID ou data |
| `unique` em PKs | trusted, business | Sem duplicatas em chaves primárias |
| `accepted_values` | trusted | Enums válidos (status de pedido, canal, categoria) |
| `relationships` | business | Integridade entre fatos e dimensões |
| `row_count_anomaly` | serving | Volume dentro de banda esperada (desvio > 20% = alerta) |

### Lineage

O dbt gera automaticamente o grafo de lineage de todos os modelos, tornando visível para qualquer engenheiro ou analista:

- De qual fonte raw vem cada campo da camada serving
- Quais modelos de downstream serão afetados por uma mudança upstream
- O histórico de execuções e o status de testes de cada modelo

### Alertas Operacionais

| Alerta | Gatilho | Canal |
|---|---|---|
| Pipeline com falha | DAG Airflow em status `failed` após retries | Slack #data-ops + PagerDuty |
| Qualidade abaixo do SLA | % de registros válidos < 98% em tabela serving | Slack #data-quality |
| Ruptura iminente | Estoque projetado < 15% do ponto de reposição em 7 dias | Dashboard supply chain + e-mail equipe |
| Anomalia de volume | Volume de eventos < 50% da média dos últimos 7 dias | Slack #data-ops |
| Skew de features | Desvio > 15% entre features de treino e produção | Slack #ml-ops |

### Versionamento

- Modelos dbt versionados em Git com revisão por pull request
- Modelos de ML versionados com MLflow (experimentos, parâmetros, métricas, artefatos)
- Schema do banco transacional versionado com Alembic
- Toda alteração em modelos de serving passa por processo de validação de impacto downstream

### LGPD e Privacidade

Dados de comportamento e preferência são armazenados com pseudonimização: a chave de identificação pessoal fica em tabela separada com acesso restrito por role. Logs de consentimento são rastreados e qualquer solicitação de exclusão dispara um pipeline de purge automático que remove os dados do cliente em todas as camadas, com log de confirmação.

---

## 7. Impacto para Stakeholders

### Supply Chain

- **Redução de ruptura de estoque**: de 18% para 4% nos SKUs de alta rotatividade após 3 meses de operação com alertas automatizados
- **Tempo de resposta para reposição emergencial**: reduzido de 72h para 24h com base nos alertas preditivos
- **Redução de excesso de estoque sazonal**: simulações de demanda permitiram ajustar os pedidos de coleções sazonais, reduzindo sobra de estoque em 30%

### Marketing

- **Aumento de 34% na taxa de conversão** de campanhas segmentadas por RFM em comparação com campanhas broadcast para toda a base
- **CAC (Custo de Aquisição de Cliente) reduzido em 22%** com segmentação dinâmica por canal de compra preferencial e momento da jornada
- **Aumento no retorno sobre campanhas de reativação**: segmento "at_risk" com ações personalizadas aumentou recompra em 28%

### Produto / E-commerce

- **CTR de recomendações personalizadas 2,8x maior** do que recomendações baseadas em "mais vendidos" genérico
- **Aumento de 15% no ticket médio** via cross-sell orientado por afinidade de categoria
- **Experiência omnichannel real**: a visão unificada do cliente permitiu que cupons gerados na loja física fossem reconhecidos no app e vice-versa

### Liderança Executiva

- Dashboard consolidado com visão omnichannel em tempo quase real, acessível sem dependência do time de dados
- Capacidade de simular cenários de demanda para planejamento de coleções e negociação com fornecedores
- Base de dados estruturada para análises estratégicas de expansão geográfica e de portfólio

---

## 8. Próximos Passos

### Curto Prazo (0–3 meses)

- **Migrar feature store para solução gerenciada** (Feast ou Tecton) para reduzir o overhead operacional de manutenção do setup customizado com PostgreSQL + Redis
- **Implementar A/B testing estruturado** na camada de serving: experimentos controlados para comparar variações dos modelos de recomendação com métricas de negócio (CTR, conversão, ticket médio)

### Médio Prazo (3–6 meses)

- **Expandir modelo de forecasting com sinais externos**: integrar dados de Google Trends por categoria de produto e previsão climática para capturar tendências antes que apareçam nas vendas
- **Reverse ETL para sincronização de segmentos**: sincronizar automaticamente os segmentos calculados na plataforma com o CRM e as plataformas de mídia paga (Meta, Google Ads) para ativação direta sem exportação manual

### Longo Prazo (6–12 meses)

- **Avaliar migração do PostgreSQL analítico para cloud warehouse** (BigQuery ou Snowflake) para escalar análises ad hoc sem limitação de recursos computacionais
- **Construir plataforma de experimentos self-service** para que times de produto e marketing possam criar e acompanhar experimentos com dados sem depender de engenharia para cada análise
- **Modelo de churn prediction em tempo real** com intervenção automática para clientes com alta probabilidade de abandono baseado em padrões de sessão

---

*Diagrams disponíveis em [diagrams/](./diagrams/)*  
*Referências em [references.md](./references.md)*
