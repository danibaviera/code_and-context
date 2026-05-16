# Construindo Arquiteturas de Dados para Mercados de Energia Renovável: ingestão em tempo real, previsão operacional e governança analítica

> **Domínio:** Energia Renovável / Mercado Regulado  
> **Stack:** Python · Apache Kafka · Apache Spark · Apache Airflow · Grafana · PostgreSQL  
> **Foco:** Streaming de telemetria, forecasting de geração, governança regulatória, observabilidade operacional

---

## 1. Contexto de Negócio

Operadoras de energia renovável — solar e eólica — enfrentam um desafio estrutural que não existe em fontes convencionais: a geração é intermitente e diretamente dependente de condições meteorológicas fora do controle do operador. O vento para. A nebulosidade aumenta. Um sistema de frente fria muda completamente o perfil de geração de um parque eólico em questão de horas.

Ao mesmo tempo, o mercado de energia opera com compromissos de despacho programados com o Operador Nacional do Sistema Elétrico (ONS). Desvios entre o que foi programado e o que efetivamente foi gerado resultam em penalizações financeiras proporcionais ao desvio — o que coloca o forecasting de geração no centro das decisões operacionais e financeiras da empresa.

O contexto regulatório brasileiro (ANEEL, ONS, CCEE) impõe requisitos adicionais de rastreabilidade: medições devem ser auditáveis, relatórios devem ser entregues em janelas de tempo definidas, e qualquer inconsistência nos dados de medição pode resultar em questionamentos durante inspeções regulatórias.

As dores identificadas na operadora antes da implementação da plataforma:

- **Ausência de visibilidade consolidada** sobre geração em múltiplos parques distribuídos geograficamente. Cada parque tinha seu próprio sistema de telemetria com interfaces e formatos distintos.
- **Integração manual com previsões meteorológicas**: analistas baixavam arquivos de previsão manualmente e alimentavam planilhas para estimar a geração do dia seguinte.
- **Dados de sensores sem padronização**: turbinas de fabricantes diferentes geravam telemetria em protocolos distintos (MODBUS, OPC-DA, IEC 61850), sem tradução para um modelo canônico.
- **Nenhuma camada de governança**: dados de medição oficial eram armazenados em diretórios locais sem controle de versão, hash de integridade ou log de acesso.
- **Decisões de despacho reativas**: o Centro de Controle operava com visão do passado recente, sem capacidade preditiva estruturada. Anomalias em equipamentos eram detectadas após impacto na geração, não antes.

A plataforma precisava transformar esse ambiente fragmentado em infraestrutura de dados confiável o suficiente para suportar tanto decisões operacionais em tempo real quanto auditoria regulatória retroativa.

---

## 2. Requisitos Funcionais e Não Funcionais

### Requisitos Funcionais

- Ingestão de telemetria de sensores em tempo real: potência gerada, tensão, temperatura de equipamentos, vibração de turbinas, irradiância solar
- Integração automatizada com APIs de previsão meteorológica (INMET, Open-Meteo, serviços privados) com atualização horária
- Modelo preditivo de geração por parque com horizonte de 24h e 72h, atualizado a cada 15 minutos
- Dashboard operacional em tempo real para o Centro de Controle com visibilidade de todos os parques
- Pipeline automático de relatórios regulatórios (ONS/ANEEL) com entrega até T+2h do período de competência
- Alertas de anomalia em equipamentos para a equipe de manutenção antes do impacto na geração
- Rastreabilidade completa de dados de medição para auditoria regulatória
- Detecção de gaps e falhas de sensores com qualificação automática da qualidade do dado

### Requisitos Não Funcionais

| Dimensão | Requisito |
|---|---|
| **Escala** | 500+ sensores por parque; múltiplos parques simultâneos; ~10 milhões de leituras/dia |
| **Latência** | Telemetria operacional: < 30 segundos / Atualização de forecast: a cada 15 minutos |
| **Disponibilidade** | 99,95% para pipeline de despacho operacional e alertas críticos |
| **Retenção** | Telemetria: 5 anos (requisito regulatório ANEEL) / Logs de auditoria: 7 anos |
| **Custo** | Tiering automático de storage (hot/warm/cold) baseado em frequência de acesso |
| **Governança** | Rastreabilidade completa para auditoria; imutabilidade de dados regulatórios |
| **Qualidade** | Tolerância zero a perdas em dados de medição oficial; flags obrigatórios para dados estimados |
| **SLA regulatório** | Relatório disponível até T+2h do final de cada período de competência |
| **Segurança** | Isolamento de redes OT (Operational Technology) e IT; acesso auditado por role |

---

## 3. Decisões Arquiteturais

### 3.1 Arquitetura híbrida Lambda para conciliar reatividade e confiabilidade

**Decisão:** adotar arquitetura Lambda com duas camadas de processamento paralelas: *speed layer* com Kafka + Spark Structured Streaming para dados operacionais de baixa latência, e *batch layer* com Spark + Airflow para processamento histórico, relatórios regulatórios e retreinamento de modelos.

**Justificativa:** os requisitos são fundamentalmente contraditórios se abordados com uma única tecnologia. A telemetria de sensores exige processamento em segundos para alertas de equipamento — latência que só streaming entrega. Os dados de medição oficial para relatórios regulatórios exigem consistência total, reprocessamento confiável e imutabilidade — propriedades que arquiteturas puramente streaming dificilmente garantem sem custo operacional proibitivo.

A arquitetura Lambda resolve essa tensão ao dedicar tecnologias distintas a requisitos distintos, com uma camada de serving unificada que abstrai a origem para os consumidores finais.

**Alternativa considerada:** Kappa Architecture (streaming unificado). Descartada porque o requisito de reprocessamento histórico auditável com garantia de idempotência em dados regulatórios é mais simples de implementar com processamento batch explícito.

---

### 3.2 Lakehouse como camada de armazenamento central

**Decisão:** implementar o padrão Lakehouse com Delta Lake sobre object storage (S3-compatible), em vez de warehouse relacional puro.

**Justificativa:** os dados chegam de fontes com graus radicalmente diferentes de maturidade de formato. Turbinas modernas geram Protobuf; equipamentos legados geram CSV em FTP; APIs meteorológicas retornam JSON com schemas que mudam sem aviso. Um warehouse puro exigiria schema enforcement rígido na ingestão, tornando a plataforma frágil a qualquer variação de fonte.

O Lakehouse permite ingestão flexível na zona raw com schema evolution progressivo, e adiciona capacidades críticas para o contexto regulatório: *time travel* (acesso a versões históricas dos dados para auditoria), compactação automática de arquivos pequenos e transações ACID que garantem que leituras regulatórias nunca vejam estados inconsistentes.

**Trade-off aceito:** maior complexidade de gerenciamento em comparação com um warehouse gerenciado. Justificado pelo ganho em flexibilidade de ingestão e pelas capacidades de auditoria via time travel.

---

### 3.3 Inferência preditiva near real-time com janela de 15 minutos

**Decisão:** servir o modelo de forecasting de geração via microsserviço Python com atualização de features e predições a cada 15 minutos.

**Justificativa:** o Centro de Controle toma decisões de ajuste de despacho em janelas de 1 hora. Para que essas decisões sejam baseadas em previsões relevantes, o forecast precisa incorporar as condições meteorológicas mais recentes — um sistema de frente fria que aparece na previsão às 14h precisa estar refletido na previsão de geração das 15h em diante, não apenas na próxima atualização diária.

**Trade-off aceito:** latência de 15 minutos entre nova leitura meteorológica e atualização do forecast, versus a complexidade de servir em tempo real sub-minuto. O trade-off é aceitável: o horizonte operacional de despacho é de horas, e uma janela de 15 minutos não cria gap significativo para as decisões que o sistema precisa suportar.

---

### 3.4 Separação entre domínios operacional e regulatório

**Decisão:** manter pipelines e zonas de armazenamento explicitamente separados para dados operacionais (alta velocidade, permite correções) e dados regulatórios (auditados, imutáveis após validação).

**Justificativa:** dados operacionais precisam de flexibilidade — uma leitura de sensor fora de range pode ser corrigida quando a causa é identificada. Dados regulatórios, uma vez validados, não podem ser alterados: qualquer correção posterior deve ser registrada como uma nova versão com justificativa auditável, não como uma substituição silenciosa do dado original.

A separação torna esse contrato explícito na arquitetura, evitando que processos operacionais inadvertidamente contaminem a integridade dos dados que serão apresentados em auditorias.

---

## 4. Trade-offs

| Decisão | Ganho | Perda |
|---|---|---|
| Lambda Architecture (híbrida) | Latência baixa para operações + confiabilidade histórica para regulatório | Duplicação de lógica de processamento; maior superfície de manutenção |
| Lakehouse vs Warehouse puro | Flexibilidade de ingestão de fontes heterogêneas; time travel para auditoria | Complexidade operacional maior; necessidade de gestão de compactação e vacuum |
| Forecast a cada 15 minutos | Previsão sempre atualizada com dados meteorológicos recentes | Custo computacional de inferência frequente; complexidade de gestão de feature freshness |
| Pipelines regulatórios isolados | Imutabilidade garantida; rastreabilidade independente | Overhead de manutenção de dois pipelines paralelos para algumas fontes |
| Kafka para telemetria IoT | Alto throughput; durabilidade de mensagens; replay para reprocessamento | Necessidade de gestão de cluster Kafka; complexidade de configuração de tópicos por parque |
| Separação OT/IT | Isolamento de redes industriais sensíveis; conformidade com IEC 62443 | Latência adicional na ponte OT→IT; necessidade de gateways de dados |

---

## 5. Implementação Técnica

### Stack

```
Ingestão Streaming:   Apache Kafka (tópicos por parque/tipo de sensor)
Ingestão Batch:       Apache Airflow + Python (APIs meteorológicas, SFTP)
Processamento Stream: Apache Spark Structured Streaming
Processamento Batch:  Apache Spark (PySpark) + Apache Airflow
Armazenamento:        Delta Lake (Lakehouse) + PostgreSQL (aggregates serving)
Transformação:        dbt (camada analítica) + PySpark (camada operacional)
Serving ML:           FastAPI (Python) para forecasting service
Monitoramento:        Grafana + Prometheus + AlertManager
Alertas críticos:     PagerDuty
Versionamento ML:     MLflow
```

### Fluxo de Dados

```
Fontes Operacionais            Ingestão                   Processamento
───────────────────            ────────                   ─────────────

Sensores IoT          ─ MQTT ─► Kafka Broker
(turbinas/painéis)              [por tópico]  ──► Spark Structured Streaming ──► Delta Lake
                                               └──► Alertas em tempo real         /raw/

APIs Meteorológicas   ─ HTTP ─► Airflow DAG ──────────────────────────────────► Delta Lake
(INMET, Open-Meteo)                                                               /raw/meteo/

SCADA / ONS           ─ SFTP ─► Airflow DAG ──────────────────────────────────► Delta Lake
(dados operacionais)                                                              /raw/scada/

Eventos Manutenção    ─ API ──► Kafka Broker ──► Spark Streaming ──────────────► Delta Lake
(CMMS)                                                                            /raw/events/

                                    │
                              [Spark Batch +]
                              [dbt models   ]
                                    │
                     ┌──────────────┼──────────────┐
              Delta Lake        Delta Lake      Delta Lake
              /trusted/         /business/      /regulatory/
                                    │
               ┌────────────────────┼──────────────────────┐
        [Forecasting     [Centro de Controle    [Relatórios
         Service]         Dashboard Grafana]     Regulatórios
         FastAPI]                                ANEEL/ONS]
```

### Zonas do Lakehouse

**raw/** — dados brutos imutáveis particionados por `parque_id / ano / mês / dia`. Cada arquivo possui hash SHA-256 registrado no catálogo de dados para verificação de integridade. Nada é deletado nessa zona.

**trusted/** — dados normalizados para unidades padrão (MW, kV, °C, m/s), com outliers identificados e sinalizados com flag `quality_flag` (valid / estimated / suspect / missing). Gaps de telemetria preenchidos por interpolação linear com flag obrigatório. Checksums por lote para auditoria.

**business/** — séries temporais agregadas por turbina/painel, por parque e por período (15 min, horário, diário). Enriquecidas com previsão meteorológica e dados de capacidade instalada. Base para o modelo de forecasting e para o dashboard operacional.

**regulatory/** — subconjunto dos dados com controles adicionais de imutabilidade: write-once por período de competência, hash de integridade por registro, log de acesso completo, e versionamento explícito de correções com justificativa auditável. Fonte oficial para todos os relatórios ANEEL/ONS.

### Pipeline de Forecasting de Geração

O modelo de previsão por parque utiliza:

- **Histórico de geração**: séries de 18–24 meses por parque, limpas e com períodos de manutenção marcados
- **Features meteorológicas**: irradiância solar (W/m²), velocidade e direção do vento por altura, temperatura ambiente, nebulosidade, pressão atmosférica
- **Calendário operacional**: manutenções programadas, capacidade disponível por turbina/painel
- **Padrões sazonais regionais**: sazonalidade de vento por mês, variação de irradiância por estação

O pipeline Airflow é disparado a cada hora para coletar a previsão meteorológica atualizada e gerar os forecasts de 24h e 72h por parque. O serviço FastAPI expõe os forecasts atualizados e publica no tópico Kafka `forecast.generation` para consumo pelo Centro de Controle.

```python
# Estrutura ilustrativa do pipeline de forecasting
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

def fetch_meteorological_forecast(parque_id: str) -> dict:
    """Coleta previsão meteorológica atualizada via API e persiste na zona trusted."""
    ...

def compute_generation_features(parque_id: str, horizon_hours: int) -> pd.DataFrame:
    """Monta o vetor de features para o horizonte solicitado a partir da zona business."""
    ...

def run_forecast_inference(features: pd.DataFrame, model_version: str) -> pd.DataFrame:
    """Executa inferência com o modelo MLflow registrado como Production."""
    ...

def validate_forecast_sanity(forecast: pd.DataFrame) -> bool:
    """
    Valida que o forecast está dentro de bounds físicos:
    - Geração não pode exceder capacidade instalada
    - Geração não pode ser negativa
    - Variação entre períodos consecutivos deve ser fisicamente plausível
    """
    ...

with DAG("generation_forecast_hourly", schedule_interval="@hourly") as dag:
    fetch_meteo   = PythonOperator(task_id="fetch_meteorological_data", ...)
    build_features = PythonOperator(task_id="compute_features",         ...)
    run_inference  = PythonOperator(task_id="run_forecast_inference",   ...)
    validate       = ShortCircuitOperator(task_id="validate_forecast",  ...)
    publish        = PythonOperator(task_id="publish_to_kafka",         ...)

    fetch_meteo >> build_features >> run_inference >> validate >> publish
```

### Detecção de Anomalias em Equipamentos

O detector de anomalias processa o stream de telemetria em tempo real via Spark Structured Streaming. Utiliza z-score deslizante calculado sobre uma janela temporal de 7 dias para cada sensor de cada equipamento, comparando o valor atual com o comportamento histórico do próprio equipamento (em vez de um threshold global, que seria insensível às diferenças entre equipamentos de fabricantes e gerações distintas).

```python
# Lógica ilustrativa de detecção de anomalia em streaming
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def compute_rolling_zscore(df, sensor_col: str, window_days: int = 7):
    """
    Calcula o z-score deslizante para um sensor específico,
    usando janela de N dias por equipment_id.
    Retorna o dataframe com colunas de z-score e flag de anomalia.
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

Anomalias detectadas são publicadas no tópico Kafka `alerts.equipment` e consumidas pelo AlertManager para notificação da equipe de manutenção.

---

## 6. Observabilidade e Governança

### Monitoramento Operacional com Grafana + Prometheus

Dashboards separados por audiência:

**Centro de Controle (operacional)**
- Geração atual por parque (MW) vs forecast (MW) — gauge em tempo real
- Desvio acumulado do programado no período corrente
- Status de conectividade de sensores por parque (verde / amarelo / vermelho)
- Alertas ativos de equipamento com severidade e tempo desde detecção

**Time de Dados (infraestrutura)**
- Throughput de mensagens Kafka por tópico (mensagens/segundo)
- Lag de consumidores Spark Streaming por tópico
- Latência end-to-end de telemetria (sensor → dashboard)
- Taxa de registros com quality_flag ≠ valid por parque
- Status de DAGs Airflow e latência de pipelines críticos

**Liderança / Regulatório**
- Performance de geração por parque (MWh realizado vs programado)
- Disponibilidade histórica de equipamentos por parque
- Status de entrega de relatórios regulatórios (entregue no SLA / com atraso / pendente)

### Qualidade de Dados

| Check | Camada | Frequência | Ação em Falha |
|---|---|---|---|
| Completude de sensores por parque | trusted | A cada 15 min | Alerta imediato + interpolação com flag |
| Range físico de valores | trusted | Em tempo real (stream) | Quarentena + notificação manutenção |
| Consistência temporal (gaps) | trusted | A cada 15 min | Preenchimento documentado + flag `estimated` |
| Concordância entre sensores redundantes | trusted | A cada hora | Flag `suspect` + alerta operacional |
| Integridade de hash (dados regulatórios) | regulatory | Por lote | Bloqueio total + alerta crítico + PagerDuty |
| Completude de relatório regulatório | regulatory | Por período | Alerta crítico + escalação automática |

### Rastreabilidade Regulatória

Cada registro na zona `regulatory/` possui os seguintes campos de auditoria obrigatórios:

| Campo | Descrição |
|---|---|
| `record_hash` | SHA-256 do conteúdo do registro antes de qualquer transformação |
| `ingestion_timestamp` | Timestamp exato (UTC) de recebimento pelo pipeline |
| `source_system` | Identificador do sistema de medição de origem |
| `pipeline_version` | Versão do pipeline (git commit hash) que processou o dado |
| `regulatory_period` | Período de competência regulatória (AAAA-MM-DD HH:MM) |
| `quality_flag` | `valid` / `estimated` / `suspect` — nunca omitido em dados regulatórios |
| `audit_user` | Identidade do processo ou usuário que escreveu o registro |

Qualquer tentativa de modificação em dados de períodos já validados dispara alerta crítico e é bloqueada pela camada de controle de acesso. Correções legítimas são registradas como novos registros com `corrects_record_hash` apontando para o original.

### Versionamento de Modelos de Forecasting

Os modelos são gerenciados pelo MLflow com o seguinte ciclo:

1. **Staging**: novo modelo treinado é registrado com métricas de validação
2. **Champion/Challenger**: modelo candidato é comparado com o modelo em produção por 7 dias em shadow mode
3. **Production**: promoção apenas se MAPE < 8% (24h) e < 15% (72h) — validado pelo pipeline Airflow
4. **Archived**: modelo anterior é arquivado (não deletado) para rastreabilidade regulatória

Rollback automático é acionado se a performance em produção degrada > 20% em relação ao baseline medido no período de promoção.

### Tolerância a Falhas

| Cenário | Mecanismo de Resiliência |
|---|---|
| Falha de sensor individual | Detecção por gap de leitura; interpolação com flag; alerta para manutenção |
| Falha de broker Kafka | Replicação em 3 brokers (RF=3); consumer groups com auto-recovery; replay de offset |
| Falha de API meteorológica | Cache da última previsão válida com timestamp de expiração; fallback para provedor secundário |
| Falha de pipeline Airflow | Retry automático configurável por DAG (3 tentativas); alerta após esgotamento |
| Indisponibilidade do forecasting service | Última previsão válida servida com campo `forecast_age_minutes`; alerta se > 30 min |
| Perda de conectividade OT→IT | Buffer local no gateway de dados com capacidade de 24h; sincronização automática ao reestabelecer |

---

## 7. Impacto para Stakeholders

### Centro de Controle Operacional

- **Visibilidade consolidada** de todos os parques em um único dashboard Grafana com latência < 30 segundos, substituindo múltiplos sistemas de monitoramento fragmentados
- **Redução de desvio de despacho**: forecast com MAPE < 8% no horizonte de 24h possibilitou ajustes proativos de programação, reduzindo penalizações por desvio em 60% nos primeiros 6 meses
- **Alertas proativos** permitiram ao Centro de Controle iniciar protocolos de contingência antes que anomalias de equipamento impactassem a geração

### Equipe de Manutenção

- **Detecção preditiva de falhas** por análise de padrões de vibração e temperatura reduziu paradas não programadas em 40%, permitindo que 70% das intervenções passassem a ser realizadas em janelas de manutenção planejada
- **Histórico completo de readings por equipamento** com resolução de 1 minuto eliminando dependência de logs locais de cada turbina para diagnóstico de falhas

### Conformidade Regulatória

- **Relatórios ANEEL/ONS gerados automaticamente** até T+2h, eliminando processo manual que frequentemente excedia o prazo
- **Rastreabilidade completa** com hash de integridade e log de acesso eliminando o risco de questionamento regulatório por inconsistência de dados
- **Audit trail estruturado** reduziu o tempo de resposta a solicitações de auditoria de semanas para horas

### Liderança Executiva

- **Redução de 60% nas penalizações** por desvio de despacho como resultado direto da melhoria de forecasting
- **Baseline histórico estruturado** habilitando análises de viabilidade para expansão de capacidade com dados de geração real vs projetado
- **Visão consolidada de performance por parque** para decisões de priorização de investimentos em manutenção e upgrades

---

## 8. Próximos Passos

### Curto Prazo (0–3 meses)

- **Modelo de manutenção preditiva por equipamento**: expandir o detector de anomalias para classificar o tipo de falha provável (desgaste de rolamento, problema elétrico, desalinhamento) com base em padrões de vibração e temperatura históricos, permitindo priorizações mais precisas da equipe de manutenção
- **Protocolo OPC-UA para equipamentos de nova geração**: padronizar a coleta de dados de novas turbinas e inversores com OPC-UA em vez de adaptadores proprietários, reduzindo o custo de integração de novos equipamentos

### Médio Prazo (3–6 meses)

- **Data catalog com Apache Atlas ou OpenMetadata**: implementar catálogo centralizado de metadados para rastreabilidade de lineage, controle de acesso baseado em sensibilidade regulatória e autodocumentação de fontes de dados
- **Features de mercado spot (PLD)**: integrar dados de Preço de Liquidação das Diferenças para correlacionar decisões de despacho com preço de mercado, habilitando otimização financeira além da operacional
- **Migração de transformações batch para dbt + Spark**: padronizar a camada de transformação analítica com dbt para ganhos de governança, teste e documentação já consolidados no artigo de retail

### Longo Prazo (6–12 meses)

- **Modelo de otimização de portfólio**: construir modelo que balanceia simultaneamente geração prevista, preços de mercado (PLD), restrições regulatórias de despacho e disponibilidade de equipamentos para maximizar receita líquida ajustada por risco
- **Certificados de energia renovável (REC/I-REC)**: expandir a rastreabilidade regulatória para suportar a emissão e auditoria de certificados de energia renovável, habilitando a empresa a participar de mercados voluntários de crédito de carbono
- **Avaliação de migração para cloud nativa**: analisar viabilidade de migração para Azure Synapse Analytics + Event Hubs ou AWS MSK + Redshift para redução de overhead operacional de gestão de infraestrutura Kafka e Spark on-premises

---

*Diagramas disponíveis em [diagrams/](./diagrams/)*  
*Referências em [references.md](./references.md)*
