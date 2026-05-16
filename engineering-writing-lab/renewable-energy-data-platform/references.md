# Referências — Plataforma de Dados para Energia Renovável

## Arquitetura e Engenharia de Dados

- **Fundamentals of Data Engineering** — Joe Reis & Matt Housley (O'Reilly, 2022)  
  Base conceitual sobre o ciclo de vida do dado, escolha de armazenamento e trade-offs arquiteturais. Especialmente relevante para as decisões de ingestão streaming vs batch.

- **Designing Data-Intensive Applications** — Martin Kleppmann (O'Reilly, 2017)  
  Referência para os conceitos de confiabilidade, replicação e tolerância a falhas. Base para as decisões de RF=3 no Kafka e garantias de entrega.

- **Lambda Architecture** — Nathan Marz  
  Artigo original da arquitetura Lambda que fundamenta a decisão de speed + batch layer.  
  [nathanmarz.com/blog/how-to-beat-the-cap-theorem.html](http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html)

## Ferramentas e Frameworks

- **Apache Kafka Documentation** — [kafka.apache.org/documentation](https://kafka.apache.org/documentation/)  
  Configuração de clusters, replication factor, consumer groups, garantias de entrega e retention policies.

- **Apache Spark Structured Streaming** — [spark.apache.org/docs/latest/structured-streaming-programming-guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)  
  Fundamento para o processamento de telemetria de sensores em tempo real e janelas deslizantes.

- **Delta Lake Documentation** — [docs.delta.io](https://docs.delta.io/)  
  Transações ACID, schema evolution, time travel para auditoria regulatória e compactação de arquivos.

- **Apache Airflow Documentation** — [airflow.apache.org/docs](https://airflow.apache.org/docs/)  
  Orquestração dos pipelines batch, DAG dependencies e SLA monitoring.

- **Grafana Documentation** — [grafana.com/docs](https://grafana.com/docs/)  
  Configuração de dashboards operacionais, alertas e integração com Prometheus.

- **MLflow Documentation** — [mlflow.org/docs/latest](https://mlflow.org/docs/latest/index.html)  
  Versionamento de modelos de forecasting, registro de experimentos e promoção de modelos via Model Registry.

## Forecasting e Séries Temporais

- **Forecasting: Principles and Practice** — Rob J Hyndman & George Athanasopoulos (3ª ed., 2021)  
  Referência para os conceitos de sazonalidade, decomposição de séries temporais e métricas de avaliação (MAPE, RMSE, MAE).  
  [otexts.com/fpp3/](https://otexts.com/fpp3/)

- **Probabilistic Forecasting of Wind Power Generation** — Jurgen Heinermann & Oliver Kramer  
  Contexto técnico para os desafios específicos de forecasting de geração eólica com múltiplas fontes de incerteza.

- **Solar Irradiance Forecasting Methods: A Review** — David Pozo-Vázquez et al.  
  Referência sobre modelos de previsão de irradiância solar e sua integração em pipelines de forecasting de geração fotovoltaica.

## Governança e Qualidade de Dados

- **The DAMA Guide to the Data Management Body of Knowledge (DMBOK2)** — DAMA International (2017)  
  Framework de referência para governança de dados, lineage, qualidade e ciclo de vida do dado em ambientes regulados.

- **Data Governance: The Definitive Guide** — Evren Eryurek et al. (O'Reilly, 2021)  
  Práticas de data catalog, controle de acesso, classificação de dados por sensibilidade e rastreabilidade de lineage.

## Regulatório Brasileiro

- **Procedimentos de Rede do ONS — Módulo 7** (Operador Nacional do Sistema Elétrico)  
  Regulamentação sobre programação de despacho, medição de energia e penalizações por desvio.  
  [ons.org.br/paginas/sobre-o-ons/procedimentos-de-rede/vigentes](https://www.ons.org.br/paginas/sobre-o-ons/procedimentos-de-rede/vigentes)

- **Resolução Normativa ANEEL nº 482** — ANEEL  
  Condições gerais para micro e minigeração distribuída e requisitos de medição.  
  [aneel.gov.br](https://www.aneel.gov.br/)

- **PRODIST — Procedimentos de Distribuição de Energia Elétrica** — ANEEL  
  Módulos de medição e qualidade de energia. Base para os requisitos de rastreabilidade de dados de medição oficial.

## Segurança em Ambientes Industriais (OT/IT)

- **IEC 62443 — Industrial Automation and Control Systems Security**  
  Padrão internacional para segurança de redes industriais, referência para a separação OT/IT e uso de data diodes.

- **NIST SP 800-82 — Guide to Industrial Control Systems (ICS) Security**  
  Guia do NIST para segurança de sistemas de controle industrial, incluindo arquiteturas de DMZ e gateways.  
  [csrc.nist.gov/publications/detail/sp/800-82/rev-2/final](https://csrc.nist.gov/publications/detail/sp/800-82/rev-2/final)

## Padrões de Lakehouse

- **Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics** — Michael Armbrust et al. (CIDR 2021)  
  Paper original que descreve o padrão Lakehouse, base para as decisões arquiteturais de Delta Lake no artigo.
