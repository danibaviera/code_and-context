# Referências — Plataforma de Dados para Retail de Beleza

## Arquitetura e Engenharia de Dados

- **Fundamentals of Data Engineering** — Joe Reis & Matt Housley (O'Reilly, 2022)  
  Base conceitual sobre o ciclo de vida do dado, escolha de armazenamento e trade-offs arquiteturais.

- **Designing Data-Intensive Applications** — Martin Kleppmann (O'Reilly, 2017)  
  Referência sobre confiabilidade, escalabilidade e replicação. Especialmente relevante para as decisões sobre CDC e separação de camadas transacional/analítica.

- **The Data Warehouse Toolkit** — Ralph Kimball & Margy Ross (Wiley, 3ª ed., 2013)  
  Base para o design dimensional: fatos, dimensões e modelagem star schema usados nas camadas business e serving.

## Ferramentas e Frameworks

- **Apache Airflow Documentation** — [airflow.apache.org](https://airflow.apache.org/docs/)  
  Orquestração de pipelines batch, DAGs, retry policies e SLAs.

- **dbt Documentation** — [docs.getdbt.com](https://docs.getdbt.com/)  
  Modelagem em camadas, testes declarativos de qualidade, lineage e documentação automática.

- **Apache Kafka Documentation** — [kafka.apache.org/documentation](https://kafka.apache.org/documentation/)  
  Arquitetura de tópicos, consumer groups, garantias de entrega e particionamento.

- **Feast: Open Source Feature Store** — [feast.dev](https://feast.dev/)  
  Referência para implementação de feature stores com consistência treino/produção.

## Machine Learning em Produção

- **Machine Learning Design Patterns** — Valliappa Lakshmanan, Sara Robinson & Michael Munn (O'Reilly, 2020)  
  Padrões para feature stores, serving e avaliação de modelos em produção. Contexto direto para a decisão de feature store do artigo.

- **Continuous Delivery for Machine Learning** — Danilo Sato, Arif Wider & Christoph Windheuser  
  Artigo da ThoughtWorks sobre pipelines de ML com validação automática antes de promoção para produção.  
  [martinfowler.com/articles/cd4ml.html](https://martinfowler.com/articles/cd4ml.html)

## Governança e Qualidade de Dados

- **LGPD — Lei Geral de Proteção de Dados Pessoais** — Lei nº 13.709/2018  
  Base legal para as decisões de pseudonimização, controle de acesso e pipeline de purge.  
  [planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm](http://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm)

- **The Data Quality Manifesto** — TDWI  
  Princípios de qualidade de dados: completude, unicidade, consistência, validade e pontualidade.

## Analytics e Segmentação

- **RFM Analysis for Customer Segmentation**  
  Referência metodológica para o modelo de segmentação Recency-Frequency-Monetary utilizado no artigo.  
  Kahreh, Z. S., Tive, M., Babania, A., & Hesan, M. (2014). *Analyzing the Applications of Customer Lifetime Value (CLV) based on Benefit Segmentation for the Banking Sector*. Procedia - Social and Behavioral Sciences.

- **Forecasting: Principles and Practice** — Rob J Hyndman & George Athanasopoulos (3ª ed., 2021)  
  Referência para os conceitos de sazonalidade, decomposição de séries temporais e avaliação de modelos preditivos (MAPE, RMSE).  
  [otexts.com/fpp3/](https://otexts.com/fpp3/)

## Padrões de Retail e Omnichannel

- **Gartner: Building a Modern Analytics Architecture for Retail**  
  Framework de referência para arquiteturas de dados em varejo omnichannel.

- **Harvard Business Review: A Study of 46,000 Shoppers Shows That Omnichannel Retailing Works** (2017)  
  Evidência empírica sobre o valor da visão unificada do cliente entre canais.
