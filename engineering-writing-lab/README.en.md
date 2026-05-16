# Engineering Writing Lab

Technical writing portfolio in data engineering. Two architectural case studies developed to demonstrate the application of modern data engineering patterns across distinct business domains — focused on readability for both technical teams and business stakeholders.

---

## Articles

| Article | Domain | Main Stack | Focus |
|---|---|---|---|
| [Beauty Retail Data Platform](./beauty-retail-data-platform/article.md) | Retail / Cosmetics | Python · Airflow · dbt · Kafka · PostgreSQL | Personalization, demand forecasting, omnichannel |
| [Renewable Energy Data Architecture](./renewable-energy-data-platform/article.md) | Energy / Regulatory | Python · Kafka · Spark · Airflow · Grafana | Streaming, operational forecasting, regulatory governance |

---

## Repository Structure

```
engineering-writing-lab/
│
├── beauty-retail-data-platform/
│   ├── article.md          # Main article
│   ├── diagrams/           # Architecture diagrams (Mermaid)
│   │   ├── architecture-overview.md
│   │   ├── data-flow.md
│   │   └── modeling-layers.md
│   └── references.md       # References and sources
│
├── renewable-energy-data-platform/
│   ├── article.md          # Main article
│   ├── diagrams/           # Architecture diagrams (Mermaid)
│   │   ├── architecture-overview.md
│   │   ├── data-flow.md
│   │   └── lakehouse-layers.md
│   └── references.md       # References and sources
│
└── README.md
```

---

## Methodology

Each article follows a standardized architectural technical writing structure:

1. **Business context** — real problem, operational pain points, and technical motivation
2. **Functional and non-functional requirements** — scale, latency, reliability, cost, and governance
3. **Architectural decisions** — technical reasoning with explicit justifications
4. **Trade-offs** — gains and losses mapped for each relevant decision
5. **Technical implementation** — stack, data flow, and illustrative code snippets
6. **Observability and governance** — monitoring, data quality, alerting, and versioning
7. **Stakeholder impact** — measurable operational and business outcomes
8. **Next steps** — planned architecture evolution

---

## About

Portfolio developed to demonstrate technical writing capabilities with a focus on data engineering. The scenarios are based on real market patterns and reflect architectural decisions found in large-scale data projects.

The articles are intentionally written to be accessible to business stakeholders without sacrificing technical depth for engineering teams.
