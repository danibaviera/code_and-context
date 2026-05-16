# Engineering Writing Lab

Portfolio de technical writing em engenharia de dados. Dois estudos de caso arquiteturais desenvolvidos para demonstrar a aplicação de padrões modernos de engenharia de dados em domínios de negócio distintos — com foco em legibilidade tanto para times técnicos quanto para stakeholders de negócio.

---

## Artigos

| Artigo | Domínio | Stack Principal | Foco |
|---|---|---|---|
| [Plataforma de Dados para Retail de Beleza](./beauty-retail-data-platform/article.md) | Varejo / Cosméticos | Python · Airflow · dbt · Kafka · PostgreSQL | Personalização, demand forecasting, omnichannel |
| [Arquitetura de Dados para Energia Renovável](./renewable-energy-data-platform/article.md) | Energia / Regulatório | Python · Kafka · Spark · Airflow · Grafana | Streaming, forecasting operacional, governança regulatória |

---

## Estrutura do Repositório

```
engineering-writing-lab/
│
├── beauty-retail-data-platform/
│   ├── article.md          # Artigo principal
│   ├── diagrams/           # Diagramas de arquitetura (Mermaid)
│   │   ├── architecture-overview.md
│   │   ├── data-flow.md
│   │   └── modeling-layers.md
│   └── references.md       # Referências e fontes
│
├── renewable-energy-data-platform/
│   ├── article.md          # Artigo principal
│   ├── diagrams/           # Diagramas de arquitetura (Mermaid)
│   │   ├── architecture-overview.md
│   │   ├── data-flow.md
│   │   └── lakehouse-layers.md
│   └── references.md       # Referências e fontes
│
└── README.md
```

---

## Metodologia

Cada artigo segue estrutura padronizada de technical writing arquitetural:

1. **Contexto de negócio** — problema real, dores operacionais e motivação técnica
2. **Requisitos funcionais e não funcionais** — escala, latência, confiabilidade, custo e governança
3. **Decisões arquiteturais** — raciocínio técnico com justificativas explícitas
4. **Trade-offs** — ganhos e perdas mapeados para cada decisão relevante
5. **Implementação técnica** — stack, fluxo de dados e trechos de código ilustrativos
6. **Observabilidade e governança** — monitoramento, qualidade, alertas e versionamento
7. **Impacto para stakeholders** — resultados operacionais e de negócio mensuráveis
8. **Próximos passos** — evolução planejada da arquitetura

---

## Sobre

Portfolio desenvolvido para demonstração de capacidade em technical writing, com foco em engenharia de dados. Os cenários são baseados em padrões reais de mercado e refletem decisões arquiteturais encontradas em projetos de dados em escala.

Os artigos são intencionalmente escritos para serem compreensíveis por stakeholders de negócio sem perder profundidade técnica para times de engenharia.
