# Tech Challenge – Fase 2 | Pipeline Híbrido para Análise da Alfabetização no Brasil

Pipeline de dados híbrido (batch + streaming) com arquitetura medalhão (Bronze → Silver → Gold), feito em PySpark e integrado à AWS (S3), para analisar o **Indicador Criança Alfabetizada** do INEP.

## 1. Contexto do problema

O Compromisso Nacional Criança Alfabetizada é a política pública que busca garantir que toda criança brasileira esteja alfabetizada até o final do 2º ano do ensino fundamental. Para medir isso, o INEP realizou em 2023 a pesquisa Alfabetiza Brasil, que definiu o ponto de corte de **743 pontos na escala Saeb**: a criança que atinge essa proficiência é considerada alfabetizada.

A partir desse corte foi criado o **Indicador Criança Alfabetizada (ICA)**, que é o percentual de alunos que atingem esse nível. A meta nacional é chegar a 100% até 2030, com metas intermediárias pactuadas por estado e por município.

O desafio de dados é que essas informações vêm de fontes diferentes (resultados por UF, resultados por município, metas anuais, microdados de alunos) e precisam ser integradas com qualidade para permitir análises de desigualdade educacional e apoiar políticas públicas.

## 2. Fontes de dados

| Entidade | Fonte | Modo |
|---|---|---|
| UF (resultados Saeb 2019/2021/2023 + metas 2024–2030, inclui Brasil) | INEP – [resultados_e_metas_ufs.xlsx](https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_ufs.xlsx) | Batch |
| Município (resultados + metas + nível de alfabetização) | INEP – [resultados_e_metas_municipios.xlsx](https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_municipios.xlsx) | Batch |
| Tabelas uf, municipio, metas e alunos | [Base dos Dados](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72) (BigQuery) | Batch (opcional, precisa de projeto GCP) |
| Novas medições de proficiência de alunos | Simulador de eventos (formato da tabela `alunos`) | Streaming |

Obs.: os arquivos do INEP são a fonte original dos dados que a Base dos Dados disponibiliza no BigQuery. O pipeline suporta as duas vias — a do BigQuery é ativada definindo a variável `GCP_BILLING_PROJECT` (é a única forma de obter os microdados de alunos, que não têm download público).

## 3. Arquitetura

```
                         DATA LAKE (S3 ou pasta local)
 BATCH
 ┌──────────────┐     ┌─────────┐     ┌─────────┐     ┌──────────────────────┐
 │ INEP (xlsx)  ├──┐  │ BRONZE  │     │ SILVER  │     │        GOLD          │
 └──────────────┘  ├─►│ bruto,  ├────►│ limpo,  ├────►│ indicador_municipio  │
 ┌──────────────┐  │  │ append  │     │integrado│     │ evolucao_uf          │
 │ Base dosDados├──┘  └─────────┘     └────┬────┘     │ metas_x_resultados   │
 │  (BigQuery)  │                          │          └──────────────────────┘
 └──────────────┘                     validações de
                                       qualidade
 STREAMING
 ┌──────────────┐     ┌─────────┐                     ┌──────────────────────┐
 │ simulador de │     │ landing │   Structured        │     GOLD STREAM      │
 │ eventos      ├────►│ (JSON)  ├── Streaming ───────►│ taxa_uf_tempo_real   │
 │(Kinesis/Fire-│     └─────────┘   (micro-lotes 5s)  └──────────────────────┘
 │ hose na AWS) │
 └──────────────┘      + monitoramento/ (métricas)  + qualidade/ (relatórios)
```

### Fluxo de dados

1. **Bronze**: os arquivos do INEP são baixados e gravados brutos em Parquet, com colunas de controle (`_fonte`, `_ingestao_ts`) e particionados pela data de ingestão em modo append — o histórico fica todo preservado.
2. **Silver**: limpeza (valores `-`, `**`, `> 80` e rodapés de observação da planilha), padronização de nomes e tipos, normalização de chaves (`id_municipio` com 7 dígitos), deduplicação e integração (município recebe o contexto da sua UF).
3. **Qualidade**: 9 validações entre a Silver e a Gold — duplicidade, valores ausentes, chaves válidas, integridade referencial município↔UF e faixa 0–100. O relatório fica salvo em `qualidade/relatorios/` e dá pra fazer o pipeline parar se algo reprovar (`FALHAR_EM_QUALIDADE=1`).
4. **Gold**: três tabelas analíticas — indicador por município comparado com as metas (gaps, atingiu ou não), evolução temporal por UF (2019 → 2021 → 2023) e resumo metas × resultados por UF.
5. **Streaming**: o simulador publica eventos JSON de novas medições na pasta de landing (na AWS seria o Kinesis Firehose entregando no S3). O Structured Streaming consome em micro-lotes de 5 segundos, valida e deduplica os eventos, grava a bronze_stream e atualiza a gold_stream com a taxa de alfabetização em tempo quase real por UF (proficiência ≥ 743 = alfabetizado).

## 4. Como executar

Requisitos: Java 17+ e Python 3.10 a 3.13 (o PySpark ainda não roda no 3.14). No Windows também precisa do `winutils.exe` e `hadoop.dll` (repositório [cdarlint/winutils](https://github.com/cdarlint/winutils), pasta hadoop-3.3.6/bin) em `C:\Users\<usuario>\hadoop\bin` — o script detecta sozinho.

```bash
pip install pyspark pandas openpyxl basedosdados boto3

# pipeline batch completo (bronze -> silver -> qualidade -> gold)
python pipeline_medalhao.py batch

# demo completa (batch + produtor de eventos + streaming)
python pipeline_medalhao.py demo

# ou o streaming em dois terminais separados:
python pipeline_medalhao.py produzir --lotes 10 --eventos 50 --intervalo 2
python pipeline_medalhao.py stream --duracao 60
```

### Executando na AWS

O mesmo código roda na nuvem, só mudando variáveis de ambiente (não muda nada no .py):

```bash
export LAKE_URI=s3a://meu-bucket/datalake
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...     # só para credenciais temporárias (AWS Academy)
export AWS_REGION=us-east-1
python pipeline_medalhao.py batch
```

No Windows tem o modelo `rodar_aws.example.ps1` — copiar para `rodar_aws.ps1`, preencher as credenciais e executar (esse arquivo está no .gitignore para as chaves nunca irem pro repositório).

Testamos a ingestão completa no S3: o pipeline gravou as camadas bronze, silver e gold direto no bucket, com os 9 checks de qualidade aprovados. Numa evolução em produção, o batch rodaria agendado no EMR Serverless (ou Glue) via EventBridge.

## 5. Tecnologias e justificativas

- **PySpark** — mesma API para batch e streaming, e o mesmo código escala do notebook para um cluster EMR sem reescrever nada.
- **Parquet** — formato colunar comprimido; ocupa muito menos espaço que CSV e as consultas analíticas leem só as colunas necessárias.
- **AWS S3** — armazenamento barato e durável para o data lake; as camadas são só prefixos no bucket.
- **Structured Streaming** — processamento em micro-lotes com checkpoint (se o job cair, retoma de onde parou sem duplicar).
- **INEP / Base dos Dados** — fontes oficiais e reprodutíveis do indicador.
- **CloudWatch (opcional)** — com `ENABLE_CLOUDWATCH=1` as métricas de cada etapa vão para a AWS, onde dá pra criar alarmes.

## 6. Decisões arquiteturais (trade-offs)

- **Batch vs streaming**: resultados e metas oficiais mudam no máximo 1x por ano, então batch resolve com custo mínimo. Já novas medições de desempenho fazem sentido como fluxo contínuo — usamos micro-lotes de 5s porque "tempo quase real" atende o caso educacional; latência menor que isso só aumentaria o custo.
- **Data lake vs data warehouse**: escolhemos lake (S3 + Parquet) pelo custo baixo e pela flexibilidade de adicionar novas fontes (Censo Escolar, PNAD) sem migração de schema. Um warehouse como Redshift só se justificaria com muita concorrência de BI.
- **Custo vs performance**: o volume atual é pequeno (5,5 mil municípios), então configuramos poucas partições de shuffle (8 em vez das 200 padrão) e usamos `coalesce(1)` nos relatórios para não gerar centenas de arquivos minúsculos no S3. A gold de municípios é particionada por UF, que é o filtro mais comum nas consultas.
- **Arquivo único**: todo o pipeline está em `pipeline_medalhao.py` para facilitar a correção e o deploy (spark-submit de um arquivo só), com a separação das camadas feita por funções.

## 7. Monitoramento

Cada etapa registra quantas linhas processou, quanto tempo levou e o status (OK/FALHA). Essas métricas são salvas em `monitoramento/execucoes/` no lake e, opcionalmente, enviadas ao CloudWatch (namespace `PipelineAlfabetizacao`), onde dá pra criar alarme de falha ou de volume zerado. O relatório de qualidade em `qualidade/relatorios/` serve como trilha de auditoria.

## 8. FinOps – controle de custos

- Parquet comprimido em todas as camadas (JSON só na landing, que é o formato de chegada dos eventos);
- Particionamento por data (bronze) e por UF (gold) — as consultas leem só as partições necessárias;
- Poucos arquivos grandes em vez de muitos pequenos (menos requests no S3);
- Limite configurável na ingestão dos microdados de alunos, para não estourar a cota gratuita do BigQuery (1 TB/mês);
- Estimativa com os volumes atuais (batch diário + streaming 1h/dia): S3 < 1 GB ≈ US$ 0,03/mês; EMR Serverless ≈ US$ 2–5/mês; CloudWatch ≈ US$ 0,30/mês. **Total abaixo de US$ 6/mês.**

## 9. Aplicação em IA

A camada Gold já sai pronta para:

- **Predição de alfabetização**: a tabela de municípios (taxa, metas, gaps, participação, nível) é uma matriz de features; enriquecendo com Censo Escolar e dados socioeconômicos dá pra treinar um modelo que preveja o indicador do próximo ano e aponte municípios em risco de não cumprir a meta.
- **Análise de desigualdade**: clusterização dos municípios (por taxa, gap e diferença para a média da UF) revela grupos de vulnerabilidade educacional, inclusive dentro de um mesmo estado.
- **Políticas públicas baseadas em dados**: o resumo metas × resultados mostra onde a meta está descolada da realidade e a evolução 2021 → 2023 mede a recuperação pós-pandemia, ajudando a priorizar investimento.

## 10. Estrutura do repositório

```
├── pipeline_medalhao.py     # pipeline completo (bronze/silver/gold + qualidade + streaming)
├── rodar_aws.example.ps1    # modelo p/ rodar na AWS (copiar p/ rodar_aws.ps1 e preencher)
├── .gitignore               # protege credenciais e dados gerados
├── README.md
├── dados_fonte/             # (gerado) xlsx baixados do INEP
└── datalake/                # (gerado) bronze/ silver/ gold/ qualidade/ monitoramento/
```
