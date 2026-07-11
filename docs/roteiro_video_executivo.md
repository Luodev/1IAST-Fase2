# Roteiro — Vídeo Executivo (até 5 min)

**Tech Challenge Fase 2 | Pipeline Híbrida para Análise da Alfabetização no Brasil**

Tom: executivo — foco em problema, decisão e valor; tecnologia aparece como meio, não como fim. Ritmo de fala: ~140 palavras/min → o texto abaixo tem ~700 palavras (~4m50s). Ensaie com cronômetro e corte onde passar.

---

## 0:00 – 0:25 | Abertura e gancho

**Na tela:** slide de capa (título do projeto + nome do grupo).

> "Olá, eu sou [nome], do grupo [nome do grupo]. Nos próximos 5 minutos vou apresentar a plataforma de dados que construímos para responder uma pergunta que vale para cada gestor público de educação do país: **quais municípios brasileiros vão — ou não vão — alfabetizar suas crianças até 2030?**"

## 0:25 – 1:20 | Problema de negócio

**Na tela:** slide com 3 números grandes: 743 pontos Saeb · meta 100% até 2030 · 56,6% abaixo da meta.

> "O Compromisso Nacional Criança Alfabetizada estabelece que toda criança esteja alfabetizada até o fim do 2º ano do ensino fundamental. O INEP mede isso pelo Indicador Criança Alfabetizada: o percentual de alunos que atingem 743 pontos na escala Saeb. A meta nacional é 100% até 2030."
>
> "O retrato de hoje: analisamos os dados oficiais de mais de 5.500 municípios e **57% deles ainda estão abaixo da meta pactuada já para 2025**. E quase metade está em nível crítico ou de atenção."
>
> "O problema de negócio é que esses dados chegam fragmentados — resultados por município, por estado, metas nacionais, estaduais e municipais, em fontes e formatos diferentes, com novas medições chegando continuamente. Sem integrá-los com qualidade, não há priorização de investimento nem política pública baseada em evidência."

## 1:20 – 2:40 | Arquitetura da solução

**Na tela:** diagrama da pipeline (mermaid do README, exportado como imagem). Aponte os blocos enquanto fala.

> "Nossa resposta foi uma **pipeline híbrida de dados na AWS**, com dois fluxos que convergem para o mesmo data lake."
>
> "O fluxo **batch** carrega as bases oficiais do INEP e as refina em três camadas — é a arquitetura medalhão: **Bronze** preserva o dado bruto e o histórico completo; **Silver** limpa, padroniza e aplica regras de qualidade — o que reprova não é descartado, vai para quarentena auditável; e **Gold** entrega quatro visões prontas para decisão, como ranking de municípios e comparação entre resultado e meta."
>
> "O fluxo de **streaming**, com Kafka gerenciado, simula a chegada de novas medições em tempo quase real — cada evento já entra classificado por nível de risco."
>
> "Três decisões de engenharia sustentam custo e confiabilidade: **particionamento por ano** em todas as camadas — as consultas leem só o ano que interessa, o que reduz diretamente o custo; **jobs idempotentes** — reprocessar nunca duplica nem corrompe o histórico, e provamos isso em notebooks de verificação; e **infraestrutura efêmera** — sobe por script, é auditada e destruída ao fim de cada ciclo. Uma sessão completa de processamento custa **cerca de um dólar**."

## 2:40 – 3:40 | Valor para análises educacionais

**Na tela:** gráfico do gap por UF (notebook de EDA, seção 10) e amostra da visão `alfabetizacao_por_municipio` no Athena.

> "O que a liderança ganha com isso? Respostas em segundos, via SQL, sobre uma base confiável e catalogada:"
>
> "**Onde estamos?** — taxa de alfabetização por município e estado, ciclo a ciclo. **Quem precisa de ajuda primeiro?** — o ranking identifica os municípios críticos dentro de cada UF. **Estamos no rumo da meta?** — o gap entre resultado e meta pactuada, por município, mostra exatamente onde a trajetória de 2030 está comprometida. **A situação melhora ou piora?** — a série histórica preservada permite medir a evolução entre ciclos e o efeito de intervenções."
>
> "Tudo com governança: qualidade validada em cada camada, quarentena auditável e custo de consulta controlado."

## 3:40 – 4:35 | Potencial para inteligência artificial

**Na tela:** slide "Gold → IA" com 3 itens.

> "A camada Gold foi desenhada para ser insumo direto de IA. Três frentes imediatas:"
>
> "**Predição** — com taxa, gap e nível de proficiência por município e ano, treinamos modelos que antecipam quais municípios não atingirão a meta de 2030, antes da próxima avaliação oficial. **Segmentação** — clusterização de municípios por vulnerabilidade educacional, permitindo desenhar intervenções por perfil, e não por média nacional. **Priorização contínua** — a flag de risco calculada no streaming permite reagir a novas medições em tempo quase real."
>
> "E como o lake é aberto a enriquecimento, dados socioeconômicos do IBGE ou do FUNDEB entram como novas fontes sem mudar a arquitetura."

## 4:35 – 5:00 | Fechamento

**Na tela:** slide final com o diagrama pequeno + repositório.

> "Em resumo: transformamos dados públicos fragmentados em uma plataforma escalável, auditável e de baixo custo, que diz **onde agir, com que urgência e com qual evidência** — e que está pronta para alimentar os modelos de IA que vão antecipar o problema em vez de apenas medi-lo. O código, a documentação e as análises estão no repositório do grupo. Obrigado."

---

## Checklist de gravação

- [ ] Cronometrar ensaio completo (alvo: 4m40s, margem para respiro)
- [ ] Exportar o diagrama mermaid do README como imagem para o slide de arquitetura
- [ ] Capturar os gráficos do notebook `01_analise_exploratoria.ipynb` (seções 8 e 10)
- [ ] Conferir números citados: 5.550 municípios · 56,6% abaixo da meta 2025 · ~US$ 1/sessão
- [ ] Requisitos do PDF cobertos: problema de negócio ✔ arquitetura ✔ valor para análises ✔ uso em IA ✔ linguagem executiva ✔
