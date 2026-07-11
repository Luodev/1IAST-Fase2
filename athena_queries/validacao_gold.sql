-- =============================================================================
-- validacao_gold.sql — Queries Athena de validação e análise
-- Tech Challenge Fase 2 — POSTECH AI Scientist
--
-- Pré-requisito: rodar o crawler alfabetizacao-br-gold-crawler após o
-- pipeline batch (etl_gold). As tabelas ficam no database alfabetizacao_br_db.
-- Execute no workgroup alfabetizacao-br-workgroup (cutoff de 1 GB por query).
--
-- Particionamento: todas as visões Gold são particionadas por `ano`
-- (Hive-style, gold/<visao>/ano=YYYY/). Sempre que possível filtre por ano
-- (WHERE ano = ...) para acionar o partition pruning e reduzir os bytes
-- escaneados — e, portanto, o custo da query (FinOps).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. Contagem geral por visão (sanidade: nenhuma visão pode estar vazia)
-- -----------------------------------------------------------------------------
SELECT 'alfabetizacao_por_municipio' AS visao, COUNT(*) AS linhas
FROM alfabetizacao_br_db.alfabetizacao_por_municipio
UNION ALL
SELECT 'evolucao_temporal', COUNT(*) FROM alfabetizacao_br_db.evolucao_temporal
UNION ALL
SELECT 'ranking_municipios', COUNT(*) FROM alfabetizacao_br_db.ranking_municipios
UNION ALL
SELECT 'comparacao_metas_nacionais', COUNT(*) FROM alfabetizacao_br_db.comparacao_metas_nacionais;

-- -----------------------------------------------------------------------------
-- 1. QUALIDADE — Duplicidade de chave em alfabetizacao_por_municipio
--    Esperado: zero linhas (chave id_municipio + ano + serie é única)
-- -----------------------------------------------------------------------------
SELECT id_municipio, ano, serie, COUNT(*) AS ocorrencias
FROM alfabetizacao_br_db.alfabetizacao_por_municipio
GROUP BY id_municipio, ano, serie
HAVING COUNT(*) > 1;

-- -----------------------------------------------------------------------------
-- 2. QUALIDADE — Valores ausentes em campos obrigatórios
--    Esperado: zero linhas (o Silver roteia nulos para a quarentena)
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS linhas_com_nulos
FROM alfabetizacao_br_db.alfabetizacao_por_municipio
WHERE id_municipio IS NULL
   OR ano IS NULL
   OR taxa_alfabetizacao IS NULL;

-- -----------------------------------------------------------------------------
-- 3. QUALIDADE — Validação de chave (id_municipio deve ter 7 dígitos IBGE)
--    Esperado: zero linhas
-- -----------------------------------------------------------------------------
SELECT id_municipio
FROM alfabetizacao_br_db.alfabetizacao_por_municipio
WHERE LENGTH(id_municipio) <> 7
   OR NOT REGEXP_LIKE(id_municipio, '^[0-9]{7}$');

-- -----------------------------------------------------------------------------
-- 4. QUALIDADE — Faixa válida do indicador (0 a 100)
--    Esperado: zero linhas
-- -----------------------------------------------------------------------------
SELECT id_municipio, ano, taxa_alfabetizacao
FROM alfabetizacao_br_db.alfabetizacao_por_municipio
WHERE taxa_alfabetizacao < 0 OR taxa_alfabetizacao > 100;

-- -----------------------------------------------------------------------------
-- 5. QUALIDADE — Consistência entre tabelas
--    Toda UF presente no ranking deve existir na evolução temporal
--    Esperado: zero linhas
-- -----------------------------------------------------------------------------
SELECT DISTINCT r.sigla_uf
FROM alfabetizacao_br_db.ranking_municipios r
LEFT JOIN alfabetizacao_br_db.evolucao_temporal e
  ON r.sigla_uf = e.sigla_uf AND r.ano = e.ano
WHERE e.sigla_uf IS NULL;

-- -----------------------------------------------------------------------------
-- 6. ANÁLISE — Top 10 municípios por taxa de alfabetização por UF (2024)
-- -----------------------------------------------------------------------------
SELECT sigla_uf, id_municipio, taxa_alfabetizacao, ranking_uf
FROM alfabetizacao_br_db.ranking_municipios
WHERE ano = 2024 AND ranking_uf <= 10
ORDER BY sigla_uf, ranking_uf
LIMIT 300;

-- -----------------------------------------------------------------------------
-- 7. ANÁLISE — Evolução da taxa média por UF (2023 → 2024)
-- -----------------------------------------------------------------------------
SELECT sigla_uf, ano, serie, taxa_media, taxa_min, taxa_max, taxa_desvio
FROM alfabetizacao_br_db.evolucao_temporal
ORDER BY sigla_uf, ano, serie;

-- -----------------------------------------------------------------------------
-- 8. ANÁLISE — Municípios que não atingiram a meta 2025 (rede municipal)
-- -----------------------------------------------------------------------------
SELECT ano, status_meta_2025, COUNT(*) AS municipios,
       ROUND(AVG(gap_meta_2025), 2) AS gap_medio
FROM alfabetizacao_br_db.alfabetizacao_por_municipio
GROUP BY ano, status_meta_2025
ORDER BY ano, status_meta_2025;

-- -----------------------------------------------------------------------------
-- 9. ANÁLISE — UFs vs meta nacional e meta estadual
-- -----------------------------------------------------------------------------
SELECT sigla_uf, ano, taxa_uf, meta_nacional_2025, meta_uf_2025,
       gap_meta_nacional, gap_meta_uf, status_meta
FROM alfabetizacao_br_db.comparacao_metas_nacionais
ORDER BY ano, taxa_uf;

-- -----------------------------------------------------------------------------
-- 10. STREAMING — Eventos recebidos via MSK (após o crawler de streaming)
--     Distribuição por flag de risco calculada no Glue Structured Streaming
-- -----------------------------------------------------------------------------
SELECT risco_alfabetizacao, COUNT(*) AS eventos,
       ROUND(AVG(taxa_alfabetizacao), 2) AS taxa_media
FROM alfabetizacao_br_db.alfabetizacao
GROUP BY risco_alfabetizacao
ORDER BY eventos DESC;
