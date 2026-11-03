CREATE OR REPLACE TABLE `${PROJETO}.${GOLD}.indicador_unificado_municipio` AS
WITH oficial AS (
  SELECT ano, id_municipio, sigla_uf, taxa_alfabetizacao, meta_do_ano, distancia_meta_pp, status_meta,
         CAST(NULL AS TIMESTAMP) AS medido_em, origem
  FROM `${PROJETO}.${GOLD}.meta_vs_resultado_municipio`
),
ultima_medicao AS (
  SELECT * FROM `${PROJETO}.${SILVER}.eventos`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ano, id_municipio ORDER BY produced_at DESC) = 1
),
meta AS (
  SELECT * FROM `${PROJETO}.${SILVER}.meta_municipio`
  WHERE cod_rede = '3' AND ano = (SELECT MAX(ano) FROM `${PROJETO}.${SILVER}.meta_municipio`)
),
simulado AS (
  SELECT
    e.ano, e.id_municipio, u.sigla_uf, e.taxa_alfabetizacao,
    CASE e.ano
      WHEN 2024 THEN m.meta_alfabetizacao_2024 WHEN 2025 THEN m.meta_alfabetizacao_2025
      WHEN 2026 THEN m.meta_alfabetizacao_2026 WHEN 2027 THEN m.meta_alfabetizacao_2027
      WHEN 2028 THEN m.meta_alfabetizacao_2028 WHEN 2029 THEN m.meta_alfabetizacao_2029
      WHEN 2030 THEN m.meta_alfabetizacao_2030
    END AS meta_do_ano,
    e.produced_at AS medido_em, e.origem
  FROM ultima_medicao AS e
  LEFT JOIN meta AS m ON m.id_municipio = e.id_municipio
  LEFT JOIN `${PROJETO}.${SILVER}.uf_codigos` AS u ON u.cod_uf = SUBSTR(e.id_municipio, 1, 2)
)
SELECT * FROM oficial
UNION ALL
SELECT
  ano, id_municipio, sigla_uf, taxa_alfabetizacao, meta_do_ano,
  ROUND(taxa_alfabetizacao - meta_do_ano, 2) AS distancia_meta_pp,
  CASE
    WHEN meta_do_ano IS NULL THEN 'sem_meta'
    WHEN taxa_alfabetizacao >= meta_do_ano THEN 'atingiu'
    WHEN taxa_alfabetizacao <  meta_do_ano THEN 'nao_atingiu'
  END AS status_meta,
  medido_em, origem
FROM simulado;
