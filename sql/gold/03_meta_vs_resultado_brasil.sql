CREATE OR REPLACE TABLE `${PROJETO}.${GOLD}.meta_vs_resultado_brasil` AS
WITH base AS (
  SELECT
    m.ano, m.taxa_alfabetizacao,
    CASE m.ano
      WHEN 2024 THEN m.meta_alfabetizacao_2024 WHEN 2025 THEN m.meta_alfabetizacao_2025
      WHEN 2026 THEN m.meta_alfabetizacao_2026 WHEN 2027 THEN m.meta_alfabetizacao_2027
      WHEN 2028 THEN m.meta_alfabetizacao_2028 WHEN 2029 THEN m.meta_alfabetizacao_2029
      WHEN 2030 THEN m.meta_alfabetizacao_2030
    END AS meta_do_ano,
    m.meta_alfabetizacao_2030 AS meta_2030_pactuada,
    m.percentual_participacao
  FROM `${PROJETO}.${SILVER}.meta_brasil` AS m
  WHERE m.cod_rede = '5'
)
SELECT
  *,
  ROUND(taxa_alfabetizacao - meta_do_ano, 2)        AS distancia_meta_pp,
  ROUND(meta_2030_pactuada - taxa_alfabetizacao, 2) AS falta_meta_2030_pp,
  CASE
    WHEN meta_do_ano IS NULL THEN 'sem_meta'
    WHEN taxa_alfabetizacao IS NULL THEN 'sem_resultado'
    WHEN taxa_alfabetizacao >= meta_do_ano THEN 'atingiu'
    WHEN taxa_alfabetizacao <  meta_do_ano THEN 'nao_atingiu'
  END AS status_meta,
  'oficial' AS origem
FROM base;
