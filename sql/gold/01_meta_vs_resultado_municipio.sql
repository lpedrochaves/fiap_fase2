CREATE OR REPLACE TABLE `${PROJETO}.${GOLD}.meta_vs_resultado_municipio` AS
WITH base AS (
  SELECT
    r.ano, r.id_municipio, r.sigla_uf, r.regiao, r.taxa_alfabetizacao, r.media_portugues,
    CASE r.ano
      WHEN 2024 THEN m.meta_alfabetizacao_2024 WHEN 2025 THEN m.meta_alfabetizacao_2025
      WHEN 2026 THEN m.meta_alfabetizacao_2026 WHEN 2027 THEN m.meta_alfabetizacao_2027
      WHEN 2028 THEN m.meta_alfabetizacao_2028 WHEN 2029 THEN m.meta_alfabetizacao_2029
      WHEN 2030 THEN m.meta_alfabetizacao_2030
    END AS meta_do_ano,
    m.meta_alfabetizacao_2030 AS meta_2030_pactuada,
    (m.id_municipio IS NOT NULL) AS meta_encontrada,
    m.taxa_alfabetizacao AS taxa_na_tabela_meta
  FROM `${PROJETO}.${SILVER}.municipio` AS r
  LEFT JOIN `${PROJETO}.${SILVER}.meta_municipio` AS m
         ON m.ano = r.ano AND m.id_municipio = r.id_municipio AND m.cod_rede = '3'
  WHERE r.cod_rede = '3'
)
SELECT
  *,
  ROUND(taxa_alfabetizacao - meta_do_ano, 2)      AS distancia_meta_pp,
  ROUND(meta_2030_pactuada - taxa_alfabetizacao, 2) AS falta_meta_2030_pp,
  CASE
    WHEN meta_do_ano IS NULL THEN 'sem_meta'
    WHEN taxa_alfabetizacao IS NULL THEN 'sem_resultado'
    WHEN taxa_alfabetizacao >= meta_do_ano THEN 'atingiu'
    WHEN taxa_alfabetizacao <  meta_do_ano THEN 'nao_atingiu'
  END AS status_meta,
  COALESCE(ABS(taxa_alfabetizacao - taxa_na_tabela_meta) > 0.06, FALSE) AS inconsistencia_fonte,
  'oficial' AS origem
FROM base;
