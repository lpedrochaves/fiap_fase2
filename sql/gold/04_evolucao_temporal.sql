CREATE OR REPLACE TABLE `${PROJETO}.${GOLD}.evolucao_temporal` AS
WITH base AS (
  SELECT 'brasil' AS nivel, 'BR' AS chave, ano, taxa_alfabetizacao AS taxa
  FROM `${PROJETO}.${GOLD}.meta_vs_resultado_brasil`
  UNION ALL
  SELECT 'uf', sigla_uf, ano, taxa_alfabetizacao FROM `${PROJETO}.${GOLD}.meta_vs_resultado_uf`
  UNION ALL
  SELECT 'municipio', id_municipio, ano, taxa_alfabetizacao FROM `${PROJETO}.${GOLD}.meta_vs_resultado_municipio`
)
SELECT
  b.nivel, b.chave, b.ano, b.taxa,
  p.taxa AS taxa_ano_anterior,
  ROUND(b.taxa - p.taxa, 2) AS variacao_pp,
  'oficial' AS origem
FROM base AS b
LEFT JOIN base AS p ON p.nivel = b.nivel AND p.chave = b.chave AND p.ano = b.ano - 1;
