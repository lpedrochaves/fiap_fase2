CREATE OR REPLACE TABLE `${PROJETO}.${GOLD}.validacao_cruzada` AS
WITH av AS (
  SELECT ano, id_municipio, sigla_uf, cod_rede, n_avaliados, soma_pesos, soma_pesos_alfabetizados
  FROM `${PROJETO}.${SILVER}.alunos_agg`
  WHERE n_avaliados > 0
),
recalculada AS (
  SELECT 'brasil' AS nivel, 'BR' AS chave, ano,
         SUM(soma_pesos_alfabetizados) AS num, SUM(soma_pesos) AS den, SUM(n_avaliados) AS n_avaliados
  FROM av WHERE cod_rede IN ('2', '3') GROUP BY ano
  UNION ALL
  SELECT 'uf', sigla_uf, ano, SUM(soma_pesos_alfabetizados), SUM(soma_pesos), SUM(n_avaliados)
  FROM av WHERE cod_rede IN ('2', '3') GROUP BY ano, sigla_uf
  UNION ALL
  SELECT 'municipio', id_municipio, ano, SUM(soma_pesos_alfabetizados), SUM(soma_pesos), SUM(n_avaliados)
  FROM av WHERE cod_rede = '3' GROUP BY ano, id_municipio
),
oficial AS (
  SELECT 'brasil' AS nivel, 'BR' AS chave, ano, taxa_alfabetizacao AS taxa_oficial
  FROM `${PROJETO}.${SILVER}.meta_brasil` WHERE cod_rede = '5'
  UNION ALL
  SELECT 'uf', sigla_uf, ano, taxa_alfabetizacao FROM `${PROJETO}.${SILVER}.uf` WHERE cod_rede = '5'
  UNION ALL
  SELECT 'municipio', id_municipio, ano, taxa_alfabetizacao FROM `${PROJETO}.${SILVER}.municipio` WHERE cod_rede = '3'
)
SELECT
  r.nivel, r.chave, r.ano, r.n_avaliados,
  ROUND(100 * SAFE_DIVIDE(r.num, r.den), 2) AS taxa_recalculada,
  o.taxa_oficial,
  ROUND(100 * SAFE_DIVIDE(r.num, r.den) - o.taxa_oficial, 2) AS diferenca_pp,
  CASE
    WHEN o.taxa_oficial IS NULL THEN 'sem_oficial'
    WHEN ABS(100 * SAFE_DIVIDE(r.num, r.den) - o.taxa_oficial) <= IF(r.nivel = 'brasil', 0.06, 0.02) THEN 'convergiu'
    ELSE 'divergente'
  END AS status_validacao,
  'oficial' AS origem
FROM recalculada AS r
LEFT JOIN oficial AS o ON o.nivel = r.nivel AND o.chave = r.chave AND o.ano = r.ano;
