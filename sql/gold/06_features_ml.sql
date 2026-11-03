CREATE OR REPLACE TABLE `${PROJETO}.${GOLD}.features_ml_municipio` AS
WITH t23 AS (
  SELECT id_municipio, sigla_uf, regiao, taxa_alfabetizacao AS taxa_2023, media_portugues AS media_portugues_2023
  FROM `${PROJETO}.${SILVER}.municipio` WHERE cod_rede = '3' AND ano = 2023
),
meta23 AS (
  SELECT id_municipio, percentual_participacao AS participacao_2023, meta_alfabetizacao_2024 AS meta_2024
  FROM `${PROJETO}.${SILVER}.meta_municipio` WHERE cod_rede = '3' AND ano = 2023
),
alunos23 AS (
  SELECT id_municipio,
         SUM(n_registros) AS n_alunos_registrados_2023,
         SUM(n_avaliados) AS n_alunos_avaliados_2023,
         SAFE_DIVIDE(SUM(n_registros) - SUM(n_avaliados), SUM(n_registros)) AS proporcao_nao_avaliados_2023
  FROM `${PROJETO}.${SILVER}.alunos_agg` WHERE cod_rede = '3' AND ano = 2023
  GROUP BY id_municipio
),
alvo AS (
  SELECT id_municipio, taxa_alfabetizacao AS alvo_taxa_2024
  FROM `${PROJETO}.${SILVER}.municipio` WHERE cod_rede = '3' AND ano = 2024
)
SELECT
  t23.id_municipio, t23.sigla_uf, t23.regiao,
  t23.taxa_2023, t23.media_portugues_2023,
  meta23.participacao_2023, meta23.meta_2024,
  alunos23.n_alunos_registrados_2023, alunos23.n_alunos_avaliados_2023, alunos23.proporcao_nao_avaliados_2023,
  alvo.alvo_taxa_2024,
  'oficial' AS origem
FROM t23
JOIN alvo ON alvo.id_municipio = t23.id_municipio
LEFT JOIN meta23 ON meta23.id_municipio = t23.id_municipio
LEFT JOIN alunos23 ON alunos23.id_municipio = t23.id_municipio
WHERE alvo.alvo_taxa_2024 IS NOT NULL;
