CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.meta_brasil` AS
SELECT
  CAST(m.ano AS INT64)   AS ano,
  CASE m.rede WHEN 'Municipal' THEN '3' WHEN 'Pública' THEN '5' ELSE 'desconhecido' END AS cod_rede,
  m.taxa_alfabetizacao,
  m.meta_alfabetizacao_2024, m.meta_alfabetizacao_2025, m.meta_alfabetizacao_2026, m.meta_alfabetizacao_2027,
  m.meta_alfabetizacao_2028, m.meta_alfabetizacao_2029, m.meta_alfabetizacao_2030,
  m.percentual_participacao
FROM `${PROJETO}.${BRONZE}.meta_alfabetizacao_brasil` AS m
WHERE m._run_id = '${RUN_ID}';
