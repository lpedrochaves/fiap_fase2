CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.meta_uf` AS
SELECT
  CAST(m.ano AS INT64)   AS ano,
  m.sigla_uf,
  u.regiao,
  CASE m.rede WHEN 'Municipal' THEN '3' WHEN 'Pública' THEN '5' ELSE 'desconhecido' END AS cod_rede,
  m.taxa_alfabetizacao,
  m.meta_alfabetizacao_2024, m.meta_alfabetizacao_2025, m.meta_alfabetizacao_2026, m.meta_alfabetizacao_2027,
  m.meta_alfabetizacao_2028, m.meta_alfabetizacao_2029, m.meta_alfabetizacao_2030
FROM `${PROJETO}.${BRONZE}.meta_alfabetizacao_uf` AS m
LEFT JOIN `${PROJETO}.${SILVER}.uf_codigos` AS u ON u.sigla_uf = m.sigla_uf
WHERE m._run_id = '${RUN_ID}';
