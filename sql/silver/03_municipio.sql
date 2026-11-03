CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.municipio` AS
SELECT
  CAST(m.ano AS INT64)                              AS ano,
  LPAD(CAST(m.id_municipio AS STRING), 7, '0')      AS id_municipio,
  u.sigla_uf,
  u.regiao,
  CAST(m.rede AS STRING)                            AS cod_rede,
  COALESCE(d.valor, 'desconhecido')                 AS rede,
  m.taxa_alfabetizacao,
  m.media_portugues
FROM `${PROJETO}.${BRONZE}.municipio` AS m
LEFT JOIN `${PROJETO}.${SILVER}.dicionario` AS d
       ON d.id_tabela = 'municipio' AND d.nome_coluna = 'rede' AND d.chave = CAST(m.rede AS STRING)
LEFT JOIN `${PROJETO}.${SILVER}.uf_codigos` AS u
       ON u.cod_uf = SUBSTR(LPAD(CAST(m.id_municipio AS STRING), 7, '0'), 1, 2)
WHERE m._run_id = '${RUN_ID}';
