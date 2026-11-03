CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.uf` AS
SELECT
  CAST(t.ano AS INT64)                 AS ano,
  t.sigla_uf,
  u.regiao,
  CAST(t.rede AS STRING)               AS cod_rede,
  COALESCE(d.valor, 'desconhecido')    AS rede,
  t.taxa_alfabetizacao,
  t.media_portugues
FROM `${PROJETO}.${BRONZE}.uf` AS t
LEFT JOIN `${PROJETO}.${SILVER}.dicionario` AS d
       ON d.id_tabela = 'uf' AND d.nome_coluna = 'rede' AND d.chave = CAST(t.rede AS STRING)
LEFT JOIN `${PROJETO}.${SILVER}.uf_codigos` AS u ON u.sigla_uf = t.sigla_uf
WHERE t._run_id = '${RUN_ID}';
