CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.alunos` AS
SELECT
  CAST(a.ano AS INT64)                               AS ano,
  a.id_aluno,
  LPAD(CAST(a.id_municipio AS STRING), 7, '0')       AS id_municipio,
  u.sigla_uf,
  CAST(a.rede AS STRING)                             AS cod_rede,
  COALESCE(d.valor, 'desconhecido')                  AS rede,
  CAST(a.preenchimento_caderno AS STRING)            AS cod_preenchimento,
  CASE a.alfabetizado WHEN '1' THEN TRUE WHEN '0' THEN FALSE END AS alfabetizado,
  a.proficiencia,
  a.peso_aluno,
  (a.preenchimento_caderno = '1' AND a.proficiencia IS NOT NULL AND a.peso_aluno IS NOT NULL) AS avaliado
FROM `${PROJETO}.${BRONZE}.alunos` AS a
LEFT JOIN `${PROJETO}.${SILVER}.dicionario` AS d
       ON d.id_tabela = 'alunos' AND d.nome_coluna = 'rede' AND d.chave = CAST(a.rede AS STRING)
LEFT JOIN `${PROJETO}.${SILVER}.uf_codigos` AS u
       ON u.cod_uf = SUBSTR(LPAD(CAST(a.id_municipio AS STRING), 7, '0'), 1, 2)
WHERE a._run_id = '${RUN_ID}';
