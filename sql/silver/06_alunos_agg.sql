CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.alunos_agg` AS
SELECT
  ano, id_municipio, sigla_uf, cod_rede,
  COUNT(*)                                          AS n_registros,
  COUNTIF(avaliado)                                 AS n_avaliados,
  SUM(IF(avaliado, peso_aluno, 0))                  AS soma_pesos,
  SUM(IF(avaliado AND alfabetizado, peso_aluno, 0)) AS soma_pesos_alfabetizados
FROM `${PROJETO}.${SILVER}.alunos`
GROUP BY ano, id_municipio, sigla_uf, cod_rede;
