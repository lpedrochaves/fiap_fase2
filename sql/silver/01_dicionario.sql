CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.dicionario` AS
SELECT id_tabela, nome_coluna, chave, valor
FROM `${PROJETO}.${BRONZE}.dicionario`
WHERE _run_id = '${RUN_ID}';
