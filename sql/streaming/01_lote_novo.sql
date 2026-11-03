CREATE OR REPLACE TABLE `${PROJETO}.${SILVER}.eventos_lote` AS
WITH novos AS (
  SELECT message_id, publish_time, data
  FROM `${PROJETO}.${BRONZE}.eventos`
  WHERE message_id NOT IN (SELECT message_id FROM `${PROJETO}.${SILVER}.eventos_processados`)
),
lidos AS (
  SELECT message_id, publish_time, data, SAFE.PARSE_JSON(data) AS j
  FROM novos
),
campos AS (
  SELECT *,
    JSON_VALUE(j, '$.event_id')           AS event_id,
    JSON_VALUE(j, '$.tipo_evento')        AS tipo_evento,
    JSON_VALUE(j, '$.produced_at')        AS produced_at_txt,
    JSON_VALUE(j, '$.id_municipio')       AS id_municipio,
    JSON_VALUE(j, '$.ano')                AS ano_txt,
    JSON_VALUE(j, '$.taxa_alfabetizacao') AS taxa_txt
  FROM lidos
),
tipado AS (
  SELECT *,
    SAFE_CAST(produced_at_txt AS TIMESTAMP) AS produced_at,
    SAFE_CAST(ano_txt AS INT64)             AS ano,
    SAFE_CAST(taxa_txt AS FLOAT64)          AS taxa_alfabetizacao
  FROM campos
),
municipios_validos AS (
  SELECT DISTINCT id_municipio FROM `${PROJETO}.${SILVER}.municipio` WHERE cod_rede = '3'
),
classificado AS (
  SELECT t.*,
    CASE
      WHEN t.j IS NULL THEN 'json_invalido'
      WHEN t.event_id IS NULL OR t.tipo_evento IS NULL OR t.produced_at_txt IS NULL
           OR t.id_municipio IS NULL OR t.ano_txt IS NULL OR t.taxa_txt IS NULL THEN 'campo_obrigatorio_ausente'
      WHEN t.tipo_evento != 'atualizacao_indicador' THEN 'tipo_evento_desconhecido'
      WHEN t.produced_at IS NULL THEN 'produced_at_invalido'
      WHEN NOT REGEXP_CONTAINS(t.id_municipio, r'^[0-9]{7}$') THEN 'id_municipio_invalido'
      WHEN m.id_municipio IS NULL THEN 'id_municipio_inexistente'
      WHEN t.ano IS NULL OR t.ano NOT BETWEEN 2023 AND 2030 THEN 'ano_invalido'
      WHEN t.taxa_alfabetizacao IS NULL OR t.taxa_alfabetizacao <= 0 OR t.taxa_alfabetizacao > 100 THEN 'taxa_fora_da_faixa'
    END AS motivo_rejeicao
  FROM tipado AS t
  LEFT JOIN municipios_validos AS m ON m.id_municipio = t.id_municipio
)
SELECT
  c.* EXCEPT (j, produced_at_txt, ano_txt, taxa_txt),
  IF(c.motivo_rejeicao IS NULL,
     ROW_NUMBER() OVER (PARTITION BY c.event_id, (c.motivo_rejeicao IS NULL) ORDER BY c.publish_time, c.message_id),
     NULL) AS ordem_no_lote,
  (e.event_id IS NOT NULL) AS ja_existe_na_silver,
  TIMESTAMP_DIFF(c.publish_time, c.produced_at, MILLISECOND) / 1000.0 AS latencia_s
FROM classificado AS c
LEFT JOIN (SELECT DISTINCT event_id FROM `${PROJETO}.${SILVER}.eventos`) AS e ON e.event_id = c.event_id;
