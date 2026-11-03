INSERT INTO `${PROJETO}.${SILVER}.eventos`
  (event_id, id_municipio, ano, taxa_alfabetizacao, produced_at, ingerido_em, latencia_publicacao_s, processado_em,
   message_id, origem)
SELECT
  event_id, id_municipio, ano, taxa_alfabetizacao, produced_at, publish_time, latencia_s, CURRENT_TIMESTAMP(),
  message_id, 'simulado'
FROM `${PROJETO}.${SILVER}.eventos_lote`
WHERE motivo_rejeicao IS NULL
  AND ordem_no_lote = 1
  AND NOT ja_existe_na_silver;
