INSERT INTO `${PROJETO}.${SILVER}.eventos_quarentena`
  (message_id, ingerido_em, motivo_rejeicao, event_id, data_bruta, processado_em)
SELECT message_id, publish_time, motivo_rejeicao, event_id, data, CURRENT_TIMESTAMP()
FROM `${PROJETO}.${SILVER}.eventos_lote`
WHERE motivo_rejeicao IS NOT NULL;
