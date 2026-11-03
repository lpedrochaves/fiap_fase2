INSERT INTO `${PROJETO}.${SILVER}.eventos_processados`
  (message_id, ingerido_em, destino, motivo_rejeicao, latencia_s, processado_em)
SELECT
  message_id, publish_time,
  CASE
    WHEN motivo_rejeicao IS NOT NULL THEN 'quarentena'
    WHEN ordem_no_lote > 1 OR ja_existe_na_silver THEN 'duplicado'
    ELSE 'valido'
  END,
  motivo_rejeicao, latencia_s, CURRENT_TIMESTAMP()
FROM `${PROJETO}.${SILVER}.eventos_lote`;
