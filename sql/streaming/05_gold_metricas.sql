CREATE OR REPLACE TABLE `${PROJETO}.${GOLD}.streaming_metricas` AS
SELECT
  DATE(ingerido_em)                                                       AS dia,
  COUNT(*)                                                                AS mensagens_recebidas,
  COUNTIF(destino = 'valido')                                             AS validos_unicos,
  COUNTIF(destino = 'duplicado')                                          AS duplicados_descartados,
  COUNTIF(destino = 'quarentena')                                         AS invalidos_quarentena,
  COUNTIF(destino = 'valido' AND latencia_s > 300)                        AS atrasados_mais_de_5min,
  COUNTIF(destino = 'valido' AND latencia_s < 0)                          AS latencias_negativas,
  ROUND(APPROX_QUANTILES(IF(destino = 'valido' AND latencia_s <= 300, latencia_s, NULL), 100 IGNORE NULLS)[SAFE_OFFSET(50)], 3) AS latencia_p50_s,
  ROUND(APPROX_QUANTILES(IF(destino = 'valido' AND latencia_s <= 300, latencia_s, NULL), 100 IGNORE NULLS)[SAFE_OFFSET(95)], 3) AS latencia_p95_s,
  'simulado'                                                              AS origem
FROM `${PROJETO}.${SILVER}.eventos_processados`
GROUP BY dia;
