"""Remove os dados simulados do streaming (Bronze, Silver e Gold de eventos).

Uso: python -m scripts.reiniciar_streaming
"""
import logging

from pipeline import config
from pipeline.bigquery import cliente, preparar_sql

log = logging.getLogger(__name__)

COMANDOS = [
    "DELETE FROM `${PROJETO}.${BRONZE}.eventos` WHERE TRUE",
    "DELETE FROM `${PROJETO}.${SILVER}.eventos` WHERE TRUE",
    "DELETE FROM `${PROJETO}.${SILVER}.eventos_quarentena` WHERE TRUE",
    "DELETE FROM `${PROJETO}.${SILVER}.eventos_processados` WHERE TRUE",
    "DROP TABLE IF EXISTS `${PROJETO}.${SILVER}.eventos_lote`",
    "DROP TABLE IF EXISTS `${PROJETO}.${GOLD}.streaming_metricas`",
    "DROP TABLE IF EXISTS `${PROJETO}.${GOLD}.indicador_unificado_municipio`",
]


def main() -> None:
    client = cliente()
    for comando in COMANDOS:
        sql = preparar_sql(comando)
        client.query(sql).result()
        log.info(sql)


if __name__ == "__main__":
    config.configurar_log()
    main()
