"""Processamento incremental (micro-lote) dos eventos recebidos em bronze.eventos.

Uso: python -m pipeline.streaming.processamento
"""
import logging

from pipeline import config
from pipeline.bigquery import cliente, preparar_sql, rodar_pasta, valor
from pipeline.monitoramento import Etapa, novo_id

log = logging.getLogger(__name__)


def executar(client) -> int:
    novas = valor(client, "SELECT COUNT(*) FROM `${PROJETO}.${BRONZE}.eventos` WHERE message_id NOT IN "
                          "(SELECT message_id FROM `${PROJETO}.${SILVER}.eventos_processados`)")
    log.info("mensagens novas: %s", novas)
    return rodar_pasta(client, "streaming")


def resumo(client) -> None:
    sql = preparar_sql("SELECT * FROM `${PROJETO}.${GOLD}.streaming_metricas` ORDER BY dia")
    for linha in client.query(sql).result():
        log.info("%s", dict(linha.items()))
        if linha["latencias_negativas"]:
            log.warning("%d eventos com latência negativa: relógio do produtor adiantado em relação ao do Pub/Sub",
                        linha["latencias_negativas"])


def main() -> None:
    client = cliente()
    with Etapa(client, novo_id(), "streaming") as etapa:
        etapa.bytes = executar(client)
    resumo(client)


if __name__ == "__main__":
    config.configurar_log()
    main()
