"""Pipeline batch: bronze -> silver -> qualidade -> gold. Interrompe na primeira falha.

Uso: python -m pipeline.orquestrador
"""
import logging
import sys

from pipeline import bronze, config, gold, qualidade, silver
from pipeline.bigquery import cliente
from pipeline.monitoramento import Etapa, novo_id

log = logging.getLogger(__name__)


def main() -> int:
    client = cliente()
    execucao_id = novo_id()
    log.info("execução %s", execucao_id)
    try:
        with Etapa(client, execucao_id, "bronze") as etapa:
            etapa.linhas = bronze.executar(client, execucao_id, config.TABELAS_FONTE)
        with Etapa(client, execucao_id, "silver") as etapa:
            etapa.bytes = silver.executar(client, execucao_id)
        with Etapa(client, execucao_id, "qualidade"):
            qualidade.executar(client, execucao_id)
        with Etapa(client, execucao_id, "gold") as etapa:
            etapa.bytes = gold.executar(client)
    except Exception as erro:
        log.error("pipeline interrompido: %s: %s", type(erro).__name__, erro)
        return 1
    log.info("pipeline concluído: %s", execucao_id)
    return 0


if __name__ == "__main__":
    config.configurar_log()
    sys.exit(main())
