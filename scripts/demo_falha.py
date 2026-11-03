"""Demonstração de falha: injeta uma duplicata na Bronze, verifica que a qualidade bloqueia o pipeline e que
a Gold não é reconstruída, e restaura o dado ao final.

Uso: python -m scripts.demo_falha
"""
import logging
import sys

from pipeline import config, qualidade, silver
from pipeline.bigquery import cliente, valor
from pipeline.monitoramento import Etapa, novo_id, ultima_carga_bronze

log = logging.getLogger(__name__)


def modificada_em(client, nome: str):
    return client.get_table(f"{config.PROJETO}.{config.GOLD}.{nome}").modified


def silver_e_qualidade(client, run_id: str) -> None:
    with Etapa(client, novo_id(), "silver") as etapa:
        etapa.bytes = silver.executar(client, run_id)
    execucao_id = novo_id()
    with Etapa(client, execucao_id, "qualidade"):
        qualidade.executar(client, execucao_id)


def main() -> int:
    client = cliente()
    run_id = ultima_carga_bronze(client)
    bronze_uf = f"`{config.PROJETO}.{config.BRONZE}.uf`"
    chave = f"_run_id = '{run_id}' AND sigla_uf = 'SP' AND ano = 2024 AND rede = '5'"
    contagem = f"SELECT COUNT(*) FROM {bronze_uf} WHERE _run_id = '{run_id}'"

    antes = valor(client, contagem)
    gold_antes = modificada_em(client, "meta_vs_resultado_brasil")
    bloqueado = False
    try:
        client.query(f"INSERT INTO {bronze_uf} SELECT * REPLACE (TIMESTAMP_ADD(_ingested_at, INTERVAL 1 SECOND) "
                     f"AS _ingested_at) FROM {bronze_uf} WHERE {chave}").result()
        log.info("duplicata injetada: %s -> %s linhas", antes, valor(client, contagem))
        try:
            silver_e_qualidade(client, run_id)
        except qualidade.QualidadeError as erro:
            bloqueado = True
            log.info("pipeline bloqueado pela qualidade: %s", erro)
    finally:
        client.query(f"DELETE FROM {bronze_uf} WHERE {chave} AND _ingested_at = "
                     f"(SELECT MAX(_ingested_at) FROM {bronze_uf} WHERE _run_id = '{run_id}')").result()
    depois = valor(client, contagem)
    gold_depois = modificada_em(client, "meta_vs_resultado_brasil")
    log.info("restaurado: %s linhas (antes: %s); gold reconstruída: %s", depois, antes, gold_depois != gold_antes)

    silver_e_qualidade(client, run_id)
    ok = bloqueado and depois == antes and gold_depois == gold_antes
    log.info("demonstração %s", "concluída" if ok else "com divergência")
    return 0 if ok else 1


if __name__ == "__main__":
    config.configurar_log()
    sys.exit(main())
