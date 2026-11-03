"""Camada Silver: executa sql/silver sobre uma carga da Bronze.

Uso: python -m pipeline.silver [--carga RUN_ID]
"""
import argparse

from pipeline import config
from pipeline.bigquery import cliente, rodar_pasta
from pipeline.monitoramento import Etapa, novo_id, ultima_carga_bronze


def executar(client, run_id: str) -> int:
    return rodar_pasta(client, "silver", RUN_ID=run_id)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--carga")
    args = parser.parse_args()

    client = cliente()
    run_id = args.carga or ultima_carga_bronze(client)
    with Etapa(client, novo_id(), "silver") as etapa:
        etapa.bytes = executar(client, run_id)


if __name__ == "__main__":
    config.configurar_log()
    main()
