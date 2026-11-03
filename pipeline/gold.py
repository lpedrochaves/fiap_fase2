"""Camada Gold: executa sql/gold.

Uso: python -m pipeline.gold
"""
from pipeline import config
from pipeline.bigquery import cliente, rodar_pasta
from pipeline.monitoramento import Etapa, novo_id


def executar(client) -> int:
    return rodar_pasta(client, "gold")


def main() -> None:
    client = cliente()
    with Etapa(client, novo_id(), "gold") as etapa:
        etapa.bytes = executar(client)


if __name__ == "__main__":
    config.configurar_log()
    main()
