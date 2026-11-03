"""Ingestão batch da fonte pública para a camada Bronze (somente append).

Uso: python -m pipeline.bronze [--dry-run]
"""
import argparse
import logging
from datetime import datetime

from google.cloud import bigquery

from pipeline import config
from pipeline.bigquery import cliente
from pipeline.monitoramento import Etapa, agora, novo_id

log = logging.getLogger(__name__)


class IntegridadeError(RuntimeError):
    pass


def contar_linhas_fonte(client: bigquery.Client, tabela: str) -> int:
    linhas = list(client.query(f"SELECT COUNT(*) FROM `{config.FONTE}.{tabela}`").result())
    return linhas[0][0]


def criar_tabela(client: bigquery.Client, tabela: str) -> None:
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{config.PROJETO}.{config.BRONZE}.{tabela}`
        PARTITION BY DATE(_ingested_at)
        CLUSTER BY _run_id
        AS SELECT s.*, CAST(NULL AS STRING) AS _run_id, CAST(NULL AS TIMESTAMP) AS _ingested_at
        FROM `{config.FONTE}.{tabela}` AS s
        WHERE FALSE
    """).result()


def inserir_carga(client: bigquery.Client, tabela: str, run_id: str, ingerido_em: datetime) -> bigquery.QueryJob:
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=2 * 1024**3,
        labels={"projeto": "alfabetizacao", "etapa": "bronze"},
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("ingerido_em", "TIMESTAMP", ingerido_em),
        ],
    )
    job = client.query(f"""
        INSERT INTO `{config.PROJETO}.{config.BRONZE}.{tabela}`
        SELECT s.*, @run_id AS _run_id, @ingerido_em AS _ingested_at
        FROM `{config.FONTE}.{tabela}` AS s
    """, job_config=job_config)
    job.result()
    return job


def executar(client: bigquery.Client, run_id: str, tabelas: list[str]) -> int:
    ingerido_em = agora()
    total = 0
    for tabela in tabelas:
        esperado = contar_linhas_fonte(client, tabela)
        criar_tabela(client, tabela)
        job = inserir_carga(client, tabela, run_id, ingerido_em)
        gravadas = job.num_dml_affected_rows
        if gravadas != esperado:
            raise IntegridadeError(f"{tabela}: fonte com {esperado} linhas, gravadas {gravadas}")
        total += gravadas
        log.info("%s: %d linhas (%.1f MB)", tabela, gravadas, job.total_bytes_billed / 1024**2)
    return total


def estimar(client: bigquery.Client, tabelas: list[str]) -> None:
    total = 0
    for tabela in tabelas:
        job = client.query(f"SELECT s.* FROM `{config.FONTE}.{tabela}` AS s",
                           job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        total += job.total_bytes_processed
        log.info("%s: %.2f MB", tabela, job.total_bytes_processed / 1024**2)
    log.info("total estimado: %.1f MB", total / 1024**2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    client = cliente()
    if args.dry_run:
        estimar(client, config.TABELAS_FONTE)
        return

    run_id = novo_id()
    with Etapa(client, run_id, "bronze") as etapa:
        etapa.linhas = executar(client, run_id, config.TABELAS_FONTE)
    log.info("carga %s concluída", run_id)


if __name__ == "__main__":
    config.configurar_log()
    main()
