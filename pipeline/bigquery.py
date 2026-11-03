import logging

from google.cloud import bigquery

from pipeline import config

log = logging.getLogger(__name__)


def cliente() -> bigquery.Client:
    return bigquery.Client(project=config.PROJETO, location=config.LOCATION)


def preparar_sql(texto: str, **parametros) -> str:
    trocas = {
        "${PROJETO}": config.PROJETO,
        "${FONTE}": config.FONTE,
        "${BRONZE}": config.BRONZE,
        "${SILVER}": config.SILVER,
        "${GOLD}": config.GOLD,
        "${MONITORING}": config.MONITORING,
    }
    trocas.update({"${" + chave + "}": str(valor) for chave, valor in parametros.items()})
    for marcador, valor in trocas.items():
        texto = texto.replace(marcador, valor)
    return texto


def rodar_sql(client: bigquery.Client, arquivo: str, **parametros) -> bigquery.QueryJob:
    sql = preparar_sql((config.PASTA_SQL / arquivo).read_text(encoding="utf-8"), **parametros)
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=config.LIMITE_BYTES,
                                         labels={"projeto": "alfabetizacao"})
    job = client.query(sql, job_config=job_config)
    job.result()
    return job


def rodar_pasta(client: bigquery.Client, pasta: str, **parametros) -> int:
    total = 0
    for arquivo in sorted((config.PASTA_SQL / pasta).glob("*.sql")):
        job = rodar_sql(client, f"{pasta}/{arquivo.name}", **parametros)
        faturado = job.total_bytes_billed or 0
        total += faturado
        log.info("%s/%s (%.1f MB)", pasta, arquivo.name, faturado / 1024**2)
    return total


def valor(client: bigquery.Client, sql: str, **parametros):
    linhas = list(client.query(preparar_sql(sql, **parametros)).result())
    return linhas[0][0] if linhas else None


def tabela(dataset: str, nome: str) -> str:
    return f"{config.PROJETO}.{dataset}.{nome}"
