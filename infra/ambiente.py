"""Cria datasets, tabelas auxiliares, tópico, assinatura BigQuery e a permissão do Pub/Sub. Idempotente.

Uso: python -m infra.ambiente
"""
import logging

import google.auth
from google.api_core.exceptions import AlreadyExists, NotFound
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, pubsub_v1

from pipeline import config
from pipeline.bigquery import cliente

log = logging.getLogger(__name__)

TABELAS = [
    (config.MONITORING, "execucoes",
     "execucao_id STRING, etapa STRING, status STRING, inicio TIMESTAMP, fim TIMESTAMP, duracao_s FLOAT64, "
     "linhas INT64, bytes_faturados INT64, erro STRING", "DATE(inicio)"),
    (config.MONITORING, "qualidade",
     "execucao_id STRING, quando TIMESTAMP, regra STRING, tipo STRING, resultado STRING, violacoes INT64, "
     "descricao STRING", "DATE(quando)"),
    (config.MONITORING, "joins",
     "execucao_id STRING, quando TIMESTAMP, nome STRING, base INT64, casadas INT64, taxa FLOAT64, limiar FLOAT64, "
     "aprovado BOOL", "DATE(quando)"),
    (config.BRONZE, "eventos",
     "subscription_name STRING, message_id STRING, publish_time TIMESTAMP, data STRING, attributes STRING",
     "DATE(publish_time)"),
    (config.SILVER, "eventos",
     "event_id STRING, id_municipio STRING, ano INT64, taxa_alfabetizacao FLOAT64, produced_at TIMESTAMP, "
     "ingerido_em TIMESTAMP, latencia_publicacao_s FLOAT64, processado_em TIMESTAMP, message_id STRING, "
     "origem STRING", None),
    (config.SILVER, "eventos_quarentena",
     "message_id STRING, ingerido_em TIMESTAMP, motivo_rejeicao STRING, event_id STRING, data_bruta STRING, "
     "processado_em TIMESTAMP", None),
    (config.SILVER, "eventos_processados",
     "message_id STRING, ingerido_em TIMESTAMP, destino STRING, motivo_rejeicao STRING, latencia_s FLOAT64, "
     "processado_em TIMESTAMP", None),
]


def criar_datasets(client: bigquery.Client) -> None:
    for nome in (config.BRONZE, config.SILVER, config.GOLD, config.MONITORING):
        dataset = bigquery.Dataset(f"{config.PROJETO}.{nome}")
        dataset.location = config.LOCATION
        client.create_dataset(dataset, exists_ok=True)
        log.info("dataset %s", nome)


def criar_tabelas(client: bigquery.Client) -> None:
    for dataset, nome, colunas, particao in TABELAS:
        sql = f"CREATE TABLE IF NOT EXISTS `{config.PROJETO}.{dataset}.{nome}` ({colunas})"
        if particao:
            sql += f" PARTITION BY {particao}"
        client.query(sql).result()
        log.info("tabela %s.%s", dataset, nome)


def numero_do_projeto() -> str:
    credenciais, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    resposta = AuthorizedSession(credenciais).get(
        f"https://cloudresourcemanager.googleapis.com/v1/projects/{config.PROJETO}")
    resposta.raise_for_status()
    return resposta.json()["projectNumber"]


def conceder_permissao(client: bigquery.Client) -> None:
    agente = f"serviceAccount:service-{numero_do_projeto()}@gcp-sa-pubsub.iam.gserviceaccount.com"
    tabela = client.get_table(f"{config.PROJETO}.{config.BRONZE}.eventos")
    politica = client.get_iam_policy(tabela)
    existentes = {(b["role"], m) for b in politica.bindings for m in b["members"]}
    for papel in ("roles/bigquery.dataEditor", "roles/bigquery.metadataViewer"):
        if (papel, agente) not in existentes:
            politica.bindings.append({"role": papel, "members": [agente]})
    client.set_iam_policy(tabela, politica)
    log.info("permissão do Pub/Sub em %s.eventos", config.BRONZE)


def criar_pubsub() -> None:
    publicador = pubsub_v1.PublisherClient()
    assinante = pubsub_v1.SubscriberClient()
    topico = publicador.topic_path(config.PROJETO, config.TOPICO)
    try:
        publicador.create_topic(request={"name": topico})
        log.info("tópico %s criado", config.TOPICO)
    except AlreadyExists:
        log.info("tópico %s já existe", config.TOPICO)

    caminho = assinante.subscription_path(config.PROJETO, config.ASSINATURA)
    try:
        assinante.get_subscription(request={"subscription": caminho})
        log.info("assinatura %s já existe", config.ASSINATURA)
    except NotFound:
        assinante.create_subscription(request={
            "name": caminho,
            "topic": topico,
            "bigquery_config": {"table": f"{config.PROJETO}.{config.BRONZE}.eventos", "write_metadata": True},
        })
        log.info("assinatura %s criada", config.ASSINATURA)


def main() -> None:
    client = cliente()
    criar_datasets(client)
    criar_tabelas(client)
    conceder_permissao(client)
    criar_pubsub()


if __name__ == "__main__":
    config.configurar_log()
    main()
