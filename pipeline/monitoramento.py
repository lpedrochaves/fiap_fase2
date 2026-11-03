import logging
import socket
import time
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud import logging as cloud_logging

from pipeline import config
from pipeline.bigquery import tabela, valor

log = logging.getLogger(__name__)


def agora() -> datetime:
    return datetime.now(timezone.utc)


def novo_id() -> str:
    return f"{agora():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def gravar(client: bigquery.Client, nome_tabela: str, linha: dict) -> None:
    erros = client.insert_rows_json(tabela(config.MONITORING, nome_tabela), [linha])
    if erros:
        raise RuntimeError(f"Falha ao gravar em {nome_tabela}: {erros}")


def notificar_falha(etapa: str, mensagem: str, execucao_id: str | None = None) -> None:
    try:
        cloud_logging.Client(project=config.PROJETO).logger(config.LOG_ALERTA).log_struct(
            {"etapa": etapa, "mensagem": mensagem, "execucao_id": execucao_id, "host": socket.gethostname()},
            severity="ERROR",
        )
    except Exception as exc:  # noqa: BLE001 - a falha original continua sendo propagada por quem chamou
        log.error("Não foi possível registrar a falha no Cloud Logging: %s", exc)


class Etapa:
    def __init__(self, client: bigquery.Client, execucao_id: str, nome: str):
        self.client = client
        self.execucao_id = execucao_id
        self.nome = nome
        self.linhas = None
        self.bytes = 0

    def __enter__(self):
        self.inicio = agora()
        self._t0 = time.time()
        log.info("etapa %s iniciada", self.nome)
        return self

    def __exit__(self, tipo, erro, _traceback):
        falhou = erro is not None
        duracao = round(time.time() - self._t0, 2)
        try:
            gravar(self.client, "execucoes", {
                "execucao_id": self.execucao_id,
                "etapa": self.nome,
                "status": "falha" if falhou else "sucesso",
                "inicio": self.inicio.isoformat(),
                "fim": agora().isoformat(),
                "duracao_s": duracao,
                "linhas": self.linhas,
                "bytes_faturados": self.bytes,
                "erro": f"{tipo.__name__}: {erro}"[:2000] if falhou else None,
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("Não foi possível registrar a execução: %s", exc)
        if falhou:
            notificar_falha(self.nome, f"{tipo.__name__}: {erro}", self.execucao_id)
            log.error("etapa %s falhou em %ss", self.nome, duracao)
        else:
            log.info("etapa %s concluída em %ss", self.nome, duracao)
        return False


def ultima_carga_bronze(client: bigquery.Client) -> str:
    run_id = valor(client, "SELECT execucao_id FROM `${PROJETO}.${MONITORING}.execucoes` "
                           "WHERE etapa = 'bronze' AND status = 'sucesso' ORDER BY inicio DESC LIMIT 1")
    if not run_id:
        raise RuntimeError("Nenhuma carga da Bronze concluída com sucesso")
    return run_id
