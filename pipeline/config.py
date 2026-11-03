import logging
import os
from pathlib import Path

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")

PROJETO = os.environ.get("GCP_PROJECT")
if not PROJETO:
    raise RuntimeError("GCP_PROJECT não definido no .env")
LOCATION = os.environ.get("BQ_LOCATION", "US")

FONTE = "basedosdados.br_inep_avaliacao_alfabetizacao"
TABELAS_FONTE = [
    "alunos", "dicionario", "meta_alfabetizacao_brasil", "meta_alfabetizacao_municipio",
    "meta_alfabetizacao_uf", "municipio", "uf",
]

BRONZE = os.environ.get("BQ_DATASET_BRONZE", "bronze")
SILVER = os.environ.get("BQ_DATASET_SILVER", "silver")
GOLD = os.environ.get("BQ_DATASET_GOLD", "gold")
MONITORING = os.environ.get("BQ_DATASET_MONITORING", "monitoring")

TOPICO = os.environ.get("PUBSUB_TOPICO", "alfabetizacao-eventos")
ASSINATURA = os.environ.get("PUBSUB_ASSINATURA", "alfabetizacao-eventos-bq")

LOG_ALERTA = "pipeline_alfabetizacao"
ALERTA_EMAIL = os.environ.get("ALERTA_EMAIL")

PASTA_SQL = RAIZ / "sql"
LIMITE_BYTES = 5 * 1024**3


def configurar_log() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
