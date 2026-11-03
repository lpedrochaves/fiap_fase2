"""Cria a métrica baseada em log, o canal de e-mail e a política de alerta de falhas. Idempotente.

Uso: python -m infra.alerta
"""
import logging
import time

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import logging as cloud_logging

from pipeline import config

log = logging.getLogger(__name__)

METRICA = "pipeline_falhas"
CANAL = "Pipeline alfabetizacao - e-mail"
POLITICA = "Pipeline alfabetizacao: falha de execucao"
API = "https://monitoring.googleapis.com/v3"


def sessao() -> AuthorizedSession:
    credenciais, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credenciais)


def criar_metrica() -> None:
    filtro = f'logName="projects/{config.PROJETO}/logs/{config.LOG_ALERTA}" AND severity>=ERROR'
    metrica = cloud_logging.Client(project=config.PROJETO).metric(
        METRICA, filter_=filtro, description="Falhas de execução do pipeline")
    if metrica.exists():
        log.info("métrica %s já existe", METRICA)
    else:
        metrica.create()
        log.info("métrica %s criada", METRICA)


def criar_canal(s: AuthorizedSession, email: str) -> str:
    base = f"{API}/projects/{config.PROJETO}/notificationChannels"
    for canal in s.get(base).json().get("notificationChannels", []):
        if canal.get("displayName") == CANAL and canal.get("labels", {}).get("email_address") == email:
            log.info("canal de e-mail já existe")
            return canal["name"]
    resposta = s.post(base, json={"type": "email", "displayName": CANAL, "labels": {"email_address": email}})
    resposta.raise_for_status()
    log.info("canal de e-mail criado")
    return resposta.json()["name"]


def aguardar_metrica(s: AuthorizedSession, tentativas: int = 20) -> None:
    url = (f"{API}/projects/{config.PROJETO}/metricDescriptors"
           f"?filter=metric.type%3D%22logging.googleapis.com%2Fuser%2F{METRICA}%22")
    for _ in range(tentativas):
        if s.get(url).json().get("metricDescriptors"):
            return
        time.sleep(15)
    raise TimeoutError(f"métrica {METRICA} indisponível no Cloud Monitoring")


def criar_politica(s: AuthorizedSession, canal: str) -> None:
    base = f"{API}/projects/{config.PROJETO}/alertPolicies"
    if any(p.get("displayName") == POLITICA for p in s.get(base).json().get("alertPolicies", [])):
        log.info("política de alerta já existe")
        return
    resposta = s.post(base, json={
        "displayName": POLITICA,
        "combiner": "OR",
        "conditions": [{
            "displayName": "Falha registrada no último minuto",
            "conditionThreshold": {
                "filter": f'metric.type="logging.googleapis.com/user/{METRICA}" resource.type="global"',
                "comparison": "COMPARISON_GT",
                "thresholdValue": 0,
                "duration": "0s",
                "aggregations": [{"alignmentPeriod": "60s", "perSeriesAligner": "ALIGN_SUM"}],
                "trigger": {"count": 1},
            },
        }],
        "notificationChannels": [canal],
        "alertStrategy": {"autoClose": "1800s"},
    })
    resposta.raise_for_status()
    log.info("política de alerta criada")


def main() -> None:
    if not config.ALERTA_EMAIL:
        raise SystemExit("ALERTA_EMAIL não definido no .env")
    s = sessao()
    criar_metrica()
    canal = criar_canal(s, config.ALERTA_EMAIL)
    aguardar_metrica(s)
    criar_politica(s, canal)


if __name__ == "__main__":
    config.configurar_log()
    main()
