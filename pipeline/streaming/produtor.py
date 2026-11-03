"""Produtor de eventos simulados para o Pub/Sub.

Gera atualizações do indicador por município (ano 2025) e injeta duplicados, eventos inválidos e
eventos atrasados para exercitar o tratamento do pipeline.

Uso: python -m pipeline.streaming.produtor --n 200
"""
import argparse
import json
import logging
import random
import time
import uuid
from collections import Counter
from datetime import timedelta

from google.cloud import pubsub_v1

from pipeline import config
from pipeline.bigquery import cliente, preparar_sql
from pipeline.monitoramento import agora

log = logging.getLogger(__name__)

DEFEITOS = ["json_quebrado", "campo_faltando", "taxa_impossivel", "municipio_inexistente", "tipo_errado"]


def carregar_municipios(client) -> list[tuple[str, float]]:
    sql = ("SELECT id_municipio, taxa_alfabetizacao FROM `${PROJETO}.${SILVER}.municipio` "
           "WHERE cod_rede = '3' AND ano = 2024 AND taxa_alfabetizacao IS NOT NULL")
    return [(r[0], float(r[1])) for r in client.query(preparar_sql(sql)).result()]


def evento_valido(municipios) -> dict:
    id_municipio, base = random.choice(municipios)
    taxa = min(100.0, max(1.0, base + random.gauss(2.5, 3.0)))
    return {
        "event_id": str(uuid.uuid4()),
        "tipo_evento": "atualizacao_indicador",
        "produced_at": agora().isoformat(),
        "id_municipio": id_municipio,
        "ano": 2025,
        "taxa_alfabetizacao": round(taxa, 2),
    }


def corromper(evento: dict) -> bytes:
    defeito = random.choice(DEFEITOS)
    if defeito == "json_quebrado":
        return b'{"event_id": "quebrado", "taxa_alfabetizacao": '
    if defeito == "campo_faltando":
        evento.pop(random.choice(["ano", "taxa_alfabetizacao", "id_municipio"]))
    elif defeito == "taxa_impossivel":
        evento["taxa_alfabetizacao"] = random.choice([135.5, -4.0, 0.0])
    elif defeito == "municipio_inexistente":
        evento["id_municipio"] = "9999999"
    elif defeito == "tipo_errado":
        evento["tipo_evento"] = "relatorio_mensal"
    return json.dumps(evento).encode("utf-8")


def gerar(municipios, n: int, p_dup: float, p_inv: float, p_atraso: float):
    validos = []
    for _ in range(n):
        sorteio = random.random()
        evento = evento_valido(municipios)
        if sorteio < p_inv:
            yield corromper(evento), "invalido"
        elif sorteio < p_inv + p_atraso:
            evento["produced_at"] = (agora() - timedelta(minutes=random.randint(6, 45))).isoformat()
            corpo = json.dumps(evento).encode("utf-8")
            validos.append(corpo)
            yield corpo, "atrasado"
        else:
            corpo = json.dumps(evento).encode("utf-8")
            validos.append(corpo)
            yield corpo, "valido"
    for corpo in random.sample(validos, min(round(n * p_dup), len(validos))):
        yield corpo, "duplicado"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument("--duplicados", type=float, default=0.05)
    parser.add_argument("--invalidos", type=float, default=0.05)
    parser.add_argument("--atrasados", type=float, default=0.05)
    parser.add_argument("--ritmo", type=float, default=50.0, help="mensagens por segundo")
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    municipios = carregar_municipios(cliente())
    publicador = pubsub_v1.PublisherClient()
    topico = publicador.topic_path(config.PROJETO, config.TOPICO)

    futuros, contagem = [], Counter()
    for corpo, tipo in gerar(municipios, args.n, args.duplicados, args.invalidos, args.atrasados):
        futuros.append(publicador.publish(topico, corpo))
        contagem[tipo] += 1
        time.sleep(1.0 / args.ritmo)
    for futuro in futuros:
        futuro.result(timeout=120)

    log.info("%d mensagens publicadas em %s: %s", sum(contagem.values()), config.TOPICO, dict(contagem))


if __name__ == "__main__":
    config.configurar_log()
    main()
