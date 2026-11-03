"""Executa uma consulta e imprime o resultado. Recusa consultas que leriam mais de 1 GB.

Aceita ${PROJETO}, ${BRONZE}, ${SILVER}, ${GOLD} e ${MONITORING} na consulta.

Uso: python -m scripts.consultar 'SELECT * FROM `${PROJETO}.${GOLD}.meta_vs_resultado_brasil`'
"""
import sys

from google.cloud import bigquery

from pipeline.bigquery import cliente, preparar_sql

LIMITE_MB = 1024


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        return
    sql = preparar_sql(" ".join(sys.argv[1:]))
    client = cliente()
    estimativa = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    mb = estimativa.total_bytes_processed / 1024**2
    print(f"leitura estimada: {mb:,.1f} MB")
    if mb > LIMITE_MB:
        raise SystemExit("consulta recusada: acima de 1 GB")
    resultado = client.query(sql).to_dataframe()
    print(resultado.head(50).to_string(index=False))
    print(f"({len(resultado)} linhas)")


if __name__ == "__main__":
    main()
