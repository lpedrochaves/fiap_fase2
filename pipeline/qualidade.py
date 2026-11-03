"""Regras de qualidade da Silver e taxa de correspondência dos joins.

Regras bloqueantes interrompem o pipeline antes da Gold. Todos os resultados são gravados em
monitoring.qualidade e monitoring.joins.

Uso: python -m pipeline.qualidade
"""
import logging

from pipeline import config
from pipeline.bigquery import cliente, preparar_sql, valor
from pipeline.monitoramento import Etapa, agora, gravar, novo_id

log = logging.getLogger(__name__)


class QualidadeError(RuntimeError):
    pass


def t(nome: str) -> str:
    return f"`${{PROJETO}}.${{SILVER}}.{nome}`"


REGRAS = [
    ("duplicidade_municipio", "bloqueante", "chave (ano, id_municipio, rede) repetida",
     f"SELECT COUNT(*) FROM (SELECT 1 FROM {t('municipio')} GROUP BY ano, id_municipio, cod_rede HAVING COUNT(*) > 1)"),
    ("duplicidade_uf", "bloqueante", "chave (ano, sigla_uf, rede) repetida",
     f"SELECT COUNT(*) FROM (SELECT 1 FROM {t('uf')} GROUP BY ano, sigla_uf, cod_rede HAVING COUNT(*) > 1)"),
    ("duplicidade_alunos", "bloqueante", "chave (ano, id_aluno) repetida",
     f"SELECT COUNT(*) FROM (SELECT 1 FROM {t('alunos')} GROUP BY ano, id_aluno HAVING COUNT(*) > 1)"),
    ("duplicidade_meta_municipio", "bloqueante", "chave (ano, id_municipio) repetida nas metas",
     f"SELECT COUNT(*) FROM (SELECT 1 FROM {t('meta_municipio')} GROUP BY ano, id_municipio HAVING COUNT(*) > 1)"),
    ("duplicidade_meta_uf", "bloqueante", "chave (ano, sigla_uf) repetida nas metas",
     f"SELECT COUNT(*) FROM (SELECT 1 FROM {t('meta_uf')} GROUP BY ano, sigla_uf HAVING COUNT(*) > 1)"),
    ("chave_nula", "bloqueante", "ano, id_municipio ou id_aluno nulos",
     f"SELECT (SELECT COUNTIF(ano IS NULL OR id_municipio IS NULL) FROM {t('municipio')}) "
     f"+ (SELECT COUNTIF(id_aluno IS NULL) FROM {t('alunos')})"),
    ("id_municipio_7_digitos", "bloqueante", "id_municipio fora do formato de 7 dígitos",
     f"SELECT (SELECT COUNTIF(LENGTH(id_municipio) != 7) FROM {t('municipio')}) "
     f"+ (SELECT COUNTIF(LENGTH(id_municipio) != 7) FROM {t('alunos_agg')}) "
     f"+ (SELECT COUNTIF(LENGTH(id_municipio) != 7) FROM {t('meta_municipio')})"),
    ("uf_nao_encontrada", "bloqueante", "município sem UF correspondente",
     f"SELECT (SELECT COUNTIF(sigla_uf IS NULL) FROM {t('municipio')}) "
     f"+ (SELECT COUNTIF(sigla_uf IS NULL) FROM {t('alunos_agg')})"),
    ("rede_desconhecida", "bloqueante", "rede sem tradução no dicionário",
     f"SELECT (SELECT COUNTIF(rede = 'desconhecido') FROM {t('municipio')}) "
     f"+ (SELECT COUNTIF(rede = 'desconhecido') FROM {t('uf')}) "
     f"+ (SELECT COUNTIF(rede = 'desconhecido') FROM {t('alunos')}) "
     f"+ (SELECT COUNTIF(cod_rede = 'desconhecido') FROM {t('meta_municipio')}) "
     f"+ (SELECT COUNTIF(cod_rede = 'desconhecido') FROM {t('meta_uf')}) "
     f"+ (SELECT COUNTIF(cod_rede = 'desconhecido') FROM {t('meta_brasil')})"),
    ("taxa_fora_da_faixa", "bloqueante", "taxa de alfabetização <= 0 ou > 100",
     f"SELECT (SELECT COUNTIF(taxa_alfabetizacao <= 0 OR taxa_alfabetizacao > 100) FROM {t('municipio')}) "
     f"+ (SELECT COUNTIF(taxa_alfabetizacao <= 0 OR taxa_alfabetizacao > 100) FROM {t('uf')})"),
    ("proficiencia_ou_peso_invalido", "bloqueante", "proficiência ou peso <= 0",
     f"SELECT COUNTIF(proficiencia <= 0 OR peso_aluno <= 0) FROM {t('alunos')}"),
    ("alfabetizado_vs_743", "bloqueante", "alfabetizado inconsistente com proficiência >= 743",
     f"SELECT COUNTIF(avaliado AND alfabetizado != (proficiencia >= 743)) FROM {t('alunos')}"),
    ("meta_municipio_sem_taxa", "informativa", "meta municipal sem taxa",
     f"SELECT COUNTIF(taxa_alfabetizacao IS NULL) FROM {t('meta_municipio')}"),
    ("meta_2030_diferente_de_80", "informativa", "meta 2030 ausente ou diferente de 80",
     f"SELECT (SELECT COUNTIF(meta_alfabetizacao_2030 IS NULL OR meta_alfabetizacao_2030 != 80) FROM {t('meta_municipio')}) "
     f"+ (SELECT COUNTIF(meta_alfabetizacao_2030 IS NULL OR meta_alfabetizacao_2030 != 80) FROM {t('meta_uf')}) "
     f"+ (SELECT COUNTIF(meta_alfabetizacao_2030 IS NULL OR meta_alfabetizacao_2030 != 80) FROM {t('meta_brasil')})"),
    ("taxa_diverge_entre_tabelas", "informativa", "taxa do município difere da tabela de metas (> 0,06 p.p.)",
     f"SELECT COUNTIF(ABS(r.taxa_alfabetizacao - m.taxa_alfabetizacao) > 0.06) "
     f"FROM {t('municipio')} AS r JOIN {t('meta_municipio')} AS m ON m.ano = r.ano AND m.id_municipio = r.id_municipio "
     f"WHERE r.cod_rede = '3' AND m.cod_rede = '3'"),
]

JOINS = [
    ("municipio_para_meta_municipio", 0.95,
     f"SELECT COUNT(*) AS base, COUNTIF(m.id_municipio IS NOT NULL) AS casadas FROM {t('municipio')} AS r "
     f"LEFT JOIN (SELECT DISTINCT ano, id_municipio FROM {t('meta_municipio')} WHERE cod_rede = '3') AS m "
     f"ON m.ano = r.ano AND m.id_municipio = r.id_municipio WHERE r.cod_rede = '3'"),
    ("meta_municipio_para_municipio", 0.95,
     f"SELECT COUNT(*) AS base, COUNTIF(r.id_municipio IS NOT NULL) AS casadas FROM {t('meta_municipio')} AS m "
     f"LEFT JOIN (SELECT DISTINCT ano, id_municipio FROM {t('municipio')} WHERE cod_rede = '3') AS r "
     f"ON r.ano = m.ano AND r.id_municipio = m.id_municipio WHERE m.cod_rede = '3'"),
    ("uf_para_meta_uf", 0.85,
     f"SELECT COUNT(*) AS base, COUNTIF(m.sigla_uf IS NOT NULL) AS casadas FROM {t('uf')} AS r "
     f"LEFT JOIN (SELECT DISTINCT ano, sigla_uf FROM {t('meta_uf')} WHERE cod_rede = '5') AS m "
     f"ON m.ano = r.ano AND m.sigla_uf = r.sigla_uf WHERE r.cod_rede = '5'"),
    ("meta_uf_para_uf", 0.85,
     f"SELECT COUNT(*) AS base, COUNTIF(r.sigla_uf IS NOT NULL) AS casadas FROM {t('meta_uf')} AS m "
     f"LEFT JOIN (SELECT DISTINCT ano, sigla_uf FROM {t('uf')} WHERE cod_rede = '5') AS r "
     f"ON r.ano = m.ano AND r.sigla_uf = m.sigla_uf "
     f"WHERE m.cod_rede = '5' AND m.ano IN (SELECT DISTINCT ano FROM {t('uf')} WHERE cod_rede = '5')"),
    ("alunos_para_uf", 0.999,
     f"SELECT COUNT(DISTINCT id_municipio) AS base, COUNT(DISTINCT IF(sigla_uf IS NOT NULL, id_municipio, NULL)) AS casadas "
     f"FROM {t('alunos_agg')}"),
    ("alunos_para_municipio", 0.95,
     f"SELECT COUNT(*) AS base, COUNTIF(r.id_municipio IS NOT NULL) AS casadas FROM "
     f"(SELECT DISTINCT ano, id_municipio FROM {t('alunos_agg')} WHERE cod_rede = '3') AS a "
     f"LEFT JOIN (SELECT DISTINCT ano, id_municipio FROM {t('municipio')} WHERE cod_rede = '3') AS r "
     f"ON r.ano = a.ano AND r.id_municipio = a.id_municipio"),
]


def executar(client, execucao_id: str) -> None:
    quando = agora().isoformat()
    falhas = []

    for nome, tipo, descricao, sql in REGRAS:
        violacoes = int(valor(client, sql) or 0)
        resultado = "OK" if violacoes == 0 else ("FALHOU" if tipo == "bloqueante" else "ALERTA")
        gravar(client, "qualidade", {
            "execucao_id": execucao_id, "quando": quando, "regra": nome, "tipo": tipo,
            "resultado": resultado, "violacoes": violacoes, "descricao": descricao,
        })
        log.info("[%s] %s (%s): %d", resultado, nome, tipo, violacoes)
        if resultado == "FALHOU":
            falhas.append(f"{nome} ({violacoes})")

    for nome, limiar, sql in JOINS:
        linha = list(client.query(preparar_sql(sql)).result())[0]
        base, casadas = int(linha["base"]), int(linha["casadas"])
        taxa = round(casadas / base, 4) if base else 0.0
        aprovado = taxa >= limiar
        gravar(client, "joins", {
            "execucao_id": execucao_id, "quando": quando, "nome": nome, "base": base, "casadas": casadas,
            "taxa": taxa, "limiar": limiar, "aprovado": aprovado,
        })
        log.info("[%s] join %s: %d/%d = %.2f%% (mínimo %.1f%%)",
                 "OK" if aprovado else "FALHOU", nome, casadas, base, taxa * 100, limiar * 100)
        if not aprovado:
            falhas.append(f"join {nome} ({taxa:.2%} < {limiar:.1%})")

    if falhas:
        raise QualidadeError("Regras bloqueantes falharam: " + "; ".join(falhas))


def main() -> None:
    client = cliente()
    execucao_id = novo_id()
    with Etapa(client, execucao_id, "qualidade"):
        executar(client, execucao_id)


if __name__ == "__main__":
    config.configurar_log()
    main()
