"""
Glue Job — Bronze → Silver (SOT)
Lê o histórico completo de cada entidade Bronze (todas as partições
ano=YYYY), aplica transformações por entidade, executa DQ como colunas
booleanas (_dq_*) e roteia registros para PASS ou QUARENTENA com
_quarentena_motivo.

Estratégia de particionamento:
  - PASS: particionado por `ano` (sot/pass/<entidade>/ano=YYYY/),
    espelhando o Bronze — partition pruning nas queries e reprocesso
    idempotente por ano via partitionOverwriteMode=dynamic.
  - QUARENTENA: particionada por data de processamento (anomesdia=YYYYMMDD),
    pois é trilha de auditoria: registros reprovados podem ter `ano` nulo ou
    inválido (justamente o motivo da reprovação), o que inviabiliza `ano`
    como chave física de partição.
  - Processamos o histórico completo a cada execução por decisão do time:
    o volume é pequeno (~35 mil linhas) e o indicador exige comparação
    entre anos; o custo de reprocessar tudo é menor que o risco de
    inconsistência entre partições processadas em datas diferentes.

Padrões do curso aplicados:
  - TRANSFORMACOES dict: função de transformação por entidade
  - DQ como colunas booleanas _dq_* (padrão etl_csv_ingestao.py)
  - _dq_passou consolida todos os checks críticos
  - _quarentena_motivo com concat_ws dos motivos de falha
  - separar_e_salvar_sot(): roteia PASS vs QUARENTENA
  - Job Bookmark para processamento incremental
  - SUMÁRIO ao fim
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ============================================================
# PARÂMETROS DO JOB
# ============================================================
# Glue Console → Job parameters:
#   --BUCKET_SOR  alfabetizacao-br-dev-bronze
#   --BUCKET_SOT  alfabetizacao-br-dev-silver

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_SOR', 'BUCKET_SOT'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

JOB_NAME       = args['JOB_NAME']
BUCKET_SOR     = args['BUCKET_SOR']
BUCKET_SOT     = args['BUCKET_SOT']
INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
ANOMESDIA      = datetime.now(timezone.utc).strftime("%Y%m%d")

# Decode dos códigos de rede INEP (dados reais do INEP/Base dos Dados)
# 0 = total, 2 = estadual, 3 = municipal, 5 = privada
REDE_MAP = F.create_map(
    F.lit("0"), F.lit("total"),
    F.lit("2"), F.lit("estadual"),
    F.lit("3"), F.lit("municipal"),
    F.lit("5"), F.lit("privada"),
)

log.info("=" * 65)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ANOMESDIA : {ANOMESDIA}")
log.info("=" * 65)

# ============================================================
# TRANSFORMAÇÕES POR ENTIDADE
# ============================================================

def transformar_indicador_municipio(df):
    """Decoda rede, garante tipos e descarta série (só existe série 2)."""
    return (df
        .withColumn("rede_desc",
            F.coalesce(REDE_MAP[F.col("rede")], F.col("rede")))
        .withColumn("taxa_alfabetizacao",
            F.round(F.col("taxa_alfabetizacao"), 2))
        .withColumn("media_portugues",
            F.round(F.col("media_portugues"), 2))
        .withColumn("_silver_processed_at", F.lit(INGESTION_TS))
    )

def transformar_indicador_uf(df):
    """Mesmo tratamento da entidade municipio, sem id_municipio."""
    return (df
        .withColumn("rede_desc",
            F.coalesce(REDE_MAP[F.col("rede")], F.col("rede")))
        .withColumn("taxa_alfabetizacao",
            F.round(F.col("taxa_alfabetizacao"), 2))
        .withColumn("media_portugues",
            F.round(F.col("media_portugues"), 2))
        .withColumn("_silver_processed_at", F.lit(INGESTION_TS))
    )

def transformar_meta_brasil(df):
    """Normaliza taxas e metas para double."""
    meta_cols = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]
    result = df.withColumn("_silver_processed_at", F.lit(INGESTION_TS))
    for c in meta_cols:
        result = result.withColumn(c, F.round(F.col(c), 2))
    return result

def transformar_meta_uf(df):
    return transformar_meta_brasil(df)

def transformar_meta_municipio(df):
    return transformar_meta_brasil(df)

TRANSFORMACOES = {
    "indicador_municipio": transformar_indicador_municipio,
    "indicador_uf":        transformar_indicador_uf,
    "meta_brasil":         transformar_meta_brasil,
    "meta_uf":             transformar_meta_uf,
    "meta_municipio":      transformar_meta_municipio,
}

# ============================================================
# DQ — COLUNAS BOOLEANAS POR ENTIDADE
# ============================================================

def aplicar_dq_indicador_municipio(df):
    return (df
        .withColumn("_dq_id_municipio_valido",
            F.col("id_municipio").isNotNull() & (F.length(F.col("id_municipio")) == 7))
        .withColumn("_dq_taxa_valida",
            F.col("taxa_alfabetizacao").isNotNull() &
            (F.col("taxa_alfabetizacao") >= 0) & (F.col("taxa_alfabetizacao") <= 100))
        .withColumn("_dq_ano_valido",
            F.col("ano").isNotNull() & (F.col("ano") >= 2020) & (F.col("ano") <= 2030))
        .withColumn("_dq_passou",
            F.col("_dq_id_municipio_valido") &
            F.col("_dq_taxa_valida") &
            F.col("_dq_ano_valido"))
    )

def aplicar_dq_indicador_uf(df):
    return (df
        .withColumn("_dq_uf_valida",
            F.col("sigla_uf").isNotNull() & (F.length(F.col("sigla_uf")) == 2))
        .withColumn("_dq_taxa_valida",
            F.col("taxa_alfabetizacao").isNotNull() &
            (F.col("taxa_alfabetizacao") >= 0) & (F.col("taxa_alfabetizacao") <= 100))
        .withColumn("_dq_ano_valido",
            F.col("ano").isNotNull() & (F.col("ano") >= 2020) & (F.col("ano") <= 2030))
        .withColumn("_dq_passou",
            F.col("_dq_uf_valida") & F.col("_dq_ano_valido"))
    )

def aplicar_dq_meta(df, chave_col):
    """DQ genérico para tabelas de meta."""
    return (df
        .withColumn("_dq_chave_valida", F.col(chave_col).isNotNull())
        .withColumn("_dq_ano_valido",
            F.col("ano").isNotNull() & (F.col("ano") >= 2020) & (F.col("ano") <= 2030))
        .withColumn("_dq_passou",
            F.col("_dq_chave_valida") & F.col("_dq_ano_valido"))
    )

DQ_FUNCTIONS = {
    "indicador_municipio": aplicar_dq_indicador_municipio,
    "indicador_uf":        aplicar_dq_indicador_uf,
    "meta_brasil":         lambda df: aplicar_dq_meta(df, "ano"),
    "meta_uf":             lambda df: aplicar_dq_meta(df, "sigla_uf"),
    "meta_municipio":      lambda df: aplicar_dq_meta(df, "id_municipio"),
}

# Descrições dos motivos de quarentena por entidade
MOTIVOS_DQ = {
    "indicador_municipio": [
        ("_dq_id_municipio_valido", "id_municipio ausente ou inválido (esperado 7 dígitos)"),
        ("_dq_taxa_valida",         "taxa_alfabetizacao fora de [0,100] ou nula"),
        ("_dq_ano_valido",          "ano fora de [2020,2030] ou nulo"),
    ],
    "indicador_uf": [
        ("_dq_uf_valida",  "sigla_uf ausente ou inválida (esperado 2 chars)"),
        ("_dq_ano_valido", "ano fora de [2020,2030] ou nulo"),
    ],
    "meta_brasil":    [("_dq_ano_valido", "ano fora de [2020,2030] ou nulo")],
    "meta_uf":        [("_dq_chave_valida", "sigla_uf nula"), ("_dq_ano_valido", "ano inválido")],
    "meta_municipio": [("_dq_chave_valida", "id_municipio nulo"), ("_dq_ano_valido", "ano inválido")],
}

# ============================================================
# SEPARAR E SALVAR SOT
# ============================================================

def separar_e_salvar_sot(df_dq, entidade):
    """
    Roteia registros para PASS ou QUARENTENA com _quarentena_motivo.
    Padrão: etl_csv_ingestao.py — função separar_e_salvar_sot()
    """
    motivos = MOTIVOS_DQ[entidade]

    # _quarentena_motivo = concat dos motivos de falha
    motivo_expr = F.concat_ws(", ", *[
        F.when(~F.col(col_dq), F.lit(desc))
        for col_dq, desc in motivos
    ])

    df_pass = df_dq.filter(F.col("_dq_passou"))
    df_quar = (df_dq
        .filter(~F.col("_dq_passou"))
        .withColumn("_quarentena_motivo", motivo_expr)
        .withColumn("_quarentena_ts",     F.lit(INGESTION_TS))
    )

    # PASS particionado por ano (Hive-style); QUARENTENA por data de
    # processamento — ver racional no docstring do módulo.
    pass_path = f"s3://{BUCKET_SOT}/sot/pass/{entidade}/"
    quar_path = f"s3://{BUCKET_SOT}/sot/quarentena/{entidade}/anomesdia={ANOMESDIA}/"

    df_pass.write.mode("overwrite").partitionBy("ano").parquet(pass_path)
    df_quar.write.mode("overwrite").parquet(quar_path)

    n_pass = df_pass.count()
    n_quar = df_quar.count()
    total  = n_pass + n_quar
    pct    = round(n_pass / total * 100, 1) if total else 0

    log.info(f"[SILVER] {entidade}: pass={n_pass} | quarentena={n_quar} | score={pct}%")
    return n_pass, n_quar

# ============================================================
# PROCESSAMENTO
# ============================================================

resultados = {}

for ENTIDADE, fn_transf in TRANSFORMACOES.items():
    log.info(f"[SILVER] Iniciando: {ENTIDADE}")

    # Leitura de TODAS as partições ano=YYYY da entidade — o indicador
    # exige o histórico completo para comparações entre anos.
    origem = f"s3://{BUCKET_SOR}/bronze/{ENTIDADE}/"
    df = spark.read.parquet(origem)
    log.info(f"[SILVER] {ENTIDADE}: {df.count()} registros lidos do Bronze (todas as partições de ano)")

    # Transformação
    df_transf = fn_transf(df)

    # DQ como colunas booleanas
    df_dq = DQ_FUNCTIONS[ENTIDADE](df_transf)

    # Rotear para PASS ou QUARENTENA
    n_pass, n_quar = separar_e_salvar_sot(df_dq, ENTIDADE)
    resultados[ENTIDADE] = (n_pass, n_quar)

# ============================================================
# SUMÁRIO
# ============================================================

log.info("=" * 65)
log.info("SUMÁRIO SILVER")
log.info(f"  Anomesdia : {ANOMESDIA}")
log.info(f"  {'Entidade':<30} {'PASS':>8} {'QUARENTENA':>12}")
for ent, (n_pass, n_quar) in resultados.items():
    log.info(f"  {ent:<30} {n_pass:>8} {n_quar:>12}")
log.info(f"  Destino SOT: s3://{BUCKET_SOT}/sot/")
log.info("=" * 65)

job.commit()
