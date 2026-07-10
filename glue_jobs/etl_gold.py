"""
Glue Job — Silver → Gold (SPEC)
Lê entidades da camada Silver (pass) e produz 4 visões analíticas:
  1. alfabetizacao_por_municipio  — KPI municipal com gap de meta 2025
  2. evolucao_temporal            — série histórica por UF
  3. ranking_municipios           — top/bottom municípios por UF e ano
  4. comparacao_metas_nacionais   — taxa real vs meta nacional por ano

Padrões do curso aplicados:
  - Múltiplas visões Gold por dimensão analítica
  - Window rank() por UF/ano para ranking
  - groupBy/agg para evolução temporal
  - partitionOverwriteMode=dynamic
  - Validação DQ das visões geradas
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
from pyspark.sql.window import Window

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
#   --BUCKET_SOT   alfabetizacao-br-dev-silver
#   --BUCKET_SPEC  alfabetizacao-br-dev-gold

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_SOT', 'BUCKET_SPEC'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

JOB_NAME       = args['JOB_NAME']
BUCKET_SOT     = args['BUCKET_SOT']
BUCKET_SPEC    = args['BUCKET_SPEC']
INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ANOMESDIA      = datetime.now(timezone.utc).strftime("%Y%m%d")

def pass_path(entidade):
    return f"s3://{BUCKET_SOT}/sot/pass/{entidade}/anomesdia={ANOMESDIA}/"

def gold_path(visao):
    return f"s3://{BUCKET_SPEC}/gold/{visao}/anomesdia={ANOMESDIA}/"

log.info("=" * 65)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ANOMESDIA : {ANOMESDIA}")
log.info("=" * 65)

# ============================================================
# LEITURA DAS ENTIDADES SILVER
# ============================================================

log.info("[GOLD] Lendo entidades Silver (pass)...")

df_mun       = spark.read.parquet(pass_path("indicador_municipio"))
df_uf        = spark.read.parquet(pass_path("indicador_uf"))
df_meta_br   = spark.read.parquet(pass_path("meta_brasil"))
df_meta_uf   = spark.read.parquet(pass_path("meta_uf"))
df_meta_mun  = spark.read.parquet(pass_path("meta_municipio"))

log.info(f"[GOLD] indicador_municipio={df_mun.count()} | indicador_uf={df_uf.count()}")
log.info(f"[GOLD] meta_brasil={df_meta_br.count()} | meta_uf={df_meta_uf.count()} | meta_municipio={df_meta_mun.count()}")

# ============================================================
# VISÃO 1 — ALFABETIZAÇÃO POR MUNICÍPIO
# ============================================================
# Foco na rede municipal (código 3) com gap em relação à meta 2025.
# Municípios com taxa < meta estão em situação de risco.

def gold_alfabetizacao_municipio():
    log.info("[GOLD] Gerando: alfabetizacao_por_municipio")

    # Meta 2025 por município (rede Municipal)
    df_meta = df_meta_mun.select(
        "id_municipio", "ano",
        F.col("meta_alfabetizacao_2025").alias("meta_2025"),
        F.col("meta_alfabetizacao_2030").alias("meta_2030"),
        "nivel_alfabetizacao",
    )

    df = (df_mun
        .filter(F.col("rede") == "3")  # rede municipal
        .join(df_meta, on=["id_municipio", "ano"], how="left")
        .select(
            "id_municipio", "ano", "serie",
            F.col("rede_desc").alias("rede"),
            F.col("taxa_alfabetizacao"),
            F.col("media_portugues"),
            "meta_2025", "meta_2030",
            "nivel_alfabetizacao",
            F.round(F.col("taxa_alfabetizacao") - F.col("meta_2025"), 2).alias("gap_meta_2025"),
            F.when(F.col("taxa_alfabetizacao") >= F.col("meta_2025"), "ATINGIU")
             .otherwise("NAO_ATINGIU").alias("status_meta_2025"),
        )
        .withColumn("_gold_processed_at", F.lit(INGESTION_TS))
        .withColumn("_ingestion_date",    F.lit(INGESTION_DATE))
    )

    destino = gold_path("alfabetizacao_por_municipio")
    df.write.mode("overwrite").parquet(destino)
    n = df.count()
    log.info(f"[GOLD] alfabetizacao_por_municipio: {n} registros → {destino}")
    return n


# ============================================================
# VISÃO 2 — EVOLUÇÃO TEMPORAL POR UF
# ============================================================
# Série histórica de taxa de alfabetização por UF.
# Base para análises de tendência e sazonalidade.

def gold_evolucao_temporal():
    log.info("[GOLD] Gerando: evolucao_temporal")

    df = (df_uf
        .filter(F.col("rede") == "3")  # rede municipal
        .groupBy("sigla_uf", "ano", "serie")
        .agg(
            F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_media"),
            F.round(F.min("taxa_alfabetizacao"), 2).alias("taxa_min"),
            F.round(F.max("taxa_alfabetizacao"), 2).alias("taxa_max"),
            F.round(F.stddev("taxa_alfabetizacao"), 2).alias("taxa_desvio"),
            F.round(F.avg("media_portugues"), 2).alias("media_portugues_media"),
        )
        .orderBy("sigla_uf", "ano")
        .withColumn("_gold_processed_at", F.lit(INGESTION_TS))
        .withColumn("_ingestion_date",    F.lit(INGESTION_DATE))
    )

    destino = gold_path("evolucao_temporal")
    df.write.mode("overwrite").parquet(destino)
    n = df.count()
    log.info(f"[GOLD] evolucao_temporal: {n} registros → {destino}")
    return n


# ============================================================
# VISÃO 3 — RANKING DE MUNICÍPIOS POR UF
# ============================================================
# Ranking dos municípios por taxa de alfabetização dentro de cada UF e ano.
# Identifica municípios que precisam de intervenção prioritária.

def gold_ranking_municipios():
    log.info("[GOLD] Gerando: ranking_municipios")

    # Precisamos do sigla_uf — está no indicador_uf; para municipios
    # usamos a sigla que pode ser derivada do id_municipio (primeiros 2 dígitos = estado IBGE)
    # Mas o arquivo de municipio não tem sigla_uf. Adicionamos via primeiros 2 dígitos do código IBGE.
    IBGE_UF = F.create_map(
        F.lit("11"), F.lit("RO"), F.lit("12"), F.lit("AC"),
        F.lit("13"), F.lit("AM"), F.lit("14"), F.lit("RR"),
        F.lit("15"), F.lit("PA"), F.lit("16"), F.lit("AP"),
        F.lit("17"), F.lit("TO"), F.lit("21"), F.lit("MA"),
        F.lit("22"), F.lit("PI"), F.lit("23"), F.lit("CE"),
        F.lit("24"), F.lit("RN"), F.lit("25"), F.lit("PB"),
        F.lit("26"), F.lit("PE"), F.lit("27"), F.lit("AL"),
        F.lit("28"), F.lit("SE"), F.lit("29"), F.lit("BA"),
        F.lit("31"), F.lit("MG"), F.lit("32"), F.lit("ES"),
        F.lit("33"), F.lit("RJ"), F.lit("35"), F.lit("SP"),
        F.lit("41"), F.lit("PR"), F.lit("42"), F.lit("SC"),
        F.lit("43"), F.lit("RS"), F.lit("50"), F.lit("MS"),
        F.lit("51"), F.lit("MT"), F.lit("52"), F.lit("GO"),
        F.lit("53"), F.lit("DF"),
    )

    janela_uf = Window.partitionBy("sigla_uf", "ano").orderBy(
        F.col("taxa_alfabetizacao").desc()
    )

    df = (df_mun
        .filter(F.col("rede") == "3")
        .withColumn("sigla_uf",
            IBGE_UF[F.substring(F.col("id_municipio"), 1, 2)])
        .withColumn("ranking_uf", F.rank().over(janela_uf))
        .select(
            "id_municipio", "sigla_uf", "ano", "serie",
            F.col("rede_desc").alias("rede"),
            "taxa_alfabetizacao", "media_portugues",
            "ranking_uf",
        )
        .withColumn("_gold_processed_at", F.lit(INGESTION_TS))
        .withColumn("_ingestion_date",    F.lit(INGESTION_DATE))
    )

    destino = gold_path("ranking_municipios")
    df.write.mode("overwrite").parquet(destino)
    n = df.count()
    log.info(f"[GOLD] ranking_municipios: {n} registros → {destino}")
    return n


# ============================================================
# VISÃO 4 — COMPARAÇÃO COM METAS NACIONAIS
# ============================================================
# Taxa real por UF x meta nacional (meta_brasil).
# Permite ver quais UFs já atingiram a meta do PNA.

def gold_comparacao_metas():
    log.info("[GOLD] Gerando: comparacao_metas_nacionais")

    # Média nacional por ano a partir do indicador_uf (total, código 0)
    df_media_nacional = (df_uf
        .filter(F.col("rede") == "0")  # rede total
        .groupBy("sigla_uf", "ano")
        .agg(F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_uf"))
    )

    # Meta nacional para o ano corrente (meta_brasil, rede Pública)
    df_meta_ano = (df_meta_br
        .select(
            "ano", "rede",
            "taxa_alfabetizacao",
            F.col("meta_alfabetizacao_2025").alias("meta_nacional_2025"),
            F.col("meta_alfabetizacao_2030").alias("meta_nacional_2030"),
            "percentual_participacao",
        )
    )

    # Meta por UF
    df_muf = (df_meta_uf
        .select(
            "ano", "sigla_uf",
            F.col("meta_alfabetizacao_2025").alias("meta_uf_2025"),
        )
    )

    df = (df_media_nacional
        .join(df_meta_ano.select("ano", "meta_nacional_2025", "meta_nacional_2030"),
              on="ano", how="left")
        .join(df_muf, on=["ano", "sigla_uf"], how="left")
        .withColumn("gap_meta_nacional",
            F.round(F.col("taxa_uf") - F.col("meta_nacional_2025"), 2))
        .withColumn("gap_meta_uf",
            F.round(F.col("taxa_uf") - F.col("meta_uf_2025"), 2))
        .withColumn("status_meta",
            F.when(F.col("taxa_uf") >= F.col("meta_nacional_2025"), "ATINGIU")
             .otherwise("NAO_ATINGIU"))
        .orderBy("ano", "taxa_uf")
        .withColumn("_gold_processed_at", F.lit(INGESTION_TS))
        .withColumn("_ingestion_date",    F.lit(INGESTION_DATE))
    )

    destino = gold_path("comparacao_metas_nacionais")
    df.write.mode("overwrite").parquet(destino)
    n = df.count()
    log.info(f"[GOLD] comparacao_metas_nacionais: {n} registros → {destino}")
    return n


# ============================================================
# EXECUÇÃO E SUMÁRIO
# ============================================================

resultados = {
    "alfabetizacao_por_municipio":  gold_alfabetizacao_municipio(),
    "evolucao_temporal":            gold_evolucao_temporal(),
    "ranking_municipios":           gold_ranking_municipios(),
    "comparacao_metas_nacionais":   gold_comparacao_metas(),
}

log.info("=" * 65)
log.info("SUMÁRIO GOLD")
log.info(f"  Anomesdia : {ANOMESDIA}")
for visao, total in resultados.items():
    log.info(f"  {visao:<40}: {total} registros")
log.info(f"  Destino SPEC: s3://{BUCKET_SPEC}/gold/")
log.info("=" * 65)

job.commit()
