"""
Glue Job — Ingestão Bronze (SOR)
Lê os 5 CSVs do INEP/Base dos Dados do S3, aplica schema explícito,
calcula _record_hash MD5, executa checagem de qualidade e grava
Parquet particionado por ANO (Hive-style: bronze/<entidade>/ano=YYYY/).

Entidades:
  indicador_municipio  — taxa de alfabetização por município/ano/série/rede
  indicador_uf         — taxa de alfabetização por UF/ano/série/rede
  meta_brasil          — metas nacionais de alfabetização (2024-2030)
  meta_uf              — metas estaduais de alfabetização
  meta_municipio       — metas municipais de alfabetização

Estratégia de particionamento:
  A base do indicador cresce um ano a cada ciclo de avaliação (2023, 2024,
  2025, ...). Particionar por `ano` dentro de cada entidade permite que
  Athena/Spark leiam apenas as partições necessárias (partition pruning),
  reduzindo bytes escaneados e custo por query (FinOps). O modo
  partitionOverwriteMode=dynamic garante que reprocessar um ano NÃO apaga
  os anos anteriores — o histórico completo é preservado, requisito para
  qualquer análise comparativa de indicador.

Padrões do curso aplicados:
  - Schema explícito com StructType (evita inferência incorreta)
  - _record_hash MD5 por chave composta (padrão etl-bronze.py)
  - Metadados com prefixo _ (_ingestion_timestamp, _source_entity, etc.)
  - CHECKS dict com PASS / FAIL / WARN e score %
  - Partição Hive-style ano=YYYY + partitionOverwriteMode=dynamic
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
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

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
#   --BUCKET_RAW   s3://alfabetizacao-br-dev-scripts/raw/
#   --BUCKET_SOR   alfabetizacao-br-dev-bronze

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_RAW', 'BUCKET_SOR'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

JOB_NAME       = args['JOB_NAME']
BUCKET_RAW     = args['BUCKET_RAW'].rstrip('/')
BUCKET_SOR     = args['BUCKET_SOR']
INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ANOMESDIA      = datetime.now(timezone.utc).strftime("%Y%m%d")

log.info("=" * 65)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ANOMESDIA : {ANOMESDIA}")
log.info(f"RAW       : {BUCKET_RAW}")
log.info(f"SOR       : s3://{BUCKET_SOR}/bronze/")
log.info("=" * 65)

# ============================================================
# SCHEMAS — Schema explícito por entidade
# ============================================================

SCHEMAS = {
    "indicador_municipio": StructType([
        StructField("ano",                        IntegerType(), True),
        StructField("id_municipio",               StringType(),  True),
        StructField("serie",                      IntegerType(), True),
        StructField("rede",                       StringType(),  True),
        StructField("taxa_alfabetizacao",         DoubleType(),  True),
        StructField("media_portugues",            DoubleType(),  True),
        StructField("proporcao_aluno_nivel_0",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_1",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_2",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_3",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_4",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_5",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_6",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_7",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_8",    DoubleType(),  True),
    ]),
    "indicador_uf": StructType([
        StructField("ano",                        IntegerType(), True),
        StructField("sigla_uf",                   StringType(),  True),
        StructField("serie",                      IntegerType(), True),
        StructField("rede",                       StringType(),  True),
        StructField("taxa_alfabetizacao",         DoubleType(),  True),
        StructField("media_portugues",            DoubleType(),  True),
        StructField("proporcao_aluno_nivel_0",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_1",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_2",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_3",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_4",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_5",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_6",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_7",    DoubleType(),  True),
        StructField("proporcao_aluno_nivel_8",    DoubleType(),  True),
    ]),
    "meta_brasil": StructType([
        StructField("ano",                        IntegerType(), True),
        StructField("rede",                       StringType(),  True),
        StructField("taxa_alfabetizacao",         DoubleType(),  True),
        StructField("meta_alfabetizacao_2024",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2025",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2026",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2027",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2028",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2029",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2030",    DoubleType(),  True),
        StructField("percentual_participacao",    DoubleType(),  True),
    ]),
    "meta_uf": StructType([
        StructField("ano",                        IntegerType(), True),
        StructField("sigla_uf",                   StringType(),  True),
        StructField("rede",                       StringType(),  True),
        StructField("taxa_alfabetizacao",         DoubleType(),  True),
        StructField("meta_alfabetizacao_2024",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2025",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2026",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2027",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2028",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2029",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2030",    DoubleType(),  True),
        StructField("percentual_participacao",    DoubleType(),  True),
    ]),
    "meta_municipio": StructType([
        StructField("ano",                        IntegerType(), True),
        StructField("id_municipio",               StringType(),  True),
        StructField("rede",                       StringType(),  True),
        StructField("taxa_alfabetizacao",         DoubleType(),  True),
        StructField("meta_alfabetizacao_2024",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2025",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2026",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2027",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2028",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2029",    DoubleType(),  True),
        StructField("meta_alfabetizacao_2030",    DoubleType(),  True),
        StructField("nivel_alfabetizacao",        StringType(),  True),
        StructField("percentual_participacao",    DoubleType(),  True),
    ]),
}

# Mapeamento: entidade → arquivo CSV no S3
ARQUIVOS = {
    "indicador_municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "indicador_uf":        "br_inep_avaliacao_alfabetizacao_uf.csv",
    "meta_brasil":         "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    "meta_uf":             "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "meta_municipio":      "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
}

# Colunas que compõem o hash único por registro
HASH_KEYS = {
    "indicador_municipio": ["ano", "id_municipio", "serie", "rede"],
    "indicador_uf":        ["ano", "sigla_uf", "serie", "rede"],
    "meta_brasil":         ["ano", "rede"],
    "meta_uf":             ["ano", "sigla_uf", "rede"],
    "meta_municipio":      ["ano", "id_municipio", "rede"],
}

# ============================================================
# CHECKS DE QUALIDADE
# ============================================================

CHECKS = {
    "indicador_municipio": [
        {"col": "id_municipio",       "tipo": "not_null",  "nivel": "FAIL"},
        {"col": "ano",                "tipo": "not_null",  "nivel": "FAIL"},
        {"col": "taxa_alfabetizacao", "tipo": "not_null",  "nivel": "FAIL"},
        {"col": "taxa_alfabetizacao", "tipo": "range", "min": 0, "max": 100, "nivel": "FAIL"},
        {"col": "rede",               "tipo": "in_set",    "valores": {"0","2","3","5"}, "nivel": "WARN"},
    ],
    "indicador_uf": [
        {"col": "sigla_uf",           "tipo": "not_null",  "nivel": "FAIL"},
        {"col": "ano",                "tipo": "not_null",  "nivel": "FAIL"},
        {"col": "taxa_alfabetizacao", "tipo": "not_null",  "nivel": "WARN"},
        {"col": "taxa_alfabetizacao", "tipo": "range", "min": 0, "max": 100, "nivel": "WARN"},
    ],
    "meta_brasil": [
        {"col": "ano",                       "tipo": "not_null", "nivel": "FAIL"},
        {"col": "meta_alfabetizacao_2030",   "tipo": "not_null", "nivel": "WARN"},
    ],
    "meta_uf": [
        {"col": "sigla_uf", "tipo": "not_null", "nivel": "FAIL"},
        {"col": "ano",      "tipo": "not_null", "nivel": "FAIL"},
    ],
    "meta_municipio": [
        {"col": "id_municipio", "tipo": "not_null", "nivel": "FAIL"},
        {"col": "ano",          "tipo": "not_null", "nivel": "FAIL"},
    ],
}

# ============================================================
# FUNÇÕES
# ============================================================

def construir_bronze(df, entidade):
    """Adiciona colunas de metadados e _record_hash."""
    chaves = HASH_KEYS[entidade]
    return (df
        .withColumn("_record_hash",
            F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in chaves])))
        .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS))
        .withColumn("_ingestion_date",      F.lit(INGESTION_DATE))
        .withColumn("_source_entity",       F.lit(entidade))
        .withColumn("_source_file",         F.lit(ARQUIVOS[entidade]))
        .withColumn("_job_name",            F.lit(JOB_NAME))
    )


def checar_qualidade(df, entidade):
    """Executa checks de qualidade e loga resultado."""
    checks = CHECKS.get(entidade, [])
    total = len(checks)
    passou = criticos = 0

    for ck in checks:
        col   = ck["col"]
        tipo  = ck["tipo"]
        nivel = ck.get("nivel", "WARN")

        try:
            if tipo == "not_null":
                n = df.filter(F.col(col).isNull()).count()
                ok = n == 0
                detalhe = f"{n} nulos"
            elif tipo == "range":
                mn, mx = ck["min"], ck["max"]
                fora = df.filter(F.col(col).isNotNull() & ((F.col(col) < mn) | (F.col(col) > mx))).count()
                ok = fora == 0
                detalhe = f"{fora} fora de [{mn},{mx}]"
            elif tipo == "in_set":
                inv = df.filter(~F.col(col).isin(ck["valores"])).count()
                ok = inv == 0
                detalhe = f"{inv} fora do conjunto {ck['valores']}"
            else:
                ok, detalhe = False, "tipo desconhecido"
        except Exception as e:
            ok, detalhe = False, str(e)

        status = "PASS" if ok else nivel
        msg = f"[DQ:BRONZE] {entidade} | {status} | {tipo} | col={col} | {detalhe}"
        if ok:
            passou += 1
            log.info(msg)
        elif nivel == "FAIL":
            criticos += 1
            log.error(msg)
        else:
            log.warning(msg)

    score = round(passou / total * 100, 1) if total else 100
    log.info(f"[DQ:BRONZE] {entidade} | score={score}% | criticos={criticos}")

    if criticos > 0:
        raise Exception(f"[DQ:BRONZE] {criticos} check(s) crítico(s) em '{entidade}'")


# ============================================================
# PROCESSAMENTO DE CADA ENTIDADE
# ============================================================

resultados = {}

for ENTIDADE, ARQUIVO in ARQUIVOS.items():
    log.info(f"[BRONZE] Iniciando: {ENTIDADE}")

    caminho = f"{BUCKET_RAW}/{ARQUIVO}"
    log.info(f"[BRONZE] Lendo: {caminho}")

    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .schema(SCHEMAS[ENTIDADE]) \
        .csv(caminho)

    total_lido = df.count()
    log.info(f"[BRONZE] {ENTIDADE}: {total_lido} registros lidos")

    df_bronze = construir_bronze(df, ENTIDADE)
    checar_qualidade(df_bronze, ENTIDADE)

    # Escrita particionada por ano (bronze/<entidade>/ano=YYYY/).
    # Com partitionOverwriteMode=dynamic, apenas as partições presentes
    # neste lote são sobrescritas — execução idempotente e histórico preservado.
    destino = f"s3://{BUCKET_SOR}/bronze/{ENTIDADE}/"
    df_bronze.write.mode("overwrite").partitionBy("ano").parquet(destino)
    log.info(f"[BRONZE] {ENTIDADE}: gravado em {destino} (particionado por ano)")

    resultados[ENTIDADE] = total_lido

# ============================================================
# SUMÁRIO
# ============================================================

log.info("=" * 65)
log.info("SUMÁRIO BRONZE")
log.info(f"  Anomesdia : {ANOMESDIA}")
for entidade, total in resultados.items():
    log.info(f"  {entidade:<30}: {total:>6} registros")
log.info(f"  Destino SOR: s3://{BUCKET_SOR}/bronze/")
log.info("=" * 65)

job.commit()
