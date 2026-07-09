# -*- coding: utf-8 -*-
# =============================================================================
# Tech Challenge - Fase 2
# Pipeline Híbrido (Batch + Streaming) para Análise da Alfabetização no Brasil
#
# Arquitetura Medalhão com PySpark:
#   BRONZE -> dados brutos do INEP (e Base dos Dados via BigQuery, opcional)
#   SILVER -> dados limpos, padronizados e integrados
#   GOLD   -> tabelas analíticas (indicador x metas, evolução, agregados)
#
# Para rodar local:   python pipeline_medalhao.py batch
# Para rodar na AWS:  definir LAKE_URI=s3a://meu-bucket/datalake + credenciais
#                     (ver rodar_aws.example.ps1)
#
# Comandos: batch
# =============================================================================

import argparse
import json
import logging
import os
import random
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timezone

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO
# -----------------------------------------------------------------------------

RAIZ = os.path.dirname(os.path.abspath(__file__))

# os workers do Spark precisam usar o mesmo python do script
# (no Windows, sem isso ele tenta abrir o "python3" da Microsoft Store)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# o Docker Desktop cria um hostname estranho (host.docker.internal) que
# impede o worker de conectar de volta no driver, então forçamos localhost
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Spark no Windows precisa do winutils.exe (colocar em C:\Users\<user>\hadoop\bin)
if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
    hadoop_dir = os.path.join(os.path.expanduser("~"), "hadoop")
    if os.path.exists(os.path.join(hadoop_dir, "bin", "winutils.exe")):
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ["PATH"]

# destino do data lake: pasta local por padrão, S3 se LAKE_URI=s3a://bucket/...
LAKE_URI = os.environ.get("LAKE_URI", os.path.join(RAIZ, "datalake").replace("\\", "/"))
if LAKE_URI.startswith("s3://"):
    LAKE_URI = "s3a://" + LAKE_URI[5:]
USA_S3 = LAKE_URI.startswith("s3a://")

# projeto de billing do BigQuery (para ingerir da Base dos Dados) - opcional
GCP_BILLING_PROJECT = os.environ.get("GCP_BILLING_PROJECT")

# pasta local onde ficam os arquivos baixados do INEP
DIR_FONTE = os.path.join(RAIZ, "dados_fonte")

URLS_INEP = {
    "inep_resultados_metas_uf": "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_ufs.xlsx",
    "inep_resultados_metas_municipio": "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_municipios.xlsx",
}

ABAS_INEP = {
    "inep_resultados_metas_uf": "Divulgação Alfabet UF e Brasil",
    "inep_resultados_metas_municipio": "Divulgação Alfabet Municipio",
}

TABELAS_BASEDOSDADOS = ["uf", "municipio", "meta_alfabetizacao_brasil",
                        "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "alunos"]

UFS_VALIDAS = {"AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
               "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
               "SE", "SP", "TO"}

# ponto de corte da escala Saeb definido pela pesquisa Alfabetiza Brasil:
# aluno com proficiência >= 743 é considerado alfabetizado
PONTO_CORTE_SAEB = 743.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("pipeline")

# lista com as métricas de execução (monitoramento simples do pipeline)
METRICAS = []


def caminho(*partes):
    """Monta um caminho dentro do lake (funciona local e no s3a://)."""
    return "/".join([LAKE_URI.rstrip("/")] + [p.strip("/") for p in partes])


# -----------------------------------------------------------------------------
# SPARK
# -----------------------------------------------------------------------------

def criar_spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("pipeline-alfabetizacao")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # o dataset é pequeno (~5,5 mil municípios), então poucas partições
        # de shuffle bastam e evitam gerar um monte de arquivo pequeno (finops)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # arrow deixa a conversão pandas -> spark bem mais rápida
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if USA_S3:
        # conector do S3 na mesma versão do hadoop que vem no pyspark (3.4.2).
        # as credenciais vêm das variáveis de ambiente da AWS
        # (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN)
        builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
        if os.environ.get("AWS_REGION"):
            builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", os.environ["AWS_REGION"])

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -----------------------------------------------------------------------------
# MONITORAMENTO
# -----------------------------------------------------------------------------

def registrar_metrica(etapa, linhas, duracao, status="OK", detalhe=""):
    """Guarda as métricas de cada etapa (e manda pro CloudWatch se habilitado)."""
    METRICAS.append({
        "etapa": etapa,
        "linhas": int(linhas),
        "duracao_s": round(duracao, 2),
        "status": status,
        "detalhe": detalhe,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    log.info("[monitor] %s | linhas=%d | %.1fs | %s", etapa, linhas, duracao, status)

    if os.environ.get("ENABLE_CLOUDWATCH") == "1":
        try:
            import boto3
            boto3.client("cloudwatch").put_metric_data(
                Namespace="PipelineAlfabetizacao",
                MetricData=[
                    {"MetricName": "LinhasProcessadas", "Value": linhas,
                     "Dimensions": [{"Name": "Etapa", "Value": etapa}]},
                    {"MetricName": "Falha", "Value": 0 if status == "OK" else 1,
                     "Dimensions": [{"Name": "Etapa", "Value": etapa}]},
                ])
        except Exception as e:
            log.warning("nao consegui mandar metrica pro cloudwatch: %s", e)


def salvar_metricas(spark):
    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(METRICAS))
    df.coalesce(1).write.mode("append").json(caminho("monitoramento", "execucoes"))
    log.info("[monitor] relatorio de execucao salvo no lake")


# -----------------------------------------------------------------------------
# BRONZE - ingestão dos dados brutos
# -----------------------------------------------------------------------------

def baixar_fontes_inep():
    """Baixa os xlsx do INEP se ainda não existirem na pasta dados_fonte."""
    os.makedirs(DIR_FONTE, exist_ok=True)
    arquivos = {}
    for nome, url in URLS_INEP.items():
        destino = os.path.join(DIR_FONTE, url.rsplit("/", 1)[-1])
        if not os.path.exists(destino):
            log.info("baixando %s", url)
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=120) as r, open(destino, "wb") as f:
                f.write(r.read())
        arquivos[nome] = destino
    return arquivos


def ler_xlsx_inep(spark, arquivo, aba):
    """Lê o excel do INEP. Ele tem 2 linhas de cabeçalho (descrição + nome
    técnico), então uso a segunda linha como nome das colunas e leio tudo
    como texto mesmo, pra guardar o dado bruto igual ao original."""
    import pandas as pd
    nomes = list(pd.read_excel(arquivo, sheet_name=aba, header=None, nrows=2).iloc[1])
    pdf = pd.read_excel(arquivo, sheet_name=aba, header=None, skiprows=2, names=nomes, dtype=str)
    pdf = pdf.where(pdf.notna(), None)
    return spark.createDataFrame(pdf)


def gravar_bronze(df, nome, fonte):
    """Grava na bronze em parquet, com colunas de controle (fonte e data da
    ingestão) e particionado pela data - assim o histórico fica preservado."""
    from pyspark.sql import functions as F
    (df.withColumn("_fonte", F.lit(fonte))
       .withColumn("_ingestao_ts", F.current_timestamp())
       .withColumn("_data_ingestao", F.current_date())
       .write.mode("append")
       .partitionBy("_data_ingestao")
       .parquet(caminho("bronze", nome)))


def executar_bronze(spark):
    log.info("========== CAMADA BRONZE ==========")
    arquivos = baixar_fontes_inep()
    for nome, arq in arquivos.items():
        t0 = time.time()
        df = ler_xlsx_inep(spark, arq, ABAS_INEP[nome])
        gravar_bronze(df, nome, fonte=URLS_INEP[nome])
        registrar_metrica("bronze." + nome, df.count(), time.time() - t0)

    # ingestão via Base dos Dados (BigQuery) - só roda se tiver projeto GCP
    if GCP_BILLING_PROJECT:
        import basedosdados as bd
        for tabela in TABELAS_BASEDOSDADOS:
            t0 = time.time()
            sql = f"SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.{tabela}`"
            if tabela == "alunos":
                # microdados de alunos são grandes, limito pra não estourar
                # a cota gratuita do BigQuery
                sql += " LIMIT 200000"
            pdf = bd.read_sql(sql, billing_project_id=GCP_BILLING_PROJECT)
            df = spark.createDataFrame(pdf.astype(str).where(pdf.notna(), None))
            gravar_bronze(df, "basedosdados_" + tabela, fonte="basedosdados/bigquery")
            registrar_metrica("bronze.basedosdados." + tabela, df.count(), time.time() - t0)
    else:
        log.info("GCP_BILLING_PROJECT nao definido - pulando Base dos Dados "
                 "(os arquivos do INEP ja cobrem resultados e metas)")


# -----------------------------------------------------------------------------
# SILVER - limpeza, padronização e integração
# -----------------------------------------------------------------------------

def limpar_numero(col):
    """Os números do INEP vêm 'sujos': tem '-', '**', '> 80', vírgula decimal...
    Converte pra double e o que não for número vira nulo (try_cast)."""
    from pyspark.sql import functions as F
    limpo = F.trim(col.cast("string"))
    limpo = F.when(limpo.isin("-", "--", "**", ""), None).otherwise(limpo)
    limpo = F.regexp_replace(limpo, r"[>\s%]", "")
    limpo = F.regexp_replace(limpo, ",", ".")
    return limpo.try_cast("double")


def ler_bronze_mais_recente(spark, nome):
    """Pega só a partição da última ingestão da bronze."""
    from pyspark.sql import functions as F
    df = spark.read.parquet(caminho("bronze", nome))
    ultima = df.agg(F.max("_data_ingestao")).collect()[0][0]
    return df.filter(F.col("_data_ingestao") == ultima)


def executar_silver(spark):
    from pyspark.sql import functions as F
    log.info("========== CAMADA SILVER ==========")

    # ---- UF (tem os resultados 2019/2021/2023 + metas, inclui linha Brasil)
    t0 = time.time()
    bruto = ler_bronze_mais_recente(spark, "inep_resultados_metas_uf")
    uf = (
        bruto.select(
            limpar_numero(F.col("ANO")).cast("int").alias("ano"),
            F.col("CD_UF").alias("id_uf"),
            F.upper(F.trim("SIGLA_UF")).alias("sigla_uf"),
            F.trim("NOME_UF").alias("nome_uf"),
            F.upper(F.trim("REDE")).alias("rede"),
            limpar_numero(F.col("SAEB_2019")).alias("taxa_alfabetizacao_2019"),
            limpar_numero(F.col("SAEB_2021")).alias("taxa_alfabetizacao_2021"),
            limpar_numero(F.col("PC_ALUNO_ALFABETIZADO")).alias("taxa_alfabetizacao"),
            *[limpar_numero(F.col(f"META_FINAL_{a}")).alias(f"meta_{a}") for a in range(2024, 2031)],
            limpar_numero(F.col("PC_AVALIADOS_LP")).alias("percentual_participacao"),
        )
        # a linha do Brasil vem sem sigla, então dou a sigla "BR" pra ela
        .withColumn("sigla_uf", F.when(F.col("sigla_uf").isNull() | (F.col("sigla_uf") == ""),
                                       F.lit("BR")).otherwise(F.col("sigla_uf")))
        # tira as linhas de rodapé (observações) que vêm no fim da planilha
        .filter(F.col("ano").isNotNull())
        .dropDuplicates(["ano", "sigla_uf", "rede"])
    )
    uf.write.mode("overwrite").parquet(caminho("silver", "alfabetizacao_uf"))
    registrar_metrica("silver.alfabetizacao_uf", uf.count(), time.time() - t0)

    # ---- Município (resultados + metas + nível de alfabetização)
    t0 = time.time()
    bruto = ler_bronze_mais_recente(spark, "inep_resultados_metas_municipio")
    mun = (
        bruto.select(
            limpar_numero(F.col("ANO")).cast("int").alias("ano"),
            F.col("CO_UF").alias("id_uf"),
            F.upper(F.trim("SG_UF")).alias("sigla_uf"),
            F.lpad(F.trim("CO_MUNICIPIO"), 7, "0").alias("id_municipio"),
            F.trim("NO_MUNICIPIO").alias("nome_municipio"),
            F.upper(F.trim("NO_TP_REDE")).alias("rede"),
            limpar_numero(F.col("PC_ALUNO_ALFABETIZADO")).alias("taxa_alfabetizacao"),
            *[limpar_numero(F.col(f"META_FINAL_{a}")).alias(f"meta_{a}") for a in range(2024, 2031)],
            limpar_numero(F.col("NIVEIS_ALFABETIZACAO_2023")).cast("int").alias("nivel_alfabetizacao"),
            limpar_numero(F.col("PC_AVALIADOS_LP")).alias("percentual_participacao"),
        )
        .filter(F.col("ano").isNotNull())
        .dropDuplicates(["ano", "id_municipio", "rede"])
    )
    mun.write.mode("overwrite").parquet(caminho("silver", "alfabetizacao_municipio"))
    registrar_metrica("silver.alfabetizacao_municipio", mun.count(), time.time() - t0)

    # ---- integração: junta o contexto da UF em cada município
    t0 = time.time()
    uf_contexto = uf.select(
        "ano", "sigla_uf",
        F.col("taxa_alfabetizacao").alias("taxa_alfabetizacao_uf"),
        F.col("meta_2030").alias("meta_2030_uf"),
    )
    integrado = mun.join(uf_contexto, ["ano", "sigla_uf"], "left")
    integrado.write.mode("overwrite").parquet(caminho("silver", "municipio_integrado"))
    registrar_metrica("silver.municipio_integrado", integrado.count(), time.time() - t0)

    return uf, mun


# -----------------------------------------------------------------------------
# QUALIDADE DE DADOS
# -----------------------------------------------------------------------------

def executar_qualidade(spark, uf, mun):
    """Validações pedidas no desafio: duplicidade, valores ausentes,
    chaves válidas e consistência entre as tabelas."""
    from pyspark.sql import functions as F
    import pandas as pd
    log.info("========== QUALIDADE DE DADOS ==========")

    checks = []

    def check(nome, falhas, total, detalhe=""):
        status = "APROVADO" if falhas == 0 else "REPROVADO"
        checks.append({"check": nome, "falhas": int(falhas), "total": int(total),
                       "status": status, "detalhe": detalhe,
                       "ts": datetime.now(timezone.utc).isoformat()})
        log.info("[qualidade] %s -> %s (falhas=%d de %d)", nome, status, falhas, total)

    total_uf = uf.count()
    total_mun = mun.count()

    # 1 - duplicidade das chaves
    check("duplicidade_chave_uf",
          total_uf - uf.dropDuplicates(["ano", "sigla_uf", "rede"]).count(), total_uf)
    check("duplicidade_chave_municipio",
          total_mun - mun.dropDuplicates(["ano", "id_municipio", "rede"]).count(), total_mun)

    # 2 - valores ausentes nas colunas obrigatórias
    check("ausencia_chave_uf",
          uf.filter(F.col("sigla_uf").isNull() | F.col("ano").isNull()).count(), total_uf)
    check("ausencia_chave_municipio",
          mun.filter(F.col("id_municipio").isNull() | F.col("ano").isNull()).count(), total_mun)
    sem_taxa = mun.filter(F.col("taxa_alfabetizacao").isNull()).count()
    check("taxa_alfabetizacao_ausente", 0, total_mun,
          f"{sem_taxa} municipios sem resultado divulgado (participacao < 70%), mantidos como nulo")

    # 3 - validade das chaves
    check("sigla_uf_valida",
          mun.filter(~F.col("sigla_uf").isin(list(UFS_VALIDAS))).count(), total_mun)
    check("id_municipio_7_digitos",
          mun.filter(F.length("id_municipio") != 7).count(), total_mun)

    # 4 - consistência entre as tabelas (todo município tem que ter UF na tabela de UF)
    orfaos = (mun.select("ano", "sigla_uf").distinct()
              .join(uf.filter(F.col("sigla_uf") != "BR").select("ano", "sigla_uf").distinct(),
                    ["ano", "sigla_uf"], "left_anti").count())
    check("integridade_municipio_uf", orfaos, total_mun)

    # 5 - percentual tem que estar entre 0 e 100
    check("taxa_entre_0_e_100",
          mun.filter((F.col("taxa_alfabetizacao") < 0) | (F.col("taxa_alfabetizacao") > 100)).count(),
          total_mun)

    # salva o relatório de qualidade no lake
    spark.createDataFrame(pd.DataFrame(checks)).coalesce(1) \
        .write.mode("append").json(caminho("qualidade", "relatorios"))

    reprovados = [c["check"] for c in checks if c["status"] == "REPROVADO"]
    if reprovados:
        log.warning("checks reprovados: %s", reprovados)
        if os.environ.get("FALHAR_EM_QUALIDADE") == "1":
            raise RuntimeError(f"qualidade reprovou: {reprovados}")
    registrar_metrica("qualidade.checks", len(checks), 0,
                      "OK" if not reprovados else "ALERTA", ";".join(reprovados))


# -----------------------------------------------------------------------------
# GOLD - tabelas analíticas
# -----------------------------------------------------------------------------

def executar_gold(spark):
    from pyspark.sql import functions as F
    log.info("========== CAMADA GOLD ==========")

    uf = spark.read.parquet(caminho("silver", "alfabetizacao_uf"))
    integrado = spark.read.parquet(caminho("silver", "municipio_integrado"))

    # ---- 1) indicador por município comparado com as metas
    t0 = time.time()
    gold_mun = (
        integrado
        .withColumn("gap_meta_2024", F.round(F.col("meta_2024") - F.col("taxa_alfabetizacao"), 2))
        .withColumn("gap_meta_2030", F.round(F.col("meta_2030") - F.col("taxa_alfabetizacao"), 2))
        .withColumn("atingiu_meta_2024",
                    F.when(F.col("taxa_alfabetizacao").isNull() | F.col("meta_2024").isNull(), None)
                     .otherwise(F.col("taxa_alfabetizacao") >= F.col("meta_2024")))
        .withColumn("diferenca_para_uf",
                    F.round(F.col("taxa_alfabetizacao") - F.col("taxa_alfabetizacao_uf"), 2))
        .select("ano", "sigla_uf", "id_municipio", "nome_municipio", "rede",
                "taxa_alfabetizacao", "nivel_alfabetizacao", "percentual_participacao",
                "meta_2024", "meta_2030", "gap_meta_2024", "gap_meta_2030",
                "atingiu_meta_2024", "taxa_alfabetizacao_uf", "diferenca_para_uf")
    )
    # particiono por UF porque a consulta mais comum é filtrar por estado
    gold_mun.write.mode("overwrite").partitionBy("sigla_uf") \
        .parquet(caminho("gold", "indicador_alfabetizacao_municipio"))
    registrar_metrica("gold.indicador_municipio", gold_mun.count(), time.time() - t0)

    # ---- 2) evolução do indicador por UF (Saeb 2019 -> 2021 -> 2023)
    t0 = time.time()
    evolucao = (
        uf.select("sigla_uf", "nome_uf", "rede",
                  F.col("taxa_alfabetizacao_2019").alias("taxa_2019"),
                  F.col("taxa_alfabetizacao_2021").alias("taxa_2021"),
                  F.col("taxa_alfabetizacao").alias("taxa_2023"),
                  "meta_2024", "meta_2030", "percentual_participacao")
        .withColumn("variacao_2019_2023", F.round(F.col("taxa_2023") - F.col("taxa_2019"), 2))
        .withColumn("recuperacao_pos_pandemia", F.round(F.col("taxa_2023") - F.col("taxa_2021"), 2))
        .withColumn("gap_meta_2030", F.round(F.col("meta_2030") - F.col("taxa_2023"), 2))
    )
    evolucao.write.mode("overwrite").parquet(caminho("gold", "evolucao_indicador_uf"))
    registrar_metrica("gold.evolucao_uf", evolucao.count(), time.time() - t0)

    # ---- 3) resumo metas x resultados agregado por UF
    t0 = time.time()
    resumo = (
        spark.read.parquet(caminho("gold", "indicador_alfabetizacao_municipio"))
        .groupBy("ano", "sigla_uf")
        .agg(F.count("*").alias("qtd_municipios"),
             F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_media_municipios"),
             F.round(F.expr("percentile_approx(taxa_alfabetizacao, 0.5)"), 2).alias("taxa_mediana"),
             F.min("taxa_alfabetizacao").alias("taxa_minima"),
             F.max("taxa_alfabetizacao").alias("taxa_maxima"),
             F.sum(F.when(F.col("atingiu_meta_2024"), 1).otherwise(0)).alias("municipios_atingiram_meta_2024"),
             F.round(F.avg("gap_meta_2030"), 2).alias("gap_medio_meta_2030"))
        .withColumn("pct_municipios_na_meta",
                    F.round(100 * F.col("municipios_atingiram_meta_2024") / F.col("qtd_municipios"), 2))
        .orderBy(F.desc("taxa_media_municipios"))
    )
    resumo.write.mode("overwrite").parquet(caminho("gold", "metas_x_resultados_uf"))
    registrar_metrica("gold.metas_x_resultados_uf", resumo.count(), time.time() - t0)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def rodar_batch(spark):
    executar_bronze(spark)
    uf, mun = executar_silver(spark)
    executar_qualidade(spark, uf, mun)
    executar_gold(spark)
    salvar_metricas(spark)

    log.info("========== AMOSTRA DA GOLD (metas x resultados por UF) ==========")
    spark.read.parquet(caminho("gold", "metas_x_resultados_uf")).show(30, truncate=False)


def main():
    parser = argparse.ArgumentParser(description="Pipeline Medalhao - Alfabetizacao (Tech Challenge Fase 2)")
    sub = parser.add_subparsers(dest="comando", required=True)

    sub.add_parser("batch", help="roda o pipeline batch (bronze -> silver -> gold)")

    args = parser.parse_args()
    log.info("data lake: %s (%s)", LAKE_URI, "AWS S3" if USA_S3 else "local")

    spark = criar_spark()
    try:
        if args.comando == "batch":
            rodar_batch(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
