"""
Glue Job — MSK Streaming Consumer (Spark Structured Streaming)
Consome o tópico 'alfabetizacao-br-streaming' do MSK e grava em Parquet no S3.

Padrões aplicados:
  - readStream.format("kafka") + PLAINTEXT
  - from_json com StructType explícito
  - writeStream.format("parquet") + checkpointLocation (exactly-once)
  - partitionBy("year","month","day","hour")
  - awaitTermination()

Parâmetros do job (Glue Console → Job parameters):
  --msk_bootstrap_servers  <broker1:9092,broker2:9092>
  --s3_output_path         s3://<bucket-streaming>/streaming/alfabetizacao/
  --checkpoint_path        s3://<bucket-streaming>/checkpoints/alfabetizacao/
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth, hour,
    round as spark_round, when,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'msk_bootstrap_servers',
        's3_output_path',
        'checkpoint_path',
    ])

    spark       = SparkSession.builder.appName("alfabetizacao-streaming").getOrCreate()
    glue_context = GlueContext(spark)
    job          = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    # ----------------------------------------------------------
    # SCHEMA — campos reais do INEP + campos sintéticos do producer
    # ----------------------------------------------------------
    schema = StructType([
        StructField("id_municipio",              StringType(),  True),
        StructField("ano",                       IntegerType(), True),
        StructField("serie",                     IntegerType(), True),
        StructField("rede",                      StringType(),  True),
        StructField("rede_desc",                 StringType(),  True),
        StructField("taxa_alfabetizacao",        DoubleType(),  True),
        StructField("media_portugues",           DoubleType(),  True),
        StructField("proporcao_aluno_nivel_0",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_1",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_2",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_3",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_4",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_5",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_6",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_7",   DoubleType(),  True),
        StructField("proporcao_aluno_nivel_8",   DoubleType(),  True),
        StructField("event_timestamp",           StringType(),  True),
        StructField("source",                    StringType(),  True),
    ])

    # ----------------------------------------------------------
    # READ STREAM — MSK (PLAINTEXT porta 9092)
    # ----------------------------------------------------------
    df_raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args['msk_bootstrap_servers'])
        .option("subscribe",               "alfabetizacao-br-streaming")
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # ----------------------------------------------------------
    # PARSE JSON
    # ----------------------------------------------------------
    df_parsed = df_raw.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schema).alias("data"),
    )

    df_flat = df_parsed.select(
        col("kafka_timestamp"),
        col("data.*"),
    )

    # ----------------------------------------------------------
    # TRANSFORMAÇÕES (enriquecimento mínimo)
    # ----------------------------------------------------------
    df_enriched = (df_flat
        .withColumn("event_ts",          to_timestamp(col("event_timestamp")))
        .withColumn("year",              year(col("event_ts")))
        .withColumn("month",             month(col("event_ts")))
        .withColumn("day",               dayofmonth(col("event_ts")))
        .withColumn("hour",              hour(col("event_ts")))
        .withColumn("taxa_alfabetizacao",spark_round(col("taxa_alfabetizacao"), 2))
        .withColumn("media_portugues",   spark_round(col("media_portugues"), 2))
        # Flag de risco: taxa abaixo de 60% é considerada crítica
        .withColumn("risco_alfabetizacao",
            when(col("taxa_alfabetizacao") < 60.0,  "CRITICO")
            .when(col("taxa_alfabetizacao") < 75.0,  "ATENCAO")
            .otherwise("NORMAL")
        )
    )

    # ----------------------------------------------------------
    # WRITE STREAM — Parquet particionado (exactly-once via checkpoint)
    # ----------------------------------------------------------
    query = (df_enriched.writeStream
        .format("parquet")
        .option("path",                args['s3_output_path'])
        .option("checkpointLocation",  args['checkpoint_path'])
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == '__main__':
    main()
