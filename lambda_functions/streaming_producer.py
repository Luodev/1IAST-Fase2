"""
Lambda — Streaming Producer
Gera eventos de alfabetização sintéticos e publica no tópico MSK (Kafka).

Padrões aplicados:
  - confluent-kafka: AdminClient.ensure_topic + delivery_report
  - PLAINTEXT (porta 9092, sem SSL/SASL)
  - Variáveis via env vars (MSK_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
  - Payload com campos reais do INEP: id_municipio, taxa_alfabetizacao, etc.
"""

import json
import os
import random
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ============================================================
# CONFIGURAÇÃO
# ============================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ["MSK_BOOTSTRAP_SERVERS"]   # host1:9092,host2:9092
TOPIC             = os.environ.get("KAFKA_TOPIC", "alfabetizacao-br-streaming")
MENSAGENS_POR_INVOCACAO = int(os.environ.get("MENSAGENS_POR_INVOCACAO", "10"))

# IDs de municípios reais extraídos do dataset INEP (rede=3, 2024)
MUNICIPIOS_REAIS = [
    "4309126", "3171030", "3532868", "1704600", "2406155",
    "2406205", "1710706", "4301750", "1712702", "2414456",
    "4318440", "2902252", "4321634", "1200328", "4319208",
    "3304904", "1501402", "2111300", "4106902", "5208707",
]

REDES = [("2", "estadual"), ("3", "municipal"), ("5", "privada")]

ANOS_VALIDOS  = [2023, 2024]
SERIES        = [2, 5]   # 2º ano EF e 5º ano EF


# ============================================================
# UTILITÁRIOS
# ============================================================

def _conf_kafka():
    return {"bootstrap.servers": BOOTSTRAP_SERVERS}


def ensure_topic(admin: AdminClient, topic: str, num_partitions=1, replication_factor=2):
    """Cria o tópico se ainda não existir (padrão lab FIAP)."""
    meta = admin.list_topics(timeout=10)
    if topic in meta.topics:
        log.info(f"Tópico '{topic}' já existe.")
        return
    new_topic = NewTopic(topic, num_partitions=num_partitions,
                         replication_factor=replication_factor)
    fs = admin.create_topics([new_topic])
    for t, f in fs.items():
        try:
            f.result()
            log.info(f"Tópico '{t}' criado.")
        except Exception as e:
            log.warning(f"Tópico '{t}': {e}")


def delivery_report(err, msg):
    """Callback de entrega"""
    if err is not None:
        log.error(f"Falha ao entregar mensagem: {err}")
    else:
        log.info(f"Mensagem entregue: {msg.topic()} [{msg.partition()}] offset={msg.offset()}")


def gerar_evento():
    """Gera um evento sintético com campos compatíveis com o schema INEP."""
    id_municipio           = random.choice(MUNICIPIOS_REAIS)
    ano                    = random.choice(ANOS_VALIDOS)
    serie                  = random.choice(SERIES)
    rede_codigo, rede_nome = random.choice(REDES)

    # Taxa de alfabetização com distribuição realista (observada: 2.1 a 100.0)
    # A maior parte dos municípios está entre 60 e 95
    taxa_base = random.gauss(80.0, 12.0)
    taxa_alfabetizacao = round(min(max(taxa_base, 10.0), 100.0), 1)

    # Média de proficiência em Português (correlacionada com taxa)
    media_portugues = round(taxa_alfabetizacao * random.uniform(1.8, 2.4), 1)

    # Proporções por nível (0-8) — soma deve ser ~100
    niveis = [random.uniform(0, 20) for _ in range(9)]
    total  = sum(niveis)
    niveis = [round(v / total * 100, 1) for v in niveis]

    return {
        "id_municipio":          id_municipio,
        "ano":                   ano,
        "serie":                 serie,
        "rede":                  rede_codigo,
        "rede_desc":             rede_nome,
        "taxa_alfabetizacao":    taxa_alfabetizacao,
        "media_portugues":       media_portugues,
        "proporcao_aluno_nivel_0": niveis[0],
        "proporcao_aluno_nivel_1": niveis[1],
        "proporcao_aluno_nivel_2": niveis[2],
        "proporcao_aluno_nivel_3": niveis[3],
        "proporcao_aluno_nivel_4": niveis[4],
        "proporcao_aluno_nivel_5": niveis[5],
        "proporcao_aluno_nivel_6": niveis[6],
        "proporcao_aluno_nivel_7": niveis[7],
        "proporcao_aluno_nivel_8": niveis[8],
        "event_timestamp":       datetime.now(timezone.utc).isoformat(),
        "source":                "synthetic_streaming",
    }


# ============================================================
# HANDLER LAMBDA
# ============================================================

def lambda_handler(event, context):
    log.info(f"Bootstrap: {BOOTSTRAP_SERVERS} | Topic: {TOPIC} | N={MENSAGENS_POR_INVOCACAO}")

    admin = AdminClient(_conf_kafka())
    ensure_topic(admin, TOPIC)

    producer = Producer(_conf_kafka())
    publicadas = 0

    for _ in range(MENSAGENS_POR_INVOCACAO):
        payload = gerar_evento()
        producer.produce(
            topic     = TOPIC,
            value     = json.dumps(payload),
            key       = payload["id_municipio"],
            callback  = delivery_report,
        )
        publicadas += 1
        producer.poll(0)

    producer.flush()
    log.info(f"Publicadas {publicadas} mensagens no tópico '{TOPIC}'.")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensagens_publicadas": publicadas,
            "topic":                TOPIC,
        }),
    }
