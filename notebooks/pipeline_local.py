"""
pipeline_local.py — Simulação local (pandas) da pipeline batch Medalhão.

Reproduz fielmente, em escala local, a lógica dos três Glue Jobs do projeto
(glue_jobs/etl_bronze.py, etl_silver.py e etl_gold.py), incluindo o
particionamento Hive-style por ano (camada/<entidade|visao>/ano=YYYY/).

Finalidade: permitir que os notebooks de verificação de idempotência
executem cada camada N vezes num "data lake" local e comparem o estado
final resultante, sem depender de recursos AWS provisionados. A semântica
de escrita replica o partitionOverwriteMode=dynamic do Spark: apenas as
partições presentes no lote são sobrescritas; as demais são preservadas.

Uso típico (ver notebooks 02–04):
    from pipeline_local import run_bronze, run_silver, run_gold, snapshot_camada
    run_bronze(dados_dir, lake_dir)
    snap_1 = snapshot_camada(lake_dir, "bronze")
"""

import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# =============================================================================
# CONFIGURAÇÃO — espelha SCHEMAS / ARQUIVOS / HASH_KEYS do etl_bronze.py
# =============================================================================

ARQUIVOS = {
    "indicador_municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "indicador_uf":        "br_inep_avaliacao_alfabetizacao_uf.csv",
    "meta_brasil":         "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    "meta_uf":             "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "meta_municipio":      "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
}

# Colunas que compõem o hash único por registro (deduplicação no Bronze)
HASH_KEYS = {
    "indicador_municipio": ["ano", "id_municipio", "serie", "rede"],
    "indicador_uf":        ["ano", "sigla_uf", "serie", "rede"],
    "meta_brasil":         ["ano", "rede"],
    "meta_uf":             ["ano", "sigla_uf", "rede"],
    "meta_municipio":      ["ano", "id_municipio", "rede"],
}

# Tipagem explícita na leitura (equivalente ao StructType do Spark) —
# chaves como string para preservar formato (ex.: código IBGE de 7 dígitos)
DTYPES_STR = ["id_municipio", "sigla_uf", "rede", "nivel_alfabetizacao"]

# Decode dos códigos de rede INEP: 0=total, 2=estadual, 3=municipal, 5=privada
REDE_MAP = {"0": "total", "2": "estadual", "3": "municipal", "5": "privada"}

# Primeiros 2 dígitos do código IBGE do município → sigla da UF
IBGE_UF = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP",
    "17": "TO", "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB",
    "26": "PE", "27": "AL", "28": "SE", "29": "BA", "31": "MG", "32": "ES",
    "33": "RJ", "35": "SP", "41": "PR", "42": "SC", "43": "RS", "50": "MS",
    "51": "MT", "52": "GO", "53": "DF",
}

# Colunas de metadados voláteis (timestamps de execução): são excluídas da
# comparação de idempotência porque mudam legitimamente a cada execução —
# idempotência é definida sobre o CONTEÚDO DE NEGÓCIO do lake.
COLUNAS_VOLATEIS = [
    "_ingestion_timestamp", "_ingestion_date", "_job_name",
    "_silver_processed_at", "_quarentena_ts", "_gold_processed_at",
]


# =============================================================================
# ESCRITA PARTICIONADA — replica o partitionOverwriteMode=dynamic do Spark
# =============================================================================

def _escrever_particionado_por_ano(df: pd.DataFrame, destino: Path) -> None:
    """Grava o DataFrame em destino/ano=YYYY/dados.parquet, sobrescrevendo
    somente as partições de ano presentes no lote (overwrite dinâmico)."""
    for ano, df_ano in df.groupby("ano"):
        part_dir = destino / f"ano={int(ano)}"
        if part_dir.exists():
            shutil.rmtree(part_dir)          # overwrite apenas desta partição
        part_dir.mkdir(parents=True, exist_ok=True)
        df_ano.drop(columns=["ano"]).to_parquet(part_dir / "dados.parquet", index=False)


def _ler_particionado_por_ano(origem: Path) -> pd.DataFrame:
    """Lê todas as partições ano=YYYY de uma entidade, restaurando a coluna
    `ano` a partir do nome do diretório (leitura Hive-style)."""
    partes = []
    for part_dir in sorted(origem.glob("ano=*")):
        df = pd.read_parquet(part_dir / "dados.parquet")
        df["ano"] = int(part_dir.name.split("=")[1])
        partes.append(df)
    return pd.concat(partes, ignore_index=True)


# =============================================================================
# CAMADA BRONZE — espelha etl_bronze.py
# =============================================================================

def run_bronze(dados_dir, lake_dir) -> dict:
    """RAW (CSV) → Bronze (Parquet particionado por ano).

    Para cada entidade: leitura com tipagem explícita, _record_hash MD5 da
    chave composta, metadados de linhagem e escrita em bronze/<ent>/ano=YYYY/.
    Retorna dict entidade → nº de registros gravados.
    """
    dados_dir, lake_dir = Path(dados_dir), Path(lake_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    resultados = {}

    for entidade, arquivo in ARQUIVOS.items():
        df = pd.read_csv(
            dados_dir / arquivo,
            dtype={c: "string" for c in DTYPES_STR},
        )

        # Hash MD5 da chave composta — chave de deduplicação (padrão do curso)
        chaves = HASH_KEYS[entidade]
        df["_record_hash"] = (
            df[chaves].astype("string").fillna("")
            .agg("|".join, axis=1)
            .map(lambda s: hashlib.md5(s.encode()).hexdigest())
        )

        # Metadados de linhagem (prefixo _, como no job Glue)
        df["_ingestion_timestamp"] = ts
        df["_source_entity"] = entidade
        df["_source_file"] = arquivo

        _escrever_particionado_por_ano(df, lake_dir / "bronze" / entidade)
        resultados[entidade] = len(df)

    return resultados


# =============================================================================
# CAMADA SILVER — espelha etl_silver.py (DQ + pass/quarentena)
# =============================================================================

def _dq_silver(df: pd.DataFrame, entidade: str) -> pd.DataFrame:
    """Aplica as regras de qualidade como colunas booleanas _dq_* e consolida
    em _dq_passou — mesma lógica do DQ_FUNCTIONS do etl_silver.py."""
    ano_ok = df["ano"].notna() & df["ano"].between(2020, 2030)

    if entidade == "indicador_municipio":
        df["_dq_id_municipio_valido"] = df["id_municipio"].notna() & (df["id_municipio"].str.len() == 7)
        df["_dq_taxa_valida"] = df["taxa_alfabetizacao"].notna() & df["taxa_alfabetizacao"].between(0, 100)
        df["_dq_ano_valido"] = ano_ok
        df["_dq_passou"] = df["_dq_id_municipio_valido"] & df["_dq_taxa_valida"] & df["_dq_ano_valido"]
    elif entidade == "indicador_uf":
        df["_dq_uf_valida"] = df["sigla_uf"].notna() & (df["sigla_uf"].str.len() == 2)
        df["_dq_taxa_valida"] = df["taxa_alfabetizacao"].notna() & df["taxa_alfabetizacao"].between(0, 100)
        df["_dq_ano_valido"] = ano_ok
        df["_dq_passou"] = df["_dq_uf_valida"] & df["_dq_ano_valido"]
    else:
        chave = {"meta_brasil": "ano", "meta_uf": "sigla_uf", "meta_municipio": "id_municipio"}[entidade]
        df["_dq_chave_valida"] = df[chave].notna()
        df["_dq_ano_valido"] = ano_ok
        df["_dq_passou"] = df["_dq_chave_valida"] & df["_dq_ano_valido"]
    return df


def run_silver(lake_dir) -> dict:
    """Bronze → Silver: transformações, DQ e roteamento pass/quarentena.

    PASS é particionado por ano (sot/pass/<ent>/ano=YYYY/); QUARENTENA é
    particionada pela data de processamento (trilha de auditoria — o `ano`
    do registro reprovado pode ser exatamente o campo inválido).
    Retorna dict entidade → (n_pass, n_quarentena).
    """
    lake_dir = Path(lake_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    anomesdia = datetime.now(timezone.utc).strftime("%Y%m%d")
    resultados = {}

    for entidade in ARQUIVOS:
        df = _ler_particionado_por_ano(lake_dir / "bronze" / entidade)

        # Transformações (espelham TRANSFORMACOES do etl_silver.py)
        if entidade in ("indicador_municipio", "indicador_uf"):
            df["rede_desc"] = df["rede"].map(REDE_MAP).fillna(df["rede"])
            df["taxa_alfabetizacao"] = df["taxa_alfabetizacao"].round(2)
            df["media_portugues"] = df["media_portugues"].round(2)
        else:
            meta_cols = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]
            df[meta_cols] = df[meta_cols].round(2)
        df["_silver_processed_at"] = ts

        # DQ + roteamento pass/quarentena
        df = _dq_silver(df, entidade)
        df_pass = df[df["_dq_passou"]].copy()
        df_quar = df[~df["_dq_passou"]].copy()

        _escrever_particionado_por_ano(df_pass, lake_dir / "sot" / "pass" / entidade)

        if len(df_quar) > 0:
            df_quar["_quarentena_ts"] = ts
            quar_dir = lake_dir / "sot" / "quarentena" / entidade / f"anomesdia={anomesdia}"
            if quar_dir.exists():
                shutil.rmtree(quar_dir)
            quar_dir.mkdir(parents=True, exist_ok=True)
            df_quar.to_parquet(quar_dir / "dados.parquet", index=False)

        resultados[entidade] = (len(df_pass), len(df_quar))

    return resultados


# =============================================================================
# CAMADA GOLD — espelha etl_gold.py (4 visões analíticas)
# =============================================================================

def run_gold(lake_dir) -> dict:
    """Silver (pass) → Gold: gera as 4 visões analíticas particionadas por ano.
    Retorna dict visão → nº de registros gravados."""
    lake_dir = Path(lake_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    ler = lambda ent: _ler_particionado_por_ano(lake_dir / "sot" / "pass" / ent)
    df_mun, df_uf = ler("indicador_municipio"), ler("indicador_uf")
    df_meta_br, df_meta_uf, df_meta_mun = ler("meta_brasil"), ler("meta_uf"), ler("meta_municipio")

    resultados = {}

    def salvar(df, visao):
        df["_gold_processed_at"] = ts
        _escrever_particionado_por_ano(df, lake_dir / "gold" / visao)
        resultados[visao] = len(df)

    # --- Visão 1: alfabetização por município (rede municipal × meta 2025) ---
    meta = df_meta_mun[["id_municipio", "ano", "meta_alfabetizacao_2025",
                        "meta_alfabetizacao_2030", "nivel_alfabetizacao"]].rename(
        columns={"meta_alfabetizacao_2025": "meta_2025", "meta_alfabetizacao_2030": "meta_2030"})
    v1 = (df_mun[df_mun["rede"] == "3"]
          .merge(meta, on=["id_municipio", "ano"], how="left"))
    v1 = v1[["id_municipio", "ano", "serie", "rede_desc", "taxa_alfabetizacao",
             "media_portugues", "meta_2025", "meta_2030", "nivel_alfabetizacao"]].rename(
        columns={"rede_desc": "rede"})
    v1["gap_meta_2025"] = (v1["taxa_alfabetizacao"] - v1["meta_2025"]).round(2)
    v1["status_meta_2025"] = np.where(
        v1["taxa_alfabetizacao"] >= v1["meta_2025"], "ATINGIU", "NAO_ATINGIU")
    salvar(v1, "alfabetizacao_por_municipio")

    # --- Visão 2: evolução temporal por UF (agregações estatísticas) ---
    v2 = (df_uf[df_uf["rede"] == "3"]
          .groupby(["sigla_uf", "ano", "serie"], as_index=False)
          .agg(taxa_media=("taxa_alfabetizacao", "mean"),
               taxa_min=("taxa_alfabetizacao", "min"),
               taxa_max=("taxa_alfabetizacao", "max"),
               taxa_desvio=("taxa_alfabetizacao", "std"),
               media_portugues_media=("media_portugues", "mean"))
          .round(2))
    salvar(v2, "evolucao_temporal")

    # --- Visão 3: ranking de municípios por UF/ano (window rank) ---
    v3 = df_mun[df_mun["rede"] == "3"].copy()
    v3["sigla_uf"] = v3["id_municipio"].str[:2].map(IBGE_UF)
    v3["ranking_uf"] = (v3.groupby(["sigla_uf", "ano"])["taxa_alfabetizacao"]
                          .rank(ascending=False, method="min").astype(int))
    v3 = v3[["id_municipio", "sigla_uf", "ano", "serie", "rede_desc",
             "taxa_alfabetizacao", "media_portugues", "ranking_uf"]].rename(
        columns={"rede_desc": "rede"})
    salvar(v3, "ranking_municipios")

    # --- Visão 4: comparação com metas nacionais e estaduais ---
    v4 = (df_uf[df_uf["rede"] == "0"]
          .groupby(["sigla_uf", "ano"], as_index=False)
          .agg(taxa_uf=("taxa_alfabetizacao", "mean")).round(2))
    meta_br = df_meta_br[["ano", "meta_alfabetizacao_2025", "meta_alfabetizacao_2030"]].rename(
        columns={"meta_alfabetizacao_2025": "meta_nacional_2025",
                 "meta_alfabetizacao_2030": "meta_nacional_2030"})
    meta_uf = df_meta_uf[["ano", "sigla_uf", "meta_alfabetizacao_2025"]].rename(
        columns={"meta_alfabetizacao_2025": "meta_uf_2025"})
    v4 = (v4.merge(meta_br, on="ano", how="left")
            .merge(meta_uf, on=["ano", "sigla_uf"], how="left"))
    v4["gap_meta_nacional"] = (v4["taxa_uf"] - v4["meta_nacional_2025"]).round(2)
    v4["gap_meta_uf"] = (v4["taxa_uf"] - v4["meta_uf_2025"]).round(2)
    v4["status_meta"] = np.where(
        v4["taxa_uf"] >= v4["meta_nacional_2025"], "ATINGIU", "NAO_ATINGIU")
    salvar(v4, "comparacao_metas_nacionais")

    return resultados


# =============================================================================
# SNAPSHOT — fingerprint do estado do lake para comparação de idempotência
# =============================================================================

def snapshot_camada(lake_dir, prefixo: str) -> pd.DataFrame:
    """Percorre lake_dir/<prefixo> e devolve um DataFrame com uma linha por
    partição física: caminho relativo, nº de linhas e hash MD5 do conteúdo
    de negócio (colunas voláteis de timestamp excluídas, linhas ordenadas).

    Duas execuções são consideradas idempotentes quando produzem snapshots
    idênticos: mesmas partições, mesmas contagens e mesmos hashes.
    """
    lake_dir = Path(lake_dir)
    base = lake_dir / prefixo
    linhas = []

    for parquet in sorted(base.rglob("*.parquet")):
        df = pd.read_parquet(parquet)
        df = df.drop(columns=[c for c in COLUNAS_VOLATEIS if c in df.columns])
        df = df.reindex(sorted(df.columns), axis=1)
        df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        conteudo = df.to_csv(index=False).encode()
        linhas.append({
            "particao": str(parquet.parent.relative_to(lake_dir)),
            "n_linhas": len(df),
            "hash_negocio": hashlib.md5(conteudo).hexdigest(),
        })

    return pd.DataFrame(linhas).sort_values("particao").reset_index(drop=True)


def comparar_snapshots(snap_a: pd.DataFrame, snap_b: pd.DataFrame) -> pd.DataFrame:
    """Compara dois snapshots partição a partição e devolve o resultado com
    a coluna `idempotente` (True quando contagem e hash coincidem)."""
    comp = snap_a.merge(snap_b, on="particao", how="outer",
                        suffixes=("_exec1", "_exec2"), indicator=True)
    comp["idempotente"] = (
        (comp["_merge"] == "both")
        & (comp["n_linhas_exec1"] == comp["n_linhas_exec2"])
        & (comp["hash_negocio_exec1"] == comp["hash_negocio_exec2"])
    )
    return comp.drop(columns="_merge")
