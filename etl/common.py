"""Utilidades compartidas por las etapas del ETL: conexiones, staging y bitácora."""
import datetime as dt
import os
from contextlib import contextmanager

import pandas as pd

from etl.config import (CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, DATA_DIR, MONGO_DB, MONGO_URI, NEO4J_AUTH,
                        NEO4J_URI)

STAGING_DIR = os.getenv("STAGING_DIR", os.path.join(os.path.dirname(os.path.abspath(DATA_DIR)), "..", "staging"))
UTC = dt.timezone.utc


# ---------------- fechas ----------------
def day_bounds(fecha: str) -> tuple[int, int]:
    """[inicio, fin) del día UTC en segundos Unix."""
    d = dt.date.fromisoformat(fecha)
    start = int(dt.datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp())
    return start, start + 86400


def to_datetime(ts: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(int(ts), tz=UTC)


def now_utc() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


# ---------------- staging (intercambio entre tareas del DAG) ----------------
def staging_path(fecha: str, name: str) -> str:
    folder = os.path.join(STAGING_DIR, f"fecha={fecha}")
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{name}.parquet")


def save_stage(df: pd.DataFrame, fecha: str, name: str) -> None:
    df.to_parquet(staging_path(fecha, name), index=False)


def load_stage(fecha: str, name: str) -> pd.DataFrame:
    return pd.read_parquet(staging_path(fecha, name))


def records(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> lista de dicts con tipos nativos de Python y None en vez de NaN."""
    if df.empty:
        return []
    return df.astype(object).where(df.notna(), None).to_dict("records")


# ---------------- conexiones ----------------
@contextmanager
def mongo_db():
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        yield client[MONGO_DB]
    finally:
        client.close()


@contextmanager
def cassandra_session():
    from cassandra.cluster import Cluster
    cluster = Cluster(CASSANDRA_HOSTS)
    try:
        yield cluster.connect(CASSANDRA_KEYSPACE)
    finally:
        cluster.shutdown()


@contextmanager
def neo4j_driver():
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    try:
        yield driver
    finally:
        driver.close()


# ---------------- bitácora de ejecuciones (MongoDB etl_runs) ----------------
def log_run(fecha: str, etapa: str, resumen: dict) -> dict:
    """Guarda el resumen de una etapa en etl_runs (_id = fecha). Upsert => idempotente."""
    with mongo_db() as db:
        db.etl_runs.update_one(
            {"_id": fecha},
            {"$set": {f"etapas.{etapa}": {**resumen, "ejecutado_en": now_utc()}, "actualizado_en": now_utc()}},
            upsert=True,
        )
    print(f"[{fecha}] {etapa}: {resumen}")
    return resumen
