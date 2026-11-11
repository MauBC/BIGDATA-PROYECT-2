"""Crea (o actualiza) los esquemas de MongoDB, Cassandra y Neo4j. Idempotente.

Uso:
  dentro de Docker : docker compose run --rm db-init
  desde Windows    : python -m scripts.init_databases   (desde la raíz del proyecto)
"""
import re
import sys
import time
from pathlib import Path

from cassandra.cluster import Cluster
from neo4j import GraphDatabase

from etl.config import CASSANDRA_HOSTS, NEO4J_AUTH, NEO4J_URI
from scripts import init_mongo

HERE = Path(__file__).resolve().parent


def split_statements(path: Path, comment: str) -> list[str]:
    """Quita comentarios de línea y separa sentencias por ';'."""
    text = "\n".join(re.split(re.escape(comment), line, maxsplit=1)[0] for line in path.read_text(encoding="utf-8").splitlines())
    return [s.strip() for s in text.split(";") if s.strip()]


def retry(fn, name, attempts=20, wait=5):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            if i == attempts:
                raise
            print(f"  [{name}] no disponible ({type(e).__name__}), reintento {i}/{attempts}...")
            time.sleep(wait)


def _cassandra_connect():
    cluster = Cluster(CASSANDRA_HOSTS)
    try:
        return cluster, cluster.connect()
    except Exception:
        cluster.shutdown()
        raise


def init_cassandra():
    cluster, session = retry(_cassandra_connect, "cassandra")
    try:
        for stmt in split_statements(HERE / "cassandra_schema.cql", "--"):
            session.execute(stmt)
        tablas = [r.table_name for r in session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'reddit'")]
        print(f"  [cassandra] tablas en keyspace reddit: {sorted(tablas)}")
    finally:
        cluster.shutdown()


def init_neo4j():
    with GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH) as driver:
        retry(driver.verify_connectivity, "neo4j")
        for stmt in split_statements(HERE / "neo4j_schema.cypher", "//"):
            driver.execute_query(stmt)
        cons = [r["name"] for r in driver.execute_query("SHOW CONSTRAINTS YIELD name").records]
        subs = driver.execute_query("MATCH (s:Subreddit) RETURN count(s) AS n").records[0]["n"]
        print(f"  [neo4j] constraints: {sorted(cons)} | nodos Subreddit: {subs}")


def main():
    print("== MongoDB");   retry(init_mongo.main, "mongo")
    print("== Cassandra"); init_cassandra()
    print("== Neo4j");     init_neo4j()
    print("Esquemas listos.")


if __name__ == "__main__":
    sys.exit(main())
