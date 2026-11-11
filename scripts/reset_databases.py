"""Vacía los DATOS de las 3 bases y el staging, conservando esquemas, índices y constraints.

Uso:  docker compose run --rm db-init -m scripts.reset_databases
"""
import shutil

from etl.common import STAGING_DIR, cassandra_session, mongo_db, neo4j_driver

CASSANDRA_TABLES = ["actividad_diaria_por_subreddit", "actividad_diaria_por_fecha", "actividad_por_hora",
                    "top_autores_por_dia", "kpis_por_nombre", "kpis_por_fecha"]


def main():
    with mongo_db() as db:
        for col in ("posts", "etl_runs", "rechazados"):
            print(f"  [mongo] {col}: {db[col].delete_many({}).deleted_count} documentos eliminados")
    with cassandra_session() as s:
        for t in CASSANDRA_TABLES:
            s.execute(f"TRUNCATE {t}")
        print(f"  [cassandra] truncadas: {CASSANDRA_TABLES}")
    with neo4j_driver() as d:
        with d.session() as s:  # CALL ... IN TRANSACTIONS requiere transacción implícita
            s.run("MATCH (n) WHERE NOT n:Subreddit CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS").consume()
        print("  [neo4j] nodos eliminados (se conservan los 3 Subreddit)")
    shutil.rmtree(STAGING_DIR, ignore_errors=True)
    print(f"  [staging] eliminado {STAGING_DIR}")


if __name__ == "__main__":
    main()
