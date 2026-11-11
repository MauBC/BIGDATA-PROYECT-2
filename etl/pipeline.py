"""Orden de las 6 etapas y ejecución manual (sin Airflow) para pruebas.

Uso:  python -m etl.pipeline 2026-06-01 [2026-06-05]   (un día o un rango inclusivo)
"""
import datetime as dt
import sys

from etl import clean, extract, kpis, load_cassandra, load_mongo, load_neo4j

ETAPAS = [
    ("extraccion", extract.run),
    ("limpieza", clean.run),
    ("carga_mongodb", load_mongo.run),
    ("carga_cassandra", load_cassandra.run),
    ("carga_neo4j", load_neo4j.run),
    ("kpis", kpis.run),
]


def run_day(fecha: str) -> None:
    for _, fn in ETAPAS:
        fn(fecha)


def main(argv: list[str]) -> None:
    desde = dt.date.fromisoformat(argv[0])
    hasta = dt.date.fromisoformat(argv[1]) if len(argv) > 1 else desde
    d = desde
    while d <= hasta:
        run_day(d.isoformat())
        d += dt.timedelta(days=1)


if __name__ == "__main__":
    main(sys.argv[1:])
