"""DAG reddit_etl – pipeline multimodelo diario (MongoDB + Cassandra + Neo4j).

Carga incremental simulada: cada ejecución procesa UN día del dataset, el de su fecha lógica
(`ds` = data_interval_start). Con catchup=True Airflow genera una ejecución por cada día entre
start_date y end_date (2026-05-31 → 2026-09-12) y las corre en orden (max_active_runs=1),
de modo que los posts siempre se cargan antes que los comentarios de días posteriores.

Todas las etapas son idempotentes: "Clear" sobre cualquier ejecución reprocesa ese día sin duplicar.
"""
import pendulum
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "grupo-bigdata",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="reddit_etl",
    description="Reddit parquet -> limpieza -> MongoDB -> Cassandra -> Neo4j -> KPIs (un día por ejecución)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 5, 31, tz="UTC"),
    end_date=pendulum.datetime(2026, 9, 12, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigdata", "reddit", "utec"],
    doc_md=__doc__,
)
def reddit_etl():
    @task(task_id="1_extraccion")
    def extraccion(ds=None):
        from etl import extract
        return extract.run(ds)

    @task(task_id="2_validacion_limpieza")
    def validacion_limpieza(ds=None):
        from etl import clean
        return clean.run(ds)

    @task(task_id="3_carga_mongodb")
    def carga_mongodb(ds=None):
        from etl import load_mongo
        return load_mongo.run(ds)

    @task(task_id="4_transformacion_cassandra")
    def transformacion_cassandra(ds=None):
        from etl import load_cassandra
        return load_cassandra.run(ds)

    @task(task_id="5_grafo_neo4j")
    def grafo_neo4j(ds=None):
        from etl import load_neo4j
        return load_neo4j.run(ds)

    @task(task_id="6_generacion_kpis")
    def generacion_kpis(ds=None):
        from etl import kpis
        return kpis.run(ds)

    (extraccion() >> validacion_limpieza() >> carga_mongodb() >> transformacion_cassandra()
     >> grafo_neo4j() >> generacion_kpis())


reddit_etl()
