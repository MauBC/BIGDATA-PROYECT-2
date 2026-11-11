# Proyecto Grupal II – Big Data (UTEC)

Pipeline multimodelo sobre una muestra de Reddit: **parquet → Airflow → MongoDB + Cassandra + Neo4j → Streamlit**.
El informe técnico completo está en `Informe_Tecnico_Reddit_BigData.pdf`.

## Requisitos
- Docker Desktop (Windows/macOS/Linux) con **≥ 8 GB de RAM** asignados.
- Puertos libres: 8080, 8501, 7474, 7687, 9042, 27017.
- Dataset ya descargado en `data/raw/` ([mashu-data/reddit-comments-sample](https://huggingface.co/datasets/mashu-data/reddit-comments-sample)).

## Puesta en marcha
Copiar `.env.example` a `.env` antes del primer arranque (`Copy-Item .env.example .env` en PowerShell). Si ya existe `.env`, conservarlo. Las credenciales de ejemplo son únicamente para desarrollo local.

```bash
docker compose up -d --build
```
Esto levanta MongoDB, Cassandra, Neo4j, Airflow (Postgres + webserver + scheduler) y el dashboard.
El servicio `db-init` crea automáticamente los esquemas de las 3 bases (idempotente).
La primera vez tarda unos minutos (descarga de imágenes y arranque de Cassandra).

| Servicio | URL | Credenciales (`.env`) |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Dashboard | http://localhost:8501 | – |
| Neo4j Browser | http://localhost:7474 | neo4j / neo4j12345 |
| MongoDB | mongodb://localhost:27017 | admin / admin123 |
| Cassandra | localhost:9042 (cqlsh) | – |

## Ejecutar el pipeline
1. Abrir Airflow y activar el DAG `reddit_etl` (está pausado al crearse).
2. Con `catchup=True` se generan 105 ejecuciones (2026-05-31 → 2026-09-12), una por día, en orden.
   Tiempo total aproximado: 25 min.
3. Re-procesar un día: en la vista *Grid*, seleccionar la ejecución → **Clear**. La carga es idempotente.

Ejecución manual sin Airflow (útil para pruebas):
```bash
docker compose exec -w /opt/airflow airflow-scheduler python -m etl.pipeline 2026-06-01 2026-06-03
```

Vaciar los datos (conserva esquemas) para volver a empezar:
```bash
docker compose run --rm db-init -m scripts.reset_databases
```

## Demos CRUD
```bash
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_mongo
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_cassandra
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_neo4j
```
Trabajan sobre registros `demo_*` y los eliminan al final: no alteran los datos del pipeline.

## Estructura
```
docker-compose.yml      servicios; .env con credenciales locales
docker/                 Dockerfiles de Airflow y del dashboard
data/raw/               dataset (parquet); data/staging/ intercambio entre tareas
dags/reddit_etl_dag.py  DAG diario con las 6 etapas
etl/                    extract, clean, load_mongo, load_cassandra, load_neo4j, kpis, pipeline
scripts/                creación de esquemas (init_*.py, *.cql, *.cypher) y reset
crud/                   demos CRUD de las 3 bases
dashboard/              app Streamlit (app.py) y consultas (consultas.py)
```

## Revisión inicial
Consultar [REVISION_INICIAL.md](REVISION_INICIAL.md) para conocer las comprobaciones realizadas y los pendientes. El dataset fuente se incluye con su documentación y atribución; `data/staging/`, las cachés y `.env` quedan fuera de Git.

## Problemas conocidos
- **Docker Desktop no arranca** con un error tipo `remove ...\dockerInference: The file cannot be accessed by the system`:
  quedan sockets huérfanos de un cierre forzado. Cerrar Docker y renombrar las carpetas
  `%LOCALAPPDATA%\Docker\run` y `%LOCALAPPDATA%\docker-secrets-engine`; Docker las recrea al iniciar.
  Para evitarlo, salir de Docker con *Quit* antes de apagar el equipo.
- Si Airflow no muestra el DAG recién copiado, esperar ~1 min (escaneo de la carpeta `dags/`).
