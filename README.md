# Pipeline multimodelo de Big Data sobre Reddit

Proyecto Grupal II · Big Data · Facultad de Computación · UTEC

Pipeline que toma una muestra de Reddit, la limpia y la reparte en tres bases NoSQL según el tipo de consulta
que mejor resuelve cada una. Un DAG de Airflow orquesta la carga día por día y un dashboard en Streamlit
muestra los indicadores.

```
parquet  →  Airflow (6 etapas)  →  MongoDB + Cassandra + Neo4j  →  Streamlit
```

## Integrantes

- Bobadilla Castillo, Mauro
- Huaman Casma, Leonardo Fabian
- Mora Huamanchay, Angel Obed
- Rondan Cantaro, Lisseth Jomely
- Sobenes Obregon, Carlos Sebastian

## Entregables

| Entregable | Ubicación |
|---|---|
| Informe técnico | `Informe_Tecnico_Reddit_BigData.pdf` |
| Código fuente | `etl/`, `dags/`, `crud/`, `dashboard/` |
| Scripts de creación de las bases | `scripts/` |
| DAG de Airflow | `dags/reddit_etl_dag.py` |
| Dashboard | `dashboard/app.py` |
| Video demostrativo | _(agregar enlace)_ |

## Dataset

[mashu-data/reddit-comments-sample](https://huggingface.co/datasets/mashu-data/reddit-comments-sample)
(Hugging Face, licencia CC BY-NC 4.0): 43,913 posts y 59,874 comentarios (103,787 registros) de r/LocalLLaMA,
r/wallstreetbets y r/SkincareAddiction, entre el 31 de mayo y el 12 de septiembre de 2026. Ya viene incluido en
`data/raw/`, con su ficha original (`data/raw/README.md`), así que no hace falta descargarlo.

## Qué guarda cada base

| Base | Contenido | Por qué |
|---|---|---|
| MongoDB | Cada post con sus comentarios anidados, bitácora de ejecuciones y registros rechazados | Un hilo completo se lee en una sola consulta |
| Cassandra | Actividad diaria y por hora, ranking de autores y KPIs por fecha | Series temporales leídas por partición, ya ordenadas por fecha |
| Neo4j | Usuarios, posts, comentarios y quién responde a quién | Las conexiones entre usuarios son recorridos de grafo |

## Requisitos

- Docker Desktop con al menos 8 GB de RAM asignados.
- Puertos libres: 8080, 8501, 7474, 7687, 9042 y 27017.

## Puesta en marcha

```powershell
Copy-Item .env.example .env      # solo la primera vez
docker compose up -d --build
```

Esto levanta MongoDB, Cassandra, Neo4j, Airflow (con Postgres) y el dashboard. El servicio `db-init` crea los
esquemas de las tres bases y termina; Airflow y el dashboard esperan a que acabe. La primera vez tarda varios
minutos.

| Servicio | URL | Usuario / contraseña |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Dashboard | http://localhost:8501 | – |
| Neo4j Browser | http://localhost:7474 | neo4j / neo4j12345 |
| MongoDB | mongodb://localhost:27017 | admin / admin123 |
| Cassandra | localhost:9042 | – |

Las credenciales son de desarrollo local (`.env.example`).

## Ejecutar el pipeline

1. En Airflow, activar el DAG `reddit_etl` (se crea pausado).
2. Con `catchup=True` se generan 105 ejecuciones, una por día del 31-may al 12-sep, que corren en orden.
   El histórico completo tarda unos 21 minutos.
3. Para reprocesar un día: vista *Grid* → seleccionar la ejecución → **Clear**. La carga es idempotente.
   Si el día reprocesado afecta a los siguientes, se reprocesan también:

```powershell
docker compose exec airflow-scheduler airflow tasks clear reddit_etl -s 2026-07-15 -e 2026-09-12 -y
```

Ejecución manual sin Airflow (empezar siempre el 2026-05-31, porque los comentarios necesitan que su post ya
esté cargado):

```powershell
docker compose exec -w /opt/airflow airflow-scheduler python -m etl.pipeline 2026-05-31 2026-06-02
```

## Demos CRUD

```powershell
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_mongo
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_cassandra
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_neo4j
```

Trabajan sobre registros `demo_*` y los borran al final, así que no alteran los datos del pipeline.

## Dashboard

http://localhost:8501. Filtros de rango de fechas (UTC) y de subreddits, cinco tarjetas y cinco pestañas:
KPIs, Actividad (Cassandra), Contenido (MongoDB), Red (Neo4j) y Pipeline. Cada KPI muestra su definición,
fórmula e interpretación.

## Verificación

```powershell
# Conteos esperados desde los parquet frente a lo cargado en las tres bases
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.conteos
# Foto de las tres bases antes y después de reprocesar o de correr las demos CRUD
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/antes.json
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto comparar /tmp/antes.json /tmp/despues.json
# Dashboard sin navegador
docker compose exec -w /app dashboard python dashboard/prueba_dashboard.py
```

Para vaciar los datos y empezar de nuevo (conserva los esquemas):

```powershell
docker compose run --rm db-init -m scripts.reset_databases
```

## Estructura

```
docker-compose.yml     servicios
docker/                Dockerfiles de Airflow y del dashboard
data/raw/              dataset (parquet) con su ficha original
dags/                  DAG reddit_etl
etl/                   etapas: extract, clean, load_mongo, load_cassandra, load_neo4j, kpis
scripts/               creación de las bases (*.cql, *.cypher, init_*.py), reset y verificación
crud/                  demos CRUD de las tres bases
dashboard/             app Streamlit y sus consultas
```

## Problemas frecuentes

- **Airflow no muestra el DAG:** esperar un minuto a que escanee la carpeta `dags/`.
- **Docker Desktop no arranca después de un apagado forzado:** cerrar Docker, renombrar las carpetas
  `%LOCALAPPDATA%\Docker\run` y `%LOCALAPPDATA%\docker-secrets-engine` y volver a abrirlo.
- **Rutas `/tmp/...` en Git Bash:** se convierten en rutas de Windows; usar PowerShell o anteponer `MSYS_NO_PATHCONV=1`.
