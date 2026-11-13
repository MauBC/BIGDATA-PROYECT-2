# Proyecto Grupal II – Big Data (UTEC)

Pipeline multimodelo sobre una muestra de Reddit: **parquet → Airflow → MongoDB + Cassandra + Neo4j → Streamlit**.

| Documento | Contenido |
|---|---|
| `Informe_Tecnico_Reddit_BigData.pdf` | Informe técnico. Es anterior a las mejoras del 13-nov; los cambios pendientes están en `VERIFICACION.md` §7 |
| [MEJORAS.md](MEJORAS.md) | Resumen de las mejoras y correcciones del 13-nov |
| [VERIFICACION.md](VERIFICACION.md) | Pruebas reproducidas con sus comandos y resultados |
| [REVISION_INICIAL.md](REVISION_INICIAL.md) | Revisión inicial y estado de sus pendientes |

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
El servicio `db-init` crea automáticamente los esquemas de las 3 bases (idempotente) y termina; el
scheduler de Airflow y el dashboard esperan a que termine bien antes de arrancar.
La primera vez tarda unos minutos (descarga de imágenes y arranque de Cassandra).

| Servicio | URL | Credenciales (valores de `.env.example`) |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Dashboard | http://localhost:8501 | – |
| Neo4j Browser | http://localhost:7474 | neo4j / neo4j12345 |
| MongoDB | mongodb://localhost:27017 | admin / admin123 |
| Cassandra | localhost:9042 (cqlsh) | – |

## Ejecutar el pipeline
1. Abrir Airflow y activar el DAG `reddit_etl` (está pausado al crearse).
2. Con `catchup=True` se generan 105 ejecuciones (2026-05-31 → 2026-09-12), una por día, en orden.
   Tiempo total observado: ~21 min (unos 11 s por día).
3. Re-procesar un día: en la vista *Grid*, seleccionar la ejecución → **Clear**. La carga es idempotente:
   con la misma fuente se obtienen los mismos IDs, conteos y KPIs.

### Reproceso con datos corregidos
Cada ejecución **reemplaza la porción de su día** en las tres bases: elimina los posts y comentarios de esa
fecha que dejaron de ser válidos (con sus nodos y relaciones en Neo4j), reemplaza los `rechazados` del día,
borra los KPIs del día antes de escribirlos y recalcula el crecimiento del día siguiente.
Hay efectos sobre **otros días** que no se rehacen solos: comentarios posteriores de un post eliminado o
restaurado y el KPI acumulado de conexiones. En ese caso, reprocesar también los días siguientes:
```bash
docker compose exec airflow-scheduler airflow tasks clear reddit_etl -s 2026-07-15 -e 2026-09-12 -y
```

### Ejecución manual sin Airflow (útil para pruebas)
Empezar siempre en el **2026-05-31**: los comentarios se anidan en posts de días anteriores y, si el post
todavía no está cargado, la etapa de MongoDB falla para no perder comentarios en silencio.
```bash
docker compose exec -w /opt/airflow airflow-scheduler python -m etl.pipeline 2026-05-31 2026-06-02
```
El comando manual y el DAG escriben en **las mismas bases y el mismo staging** (`data/staging/`). Para no
mezclar una prueba con el histórico, usar un staging propio (`-e STAGING_DIR=/tmp/stg_prueba`) y vaciar
las bases antes y después de la prueba.

Vaciar los datos (conserva esquemas) para volver a empezar; también borra `data/staging/`:
```bash
docker compose run --rm db-init -m scripts.reset_databases
```

## Demos CRUD
```bash
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_mongo
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_cassandra
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_neo4j
```
Trabajan sobre registros `demo_*` y los eliminan al final: no alteran los datos del pipeline
(verificado comparando una foto de las tres bases antes y después).

## Dashboard
http://localhost:8501 · filtros de rango (UTC) y subreddits, cinco tarjetas y cinco pestañas: KPIs,
Actividad (Cassandra), Contenido (MongoDB), Red (Neo4j) y Pipeline (bitácora y rechazados).
- **Valor del período de cada KPI**: se pondera por volumen (razón de sumas), no es la media de los
  valores diarios; el crecimiento usa la mediana diaria. El método aparece bajo cada valor.
- **Razón diaria comentarios / posts** (KPI) y **comentarios por post publicado** (tarjeta, MongoDB) son
  distintos: la primera compara volúmenes del mismo día; la segunda sigue a cada publicación.
- El KPI **usuario con más conexiones** es global y acumulado: no responde al filtro de subreddits.
- Los posts terminan el 31-ago; desde el 1-sep solo hay comentarios tardíos.

## Estructura
```
docker-compose.yml      servicios; .env con credenciales locales
docker/                 Dockerfiles de Airflow y del dashboard
data/raw/               dataset (parquet); data/staging/ intercambio entre tareas
dags/reddit_etl_dag.py  DAG diario con las 6 etapas
etl/                    extract, clean, load_mongo, load_cassandra, load_neo4j, kpis, pipeline
scripts/                creación de esquemas (init_*.py, *.cql, *.cypher) y reset
scripts/verificacion/   comprobaciones reproducibles (conteos, fotos antes/después, reproceso corregido)
crud/                   demos CRUD de las 3 bases
dashboard/              app Streamlit (app.py), consultas (consultas.py) y prueba sin navegador
MEJORAS.md              resumen de mejoras · VERIFICACION.md pruebas y evidencia
```

## Verificación
[VERIFICACION.md](VERIFICACION.md) recoge las pruebas reproducidas (arranque, carga, histórico, reproceso,
CRUD y dashboard) con sus comandos. Las comprobaciones se pueden repetir con:
```bash
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.conteos   # fuente vs. 3 bases
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/antes.json
docker compose exec -w /app dashboard python dashboard/prueba_dashboard.py                    # dashboard sin navegador
```
La prueba de reproceso con datos corregidos es destructiva y se ejecuta en un proyecto aislado
(instrucciones en `scripts/verificacion/correccion.py`).

## Datos y Git
El dataset fuente se incluye con su documentación y atribución (`data/raw/README.md`); `data/staging/`,
las cachés y `.env` quedan fuera de Git.

## Problemas conocidos
- **Docker Desktop no arranca** con un error tipo `remove ...\dockerInference: The file cannot be accessed by the system`:
  quedan sockets huérfanos de un cierre forzado. Cerrar Docker y renombrar las carpetas
  `%LOCALAPPDATA%\Docker\run` y `%LOCALAPPDATA%\docker-secrets-engine`; Docker las recrea al iniciar.
  Para evitarlo, salir de Docker con *Quit* antes de apagar el equipo.
- Si Airflow no muestra el DAG recién copiado, esperar ~1 min (escaneo de la carpeta `dags/`).
- En Git Bash, las rutas tipo `/tmp/...` pasadas a `docker compose` se convierten en rutas de Windows.
  Usar PowerShell o anteponer `MSYS_NO_PATHCONV=1` al comando.
