# Verificación — 13 de noviembre de 2026

Pruebas ejecutadas en una instalación limpia (sin contenedores ni volúmenes previos) en Windows 11 con
Docker Desktop 28.0.4 (8 GB, 12 CPU). Todo lo que figura aquí se **observó en esta sesión**. Cuando
se mencionan cifras del informe técnico, se indica si se reprodujeron.

Los scripts de comprobación están en `scripts/verificacion/` y `dashboard/prueba_dashboard.py`; cada
sección incluye el comando para repetirla.

## Resumen

| # | Prueba | Resultado |
|---|---|---|
| 1 | Arranque desde cero | ✅ Imágenes construidas, 6 servicios *healthy* y el dashboard en marcha, `db-init` y `airflow-init` con código 0, DAG sin errores de importación |
| 2 | Carga corta 31-may → 02-jun | ✅ Coincide con la fuente en las 3 bases. ❌ El comando del README (desde el 01-jun) perdía 43 comentarios: **corregido** |
| 3 | Histórico completo con el DAG | ✅ 105/105 ejecuciones, 630/630 tareas, sin reintentos, 20.8 min. Todos los conteos coinciden con la fuente y con el informe |
| 4 | Reproceso con la misma fuente | ✅ *Clear* del 15-jul: IDs, relaciones, KPIs y actividad sin diferencias |
| 4b | Reproceso con fuente corregida | ❌ Antes: posts, comentarios, nodos, enlaces y rechazados obsoletos. ✅ Tras las correcciones: el día se reemplaza y los días dependientes se restauran al reprocesarlos |
| 5 | CRUD de las 3 bases | ✅ Todas las operaciones; foto antes/después sin diferencias (no quedan registros `demo_*`) |
| 6 | Dashboard | ✅ 5 pestañas y 4 combinaciones de filtros sin excepciones; un rango sin datos muestra aviso; totales y KPIs coinciden con las bases. ⚠️ No se revisó visualmente en un navegador |

## 1. Arranque

```powershell
docker version; docker compose config --quiet
docker compose up -d --build
docker compose ps -a
docker compose exec airflow-scheduler airflow dags list-import-errors   # "No data found"
docker compose exec airflow-scheduler airflow tasks list reddit_etl      # 6 tareas
```

- Servidor Docker 28.0.4; `docker compose config --quiet` correcto. Se conservó el `.env` existente.
- Imágenes: Airflow instala `pymongo 4.11`, `cassandra-driver 3.29.2` y `neo4j 5.27.0` (fijados por las
  *constraints* de Airflow). El dashboard instala las versiones de `requirements.txt`; sus dependencias
  transitivas no están fijadas (p. ej. resolvió `numpy 2.4.6`).
- `db-init` crea 3 colecciones con 8 índices, 6 tablas Cassandra y 4 *constraints* de Neo4j; `airflow-init`
  crea el usuario admin. Ambos terminan con código 0.
- **Hallazgo corregido**: el dashboard y el scheduler no esperaban a `db-init` (el dashboard arrancó
  antes de que existieran los esquemas). Ahora dependen de `db-init: service_completed_successfully`.

## 2. Carga corta

```powershell
docker compose exec -e STAGING_DIR=/tmp/stg_corta -w /opt/airflow airflow-scheduler python -m etl.pipeline 2026-05-31 2026-06-02
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.conteos 2026-06-02
```

Resultado: 1,576 posts, 1,211 comentarios anidados, 328 rechazados (`autor_bot`), 1,796 usuarios,
493 `RESPONDE_A`, 0 comentarios sin post y 0 días con diferencias en Cassandra; todo igual a lo
esperado desde los Parquet.

**Fallo reproducido (código original):** el comando del README empezaba el 2026-06-01. Los comentarios
del 01 y 02-jun sobre posts del 31-may no tenían documento destino. La bitácora reportaba
`comentarios_cargados = 1,879`, pero solo había 1,836 anidados: **43 comentarios perdidos en
silencio** en MongoDB y 43 nodos `Comentario` sin `PERTENECE_A` en Neo4j. Ahora la etapa 3 cuenta lo
que realmente anida, registra `comentarios_sin_post` y falla con un mensaje que indica procesar antes
los días de los posts. El README empieza en el 2026-05-31.

## 3. Histórico completo (DAG desde bases vacías)

```powershell
docker compose run --rm -e STAGING_DIR=/tmp/x db-init -m scripts.reset_databases
docker compose exec airflow-scheduler airflow dags unpause reddit_etl
# al terminar
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.conteos
```

Airflow: 105 ejecuciones (2026-05-31 → 2026-09-12), 630 instancias de tarea en `success`, 0 reintentos,
20.8 min en total (mediana de 11.1 s por ejecución, máximo de 16.9 s). Los tiempos se mantienen estables
a medida que crece el grafo.

| Comprobación | Esperado (fuente) | Observado | Informe |
|---|---|---|---|
| Posts válidos (MongoDB / Neo4j) | 43,913 | 43,913 / 43,913 | 43,913 ✅ |
| Comentarios válidos (anidados / nodos) | 49,521 | 49,521 / 49,521 | 49,521 ✅ |
| Rechazados (`autor_bot`) | 10,353 | 10,353 | 10,353 ✅ |
| IDs de comentario duplicados en MongoDB | 0 | 0 | – |
| `etl_runs` con las 6 etapas | 105 | 105 | – |
| `Usuario` | 41,720 | 41,720 | 41,720 ✅ |
| `PUBLICA` / `COMENTA` | 42,720 / 48,612 | 42,720 / 48,612 | ✅ |
| `PERTENECE_A` / `EN` | 49,521 / 43,913 | 49,521 / 43,913 | ✅ |
| `RESPONDE_A` | 22,452 (+46 padres descartados) | 22,452; 46 respuestas sin padre | ✅ |
| Comentarios sin post / usuarios huérfanos | 0 / 0 | 0 / 0 | – |
| Filas Cassandra (actividad ×2 / hora / top / kpis ×2) | – | 315 / 7,560 / 5,751 / 2,915 | ✅ |
| Días × comunidad con totales distintos de la fuente | 0 | 0 de 315 | – |
| KPI de conexiones al 12-sep | – | 897 (ReaLitTea) | 897 ✅ |
| Usuarios en más de un subreddit / grado medio | – | 85 / 2.19 | 85 / 2.2 ✅ |

Totales en Cassandra por comunidad (posts / comentarios válidos): LocalLLaMA 3,778 / 20,405;
wallstreetbets 10,310 / 12,068; SkincareAddiction 29,825 / 17,048.

Las 46 respuestas sin `RESPONDE_A` tienen un comentario padre escrito por un bot y descartado en la
limpieza; no hay comentarios anteriores a su post ni respuestas anteriores a su padre. Ningún post
destino faltó durante el DAG (`comentarios_sin_post_reportados = 0`).

## 4. Reproceso

### Misma fuente

```powershell
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/antes.json
docker compose exec airflow-scheduler airflow tasks clear reddit_etl -s 2026-07-15 -e 2026-07-15 -y
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/despues.json
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto comparar /tmp/antes.json /tmp/despues.json
```

Las 6 tareas del 15-jul se volvieron a ejecutar (`try 2`) y la comparación de la foto completa
(93,434 IDs, 207,218 relaciones, 2,915 KPIs por tabla y 315 filas de actividad) dio **SIN DIFERENCIAS**.
En la bitácora: 0 posts insertados, 398 actualizados y 448 comentarios retirados y vueltos a anidar.

### Fuente corregida (prueba destructiva, proyecto Compose aislado)

```powershell
docker compose stop   # libera RAM; los datos del stack principal quedan en sus volúmenes
docker compose -p bigdata-test -f docker-compose.yml -f scripts/verificacion/compose.prueba.yml up -d db-init
docker compose -p bigdata-test -f docker-compose.yml -f scripts/verificacion/compose.prueba.yml run --rm --no-deps --entrypoint python db-init -m scripts.verificacion.correccion
docker compose -p bigdata-test -f docker-compose.yml -f scripts/verificacion/compose.prueba.yml down -v
docker compose up -d
```

Sobre una copia de la fuente: el comentario `op1x4k4` (01-jun, con la respuesta `op22oi6`) pasa a
ser de un bot y el post `1ttd6gq` (01-jun, con 3 comentarios ese día y 1 el 02-jun) queda sin título.
Solo se reprocesa el 01-jun.

| Efecto | Código original | Código corregido |
|---|---|---|
| Post y comentario ahora inválidos en MongoDB | Seguían en `posts` | Eliminados |
| Nodos, `RESPONDE_A` de la respuesta y usuario huérfano en Neo4j | Seguían | Eliminados |
| Comentarios del post inválido | Seguían válidos | Rechazados (`post_inexistente`) |
| KPIs del 01-jun | Recalculados | Recalculados (y se borran los que dejan de aplicar) |
| Crecimiento del 02-jun (depende del 01-jun) | Obsoleto | Recalculado |
| Al volver a la fuente original: `rechazados` | 2 entradas obsoletas | Reemplazados por fecha |
| Al volver a la fuente original: resto | – | Igual salvo el comentario `op9ff2r` del **02-jun** sobre el post restaurado |
| Reprocesando también el 02-jun | – | **SIN DIFERENCIAS** respecto a la foto inicial |

Limitación documentada: los efectos sobre **otros días** (comentarios posteriores de un post eliminado
o restaurado y el KPI acumulado de conexiones) requieren reprocesar esos días:
`airflow tasks clear reddit_etl -s <día> -e 2026-09-12 -y`.

## 5. CRUD

```powershell
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/antes_crud.json
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_mongo
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_cassandra
docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_neo4j
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/despues_crud.json
docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto comparar /tmp/antes_crud.json /tmp/despues_crud.json
```

- **MongoDB**: inserta un post con comentario anidado; el validador rechaza un documento inválido
  (`WriteError`, código 121). Lee por `_id` y el top 3 de wallstreetbets (211, 198 y 174 comentarios).
  Hace `$set`, `$push` y `$pull`, y `delete_one` (1 documento eliminado).
- **Cassandra**: INSERT de 3 días (ordenados por fecha DESC), upsert sobre la misma clave, SELECT por
  partición y rango, UPDATE, INSERT con TTL de 3 s (la fila desaparece) y DELETE de fila y de partición.
- **Neo4j**: MERGE de usuarios y post; relaciones `PUBLICA`, `EN`, `COMENTA`, `PERTENECE_A` y `RESPONDE_A`;
  lectura del subgrafo; `SET p += {...}`; DELETE de relación y `DETACH DELETE` de 5 nodos.
- Las tres terminan con código 0 y la foto antes/después da **SIN DIFERENCIAS**.

## 6. Dashboard

```powershell
docker compose exec -w /app dashboard python dashboard/prueba_dashboard.py
```

Ejecuta la app con `streamlit.testing.AppTest` (sin navegador) y resume excepciones, avisos, pestañas y
métricas. `http://localhost:8501/_stcore/health` responde `ok`.

| Escenario | Excepciones | Registros | Posts | Comentarios | Com./post publicado | Eliminados |
|---|---|---|---|---|---|---|
| Rango completo, 3 comunidades | 0 | 93,434 | 43,913 | 49,521 | 1.13 | 1.4 % |
| Solo LocalLLaMA | 0 | 24,183 | 3,778 | 20,405 | 5.40 | 1.8 % |
| Solo septiembre (1–12) | 0 | 232 | 0 | 232 | – | 0.0 % |
| Julio, wsb + Skincare | 0 | 21,574 | 12,571 | 9,003 | 0.72 | 1.3 % |
| 5–12 sep **antes** de cargarlo | 0 | Aviso «No hay datos cargados en Cassandra…» | | | | |

En todos los casos se renderizan las 5 pestañas y los 8 KPIs. Con una sola comunidad aparece el aviso
de que el KPI de conexiones es global. En septiembre, las tablas de posts y flairs muestran un aviso en
vez de quedar vacías.

Contraste con las bases:
- Julio, wsb + Skincare: Cassandra suma 12,571 posts y 9,003 comentarios (= 21,574) y MongoDB da
  0.72 comentarios por post publicado.
- Score medio de LocalLLaMA: 8.53 en el dashboard, 8.53 en Cassandra (Σ score / Σ posts) y 8.53 en MongoDB.
- Tasa de eliminados global: 1.43 % en el dashboard y en MongoDB (707 de 49,521).

**No verificado**: aspecto visual en un navegador (no había herramienta de navegador disponible en la
sesión). Streamlit avisa en los logs de que `use_container_width` está obsoleto, sin efecto con la
versión fijada (1.50).

## 7. Cambios que debe recoger el informe técnico

Las cifras de conteo del informe (secciones 4, 6 y 7) y de las conclusiones se reprodujeron. Debe
actualizarse lo siguiente:

1. **Sección 5 (MongoDB, idempotencia).** Al reprocesar se reemplaza la porción del día: se retiran los
   comentarios anidados de esa fecha y se vuelven a anidar, y se eliminan los posts del día que dejaron
   de ser válidos. Si falta un post destino, la carga falla en lugar de descartar comentarios.
2. **Sección 7 (Neo4j).** Los `Comentario` guardan `post_id` y `parent_id` (dos índices nuevos) para
   enlazar en ambos sentidos. Al reprocesar se eliminan los nodos de la fecha que ya no son válidos y
   los usuarios huérfanos.
3. **Sección 8 (DAG).** Tiempo observado: 11.1 s de mediana por día y 20.8 min en total. La prueba del
   15-jul se repitió comparando IDs, relaciones y KPIs, no solo conteos. Conviene añadir el caso de la
   fuente corregida y el comando para reprocesar los días siguientes.
4. **Sección 9 (KPIs).**
   - Renombrar «Engagement (comentarios por post)» como **«Razón diaria comentarios / posts»** y aclarar
     que no mide la conversación por publicación. Añadir **«Comentarios por post publicado»**
     (MongoDB): LocalLLaMA 5.40 · wsb 1.17 · Skincare 0.57 · global 1.13.
   - La tabla dice «promedio diario». Los valores del informe son medias simples de los valores diarios
     y se reproducen así. El dashboard ahora pondera por volumen (31-may → 31-ago):

     | KPI | Informe (media diaria) | Ponderado (dashboard) |
     |---|---|---|
     | Razón comentarios / posts | 5.3 · 1.2 · 0.6 | 5.37 · 1.17 · 0.57 |
     | Tasa de eliminados | 2.2 % · 1.2 % · 1.2 % | 1.85 % · 1.14 % · 1.14 % |
     | Profundidad media | 1.10 · 0.59 · 0.62 | 1.11 · 0.63 · 0.64 |
     | Score medio por post | 8.2 · 4.3 · 1.3 | 8.53 · 4.25 · 1.31 |
     | Actividad por usuario | 1.65 (Skincare) · 1.2 (wsb) | 1.65 · 1.21 |

     (Orden: LocalLLaMA · wsb · Skincare, salvo en la última fila). Las cifras globales de registros
     (1,002/día; 93,434), crecimiento (mediana +1.3 %, de −60 % a +270 %) y conexiones (897) no cambian.
   - Indicar que el KPI de conexiones es global (no se filtra por comunidad) y acumulado.
5. **Sección 11 (capturas).** Rehacer la figura 4 (tarjeta «Comentarios por post publicado» y valores
   del período con su método) y la figura 8 (rechazados filtrados por rango y comunidad). La figura 7
   tiene ahora una nota sobre los totales globales del grafo.
6. **Conclusiones.** «LocalLLaMA … 5.3 comentarios por post» debe decir «5.4 comentarios por post
   publicado» o identificarse como razón diaria. El resto se reprodujo: 17.3 % de bots (93.0 %
   AutoModerator), 68 % de los posts y 78 % sin respuesta en Skincare, pico de wsb a las 21 h UTC, grado
   medio 2.2, grado máximo 1,084 (los 1,084 comentarios de ReaLitTea) y 897 conexiones, 85 usuarios en
   más de una comunidad.
7. **Anexo.** Añadir el comando manual desde el 2026-05-31 y los comandos de verificación.
