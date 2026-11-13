# Mejoras — 13 de noviembre de 2026

Resumen de los cambios hechos tras reproducir el proyecto en una instalación limpia. La evidencia y los
comandos de cada prueba están en [VERIFICACION.md](VERIFICACION.md).

**Lo que no cambió:** la arquitectura del curso (MongoDB, Cassandra, Neo4j, Airflow y Streamlit), los
esquemas existentes, el DAG de 6 etapas y los resultados del histórico. Con la misma fuente, el pipeline
corregido produce exactamente las cifras del informe: 43,913 posts, 49,521 comentarios, 10,353
rechazados, 41,720 usuarios, 22,452 `RESPONDE_A` y 897 conexiones para ReaLitTea.

## 1. Pérdida silenciosa de comentarios (corregido)

**Problema.** Si se procesaba un día antes que el día de sus posts, los comentarios no tenían documento
donde anidarse y se descartaban sin aviso. La bitácora los daba igualmente por cargados. El comando
manual del README empezaba el 01-jun: se perdían 43 comentarios en MongoDB y quedaban 43 nodos
`Comentario` sin `PERTENECE_A` en Neo4j.

**Solución** (`etl/load_mongo.py`, `README.md`):
- La etapa 3 cuenta lo que realmente anida y registra `posts_destino_faltantes` y `comentarios_sin_post`.
- Si falta algún post destino, la tarea falla con un mensaje que indica procesar antes los días de esos posts.
- El comando manual del README empieza el 2026-05-31.

## 2. Reproceso con datos corregidos (corregido)

**Problema.** Reprocesar un día con la misma fuente no duplicaba nada. Pero si la fuente cambiaba (un
comentario pasaba a ser de un bot, un post quedaba inválido), los registros antiguos seguían en MongoDB
y Neo4j, `rechazados` conservaba entradas obsoletas y el crecimiento del día siguiente no se recalculaba.

**Solución.** Cada ejecución reemplaza la porción de su día en las tres bases:

| Componente | Cambio |
|---|---|
| `etl/clean.py` | Reemplaza los `rechazados` de la fecha. Los comentarios se validan contra los posts **válidos** (motivo `post_inexistente` = inexistente o rechazado) |
| `etl/extract.py` | `all_posts()` devuelve los campos obligatorios de todos los posts para esa validación |
| `etl/load_mongo.py` | Elimina los posts de la fecha que ya no son válidos; retira los comentarios anidados de la fecha y vuelve a anidar los válidos |
| `etl/load_neo4j.py` | Elimina los `Post`/`Comentario` de la fecha que ya no son válidos; rehace las relaciones de autoría y pertenencia (por si cambia el autor); borra los `Usuario` sin relaciones; enlaza `PERTENECE_A` y `RESPONDE_A` en ambos sentidos con `post_id`/`parent_id` |
| `scripts/neo4j_schema.cypher` | Índices `comentario_post_id` y `comentario_parent_id` |
| `etl/kpis.py` | Borra los KPIs del día antes de escribirlos (ninguno queda huérfano) y recalcula el crecimiento del día siguiente si ya se procesó |

Los efectos sobre **otros días** (comentarios posteriores de un post eliminado o restaurado y el KPI
acumulado de conexiones) se resuelven reprocesando esos días; el README indica el comando.

## 3. Orden de arranque (corregido)

**Problema.** El scheduler y el dashboard podían arrancar antes de que `db-init` creara los esquemas,
índices y *constraints*.

**Solución** (`docker-compose.yml`): ambos dependen de `db-init: service_completed_successfully`.

## 4. KPIs y dashboard

| Antes | Ahora | Archivo |
|---|---|---|
| «Engagement (comentarios por post)» = comentarios del día / posts del día, aunque los comentarios respondan a posts anteriores | Se llama **«Razón diaria comentarios / posts»** y su definición explica qué mide y qué no | `etl/kpis.py` |
| Tarjeta «Comentarios por post» = comentarios / posts del rango | **«Comentarios por post publicado»**: comentarios anidados en MongoDB de los posts publicados en el rango (LocalLLaMA 5.40 · wsb 1.17 · Skincare 0.57) | `dashboard/app.py`, `dashboard/consultas.py` |
| «Valor del período» = media de las medias diarias (sin ponderar) | Razón de sumas ponderada por volumen; mediana para el crecimiento; último valor acumulado para conexiones. El método aparece bajo cada valor | `dashboard/app.py` |
| El KPI de conexiones no indicaba que es global | Se indica en la definición y aparece un aviso cuando se filtra por comunidad | `etl/kpis.py`, `dashboard/app.py` |
| «Rechazados por motivo» mostraba siempre el total | Se filtra por rango y comunidad | `dashboard/consultas.py`, `dashboard/app.py` |
| Rangos sin posts (septiembre) dejaban tablas vacías | Muestran un aviso; la pestaña Red aclara que sus totales son del grafo completo | `dashboard/app.py` |

Efecto en los valores del informe (31-may → 31-ago, LocalLLaMA · wsb · Skincare): la tasa de eliminados
pasa de 2.2 · 1.2 · 1.2 % a 1.85 · 1.14 · 1.14 %, y el score medio de 8.2 · 4.3 · 1.3 a 8.53 · 4.25 · 1.31.
El resto cambia en centésimas.

## 5. Herramientas de verificación (nuevo)

| Archivo | Uso |
|---|---|
| `scripts/verificacion/conteos.py` | Conteos esperados desde los Parquet frente a las tres bases |
| `scripts/verificacion/foto.py` | Foto de IDs, relaciones, KPIs y actividad; compara antes y después (reproceso, CRUD) |
| `scripts/verificacion/correccion.py` | Prueba destructiva de reproceso con fuente corregida; se niega a correr sobre bases con datos |
| `scripts/verificacion/compose.prueba.yml` | Proyecto Compose aislado (volúmenes propios, sin puertos) para esa prueba |
| `dashboard/prueba_dashboard.py` | Ejecuta el dashboard sin navegador (`AppTest`) con varios filtros |

## 6. Documentación

- `README.md`: reproceso con datos corregidos, comando manual desde el 31-may, staging compartido entre
  el DAG y el comando manual, verificación y nuevos problemas conocidos.
- `VERIFICACION.md`: pruebas, comandos, resultados y lista de cambios para el informe PDF.
- `REVISION_INICIAL.md`: estado de los pendientes de la revisión anterior.
- `crud/crud_cassandra.py`: el texto de la demo identifica el KPI como razón diaria.

## Pendientes

1. Grabar el video demostrativo (20–25 min, con la participación de todos).
2. Actualizar el informe PDF según [VERIFICACION.md §7](VERIFICACION.md#7-cambios-que-debe-recoger-el-informe-técnico)
   y rehacer las capturas 4 y 8.
3. Revisar el dashboard en un navegador antes de rehacer las capturas.
4. Menores: el KPI acumulado de conexiones no se recalcula en los días posteriores a un reproceso
   (documentado); Streamlit avisa de que `use_container_width` está obsoleto; las dependencias
   transitivas del dashboard no están fijadas.
