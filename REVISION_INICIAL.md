# Revisión inicial — 10 de noviembre de 2026

## Contenido recibido

- 22 archivos Python en ETL, DAG, scripts, CRUD y dashboard.
- Docker Compose con MongoDB 7, Cassandra 4.1, Neo4j 5.26, Airflow 2.10.5, Postgres y Streamlit.
- DAG diario con seis etapas, reintentos y simulación incremental histórica.
- Esquemas, índices y restricciones de las tres bases; demos CRUD por base.
- Ocho KPIs con definición, fórmula e interpretación; dashboard con cinco pestañas.
- Informe técnico de 14 páginas con modelos, resultados declarados y capturas.
- Dataset fuente: ocho Parquet, documentación, ejemplos y estadísticas. Hay además 105 particiones de staging locales, excluidas de Git por ser regenerables.

## Dataset y atribución

Fuente: https://huggingface.co/datasets/mashu-data/reddit-comments-sample

Autor/proveedor: Mashu Data. Licencia declarada: CC BY-NC 4.0, con las salvedades sobre derechos de terceros descritas en `data/raw/README.md`. La licencia del dataset no se extiende automáticamente al código del proyecto.

43,913 posts + 59,874 comentarios = 103,787 registros de LocalLLaMA, wallstreetbets y SkincareAddiction. Los posts cubren del 31 de mayo al 31 de agosto de 2026 (UTC); hay comentarios hasta el 12 de septiembre. La muestra no es representativa ni un archivo completo de las comunidades.

La revisión de datos declarada en `data/raw/stats/summary.json` es `28f3a79a323cbb994f55c04481a1c87551f5c315`. Los ocho SHA-256 de ese manifiesto coinciden con los archivos locales. No hace falta volver a descargar el dataset.

## Validaciones realizadas

- Análisis sintáctico de los 22 archivos Python: correcto.
- `docker compose config --quiet`: correcto con la configuración local existente.
- Lectura de los ocho Parquet y comparación de sus checksums: correcta.
- Ejecución local de `clean_posts` y `clean_comments` sobre el dataset completo: 43,913 posts válidos, 49,521 comentarios válidos y 10,353 rechazos (bots), coherentes con el informe.
- Se detectan 46 respuestas cuyo padre fue excluido como bot, limitación ya descrita por el código y el informe. No se encontraron padres conservados con fecha UTC posterior a la respuesta.
- Búsqueda de patrones habituales de tokens y claves privadas en texto: sin hallazgos confirmados. No constituye una auditoría exhaustiva. `.env` se excluye; se proporciona `.env.example` con valores de desarrollo ya documentados.

## Ajustes mínimos antes de versionar

- Exclusión de credenciales locales, cachés, bytecode y staging.
- Plantilla de variables y paso de configuración en el README.
- Corrección de la ruta al informe en el README.
- Corrección del valor inicial del selector de ranking cuando todo el rango está en septiembre (antes podía quedar por debajo de la fecha mínima).
- Exclusión de `.git` y `.env` del contexto de construcción Docker.
- Los pequeños Parquet fuente se versionan directamente, sin depender de Git LFS.

## Comparación con el enunciado

La estructura implementa los componentes solicitados: fuente social de más de 50,000 registros, MongoDB/Cassandra/Neo4j, DAG de seis etapas programado, CRUD de las tres bases, dashboard y al menos cinco KPIs (hay ocho). El informe incluye las secciones y capturas requeridas. La programación está acotada al backfill histórico: simula incrementos, no descarga datos nuevos periódicamente.

El enunciado fija la entrega para el 17 de noviembre de 2026 y solicita además un video de 20–25 minutos con participación de todos y evaluación individual de pares. No se encontró un video en los archivos recibidos; la evaluación de pares es una actividad externa.

## Pendientes antes de afirmar que todo funciona

1. Arrancar Docker Desktop: el cliente existe, pero el motor Linux no estaba disponible durante esta revisión. No se ejecutaron contenedores, CRUD, consultas reales ni el DAG completo.
2. Reproducir desde bases nuevas el backfill, verificar conteos y reprocesar un día. Las afirmaciones del PDF sobre ejecuciones exitosas e idempotencia son evidencia recibida, no resultados reproducidos en esta revisión.
3. Revisar idempotencia ante cambios de datos: MongoDB/Neo4j no eliminan automáticamente registros antes válidos que luego sean rechazados; KPIs omitidos en una nueva corrida pueden dejar valores antiguos en Cassandra. Repetir exactamente la misma fuente es un caso más limitado.
4. Revisar las interpretaciones analíticas: comentarios del día/posts del día no mide una cohorte de publicaciones; porcentajes y promedios del dashboard se promedian por día/comunidad sin ponderación. El KPI de conexiones acumuladas usa el histórico global.
5. Verificar construcción de imágenes y versiones efectivas de dependencias de Airflow, además de conectividad y orden de inicialización de los servicios.
6. Verificar visualmente el dashboard, revisar el informe tras las mejoras y preparar el video grupal. El PDF se leyó por extracción de texto; no se certificó su maquetación visual.

Esta entrega es una base inicial para continuar el desarrollo; no una certificación de funcionamiento integral.

## Actualización — 13 de noviembre de 2026

Detalle, comandos y evidencia en [VERIFICACION.md](VERIFICACION.md). Estado de los pendientes anteriores:

1. ✅ Docker arrancado; instalación limpia, contenedores, CRUD, consultas y DAG completo ejecutados.
2. ✅ Backfill reproducido desde bases vacías (105/105 ejecuciones); conteos iguales a la fuente y al informe;
   reproceso del 15-jul sin diferencias en IDs, relaciones ni KPIs.
3. ✅ Corregido: cada ejecución reemplaza la porción de su día en MongoDB, Neo4j, `rechazados` y KPIs, y
   recalcula el crecimiento del día siguiente. Los efectos sobre otros días se resuelven reprocesándolos.
4. ✅ Corregido: KPI renombrado como razón diaria, nueva métrica de comentarios por post publicado
   (MongoDB), valores del período ponderados y aviso de KPI global.
5. ✅ Imágenes y versiones efectivas verificadas; el scheduler y el dashboard ahora esperan a `db-init`.
6. ⚠️ Dashboard verificado con `AppTest` y contra las bases, pero no a la vista. Quedan por hacer el
   informe actualizado (lista en VERIFICACION.md §7) y el video grupal.

Además se corrigió la pérdida silenciosa de comentarios cuando se procesaba un día antes que el de sus
posts (el comando manual del README empezaba el 01-jun).
