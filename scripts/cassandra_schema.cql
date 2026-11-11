-- Esquema Cassandra del proyecto (idempotente: IF NOT EXISTS).
-- Diseño "query-first": cada tabla responde a UNA consulta del dashboard/KPIs.
-- Se desnormaliza a propósito (la misma métrica puede escribirse en 2 tablas).
-- Todas las escrituras del ETL son UPSERTs por clave primaria => re-ejecutar un día no duplica.

CREATE KEYSPACE IF NOT EXISTS reddit
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};  -- 1 nodo (entorno local)

-- Q1: "Actividad diaria de un subreddit entre dos fechas" (serie temporal principal).
--     Partición = subreddit, filas ordenadas por fecha descendente.
CREATE TABLE IF NOT EXISTS reddit.actividad_diaria_por_subreddit (
    subreddit               text,
    fecha                   date,
    posts                   int,
    comentarios             int,
    autores_unicos          int,
    score_posts             bigint,
    score_comentarios       bigint,
    avg_score_post          double,
    avg_upvote_ratio        double,
    posts_sin_comentarios   int,
    comentarios_eliminados  int,
    profundidad_max         int,
    profundidad_media       double,
    actualizado_en          timestamp,
    PRIMARY KEY ((subreddit), fecha)
) WITH CLUSTERING ORDER BY (fecha DESC);

-- Q2: "Resumen de todos los subreddits para una fecha dada" (vista del día).
CREATE TABLE IF NOT EXISTS reddit.actividad_diaria_por_fecha (
    fecha           date,
    subreddit       text,
    posts           int,
    comentarios     int,
    autores_unicos  int,
    actualizado_en  timestamp,
    PRIMARY KEY ((fecha), subreddit)
);

-- Q3: "Distribución por hora (UTC) de la actividad de un subreddit en un día" (heatmap).
CREATE TABLE IF NOT EXISTS reddit.actividad_por_hora (
    subreddit    text,
    fecha        date,
    hora         tinyint,
    posts        int,
    comentarios  int,
    PRIMARY KEY ((subreddit, fecha), hora)
) WITH CLUSTERING ORDER BY (hora ASC);

-- Q4: "Top autores más activos de un subreddit en un día" (ranking ya ordenado).
--     El ETL borra la partición (subreddit, fecha) antes de reinsertar => idempotente
--     aunque `actividad` forme parte de la clave.
CREATE TABLE IF NOT EXISTS reddit.top_autores_por_dia (
    subreddit    text,
    fecha        date,
    actividad    int,      -- posts + comentarios del autor ese día
    autor        text,
    posts        int,
    comentarios  int,
    score_total  bigint,
    PRIMARY KEY ((subreddit, fecha), actividad, autor)
) WITH CLUSTERING ORDER BY (actividad DESC, autor ASC);

-- Q5: "Serie temporal de un KPI para un subreddit (o 'ALL')".
CREATE TABLE IF NOT EXISTS reddit.kpis_por_nombre (
    kpi        text,
    subreddit  text,       -- 'ALL' = global
    fecha      date,
    valor      double,
    PRIMARY KEY ((kpi, subreddit), fecha)
) WITH CLUSTERING ORDER BY (fecha DESC);

-- Q6: "Todos los KPIs calculados para una fecha" (tarjetas del dashboard).
CREATE TABLE IF NOT EXISTS reddit.kpis_por_fecha (
    fecha      date,
    kpi        text,
    subreddit  text,
    valor      double,
    PRIMARY KEY ((fecha), kpi, subreddit)
);
