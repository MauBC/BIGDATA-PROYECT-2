"""Capa de acceso a datos del dashboard: una función por consulta, cacheadas 5 min.

Cada base se usa para lo que su modelo hace mejor:
- Cassandra: series temporales y KPIs por fecha (lecturas por partición, sin agregaciones).
- MongoDB  : documentos y agregaciones sobre los posts con comentarios anidados.
- Neo4j    : métricas de red (conexiones entre usuarios, centralidad).
"""
import datetime as dt

import pandas as pd
import streamlit as st

from etl.common import cassandra_session, mongo_db, neo4j_driver
from etl.config import SUBREDDITS

TTL = 300


# ---------------- Cassandra ----------------
@st.cache_data(ttl=TTL)
def actividad_diaria(desde: dt.date, hasta: dt.date) -> pd.DataFrame:
    rows = []
    with cassandra_session() as s:
        stmt = s.prepare("SELECT * FROM actividad_diaria_por_subreddit WHERE subreddit = ? AND fecha >= ? AND fecha <= ?")
        for sub in SUBREDDITS:
            rows += [r._asdict() for r in s.execute(stmt, (sub, desde, hasta))]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"].astype(str))
    return df


@st.cache_data(ttl=TTL)
def kpi_series(kpis: tuple, desde: dt.date, hasta: dt.date) -> pd.DataFrame:
    rows = []
    with cassandra_session() as s:
        stmt = s.prepare("SELECT kpi, subreddit, fecha, valor FROM kpis_por_nombre WHERE kpi = ? AND subreddit = ? "
                         "AND fecha >= ? AND fecha <= ?")
        for kpi in kpis:
            for sub in ["ALL", *SUBREDDITS]:
                rows += [r._asdict() for r in s.execute(stmt, (kpi, sub, desde, hasta))]
    df = pd.DataFrame(rows, columns=["kpi", "subreddit", "fecha", "valor"])
    if not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"].astype(str))
    return df


@st.cache_data(ttl=TTL)
def actividad_por_hora(subreddits: tuple, desde: dt.date, hasta: dt.date) -> pd.DataFrame:
    rows = []
    with cassandra_session() as s:
        stmt = s.prepare("SELECT subreddit, fecha, hora, posts, comentarios FROM actividad_por_hora WHERE subreddit = ? AND fecha = ?")
        futures = []
        d = desde
        while d <= hasta:
            futures += [s.execute_async(stmt, (sub, d)) for sub in subreddits]
            d += dt.timedelta(days=1)
        for f in futures:
            rows += [r._asdict() for r in f.result()]
    df = pd.DataFrame(rows, columns=["subreddit", "fecha", "hora", "posts", "comentarios"])
    if not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"].astype(str))
    return df


@st.cache_data(ttl=TTL)
def top_autores(subreddit: str, fecha: dt.date) -> pd.DataFrame:
    with cassandra_session() as s:
        rows = s.execute("SELECT autor, actividad, posts, comentarios, score_total FROM top_autores_por_dia "
                         "WHERE subreddit = %s AND fecha = %s LIMIT 10", (subreddit, fecha))
        return pd.DataFrame([r._asdict() for r in rows])


# ---------------- MongoDB ----------------
@st.cache_data(ttl=TTL)
def top_posts(subreddits: tuple, desde: str, hasta: str, n: int = 10) -> pd.DataFrame:
    with mongo_db() as db:
        cur = db.posts.aggregate([
            {"$match": {"subreddit": {"$in": list(subreddits)}, "fecha": {"$gte": desde, "$lte": hasta}}},
            {"$project": {"_id": 0, "subreddit": 1, "fecha": 1, "title": 1, "score": 1,
                          "comentarios": {"$size": "$comments"},
                          "profundidad_max": {"$ifNull": [{"$max": "$comments.depth"}, 0]}}},
            {"$sort": {"comentarios": -1, "score": -1}}, {"$limit": n}])
        return pd.DataFrame(list(cur))


@st.cache_data(ttl=TTL)
def conversacion_por_post(subreddits: tuple, desde: str, hasta: str) -> dict:
    """Posts publicados en el rango y comentarios anidados que recibieron (hasta el corte del dataset)."""
    with mongo_db() as db:
        r = next(db.posts.aggregate([
            {"$match": {"subreddit": {"$in": list(subreddits)}, "fecha": {"$gte": desde, "$lte": hasta}}},
            {"$group": {"_id": None, "posts": {"$sum": 1}, "comentarios": {"$sum": {"$size": "$comments"}}}}]), None)
    return {"posts": r["posts"], "comentarios": r["comentarios"]} if r else {"posts": 0, "comentarios": 0}


@st.cache_data(ttl=TTL)
def flairs(subreddits: tuple, desde: str, hasta: str) -> pd.DataFrame:
    with mongo_db() as db:
        cur = db.posts.aggregate([
            {"$match": {"subreddit": {"$in": list(subreddits)}, "fecha": {"$gte": desde, "$lte": hasta},
                        "flair": {"$ne": None}}},
            {"$group": {"_id": {"subreddit": "$subreddit", "flair": "$flair"}, "posts": {"$sum": 1}}},
            {"$sort": {"posts": -1}}])
        return pd.DataFrame([{**r["_id"], "posts": r["posts"]} for r in cur])


@st.cache_data(ttl=TTL)
def distribucion_profundidad(subreddits: tuple, desde: str, hasta: str) -> pd.DataFrame:
    with mongo_db() as db:
        cur = db.posts.aggregate([
            {"$match": {"subreddit": {"$in": list(subreddits)}}},
            {"$unwind": "$comments"},
            {"$match": {"comments.fecha": {"$gte": desde, "$lte": hasta}}},
            {"$group": {"_id": {"subreddit": "$subreddit", "depth": "$comments.depth"}, "comentarios": {"$sum": 1}}}])
        return pd.DataFrame([{**r["_id"], "comentarios": r["comentarios"]} for r in cur])


@st.cache_data(ttl=TTL)
def etl_runs() -> pd.DataFrame:
    with mongo_db() as db:
        rows = []
        for r in db.etl_runs.find().sort("_id", 1):
            e = r.get("etapas", {})
            rows.append({
                "fecha": r["_id"],
                "posts_extraidos": e.get("extraccion", {}).get("posts"),
                "comentarios_extraidos": e.get("extraccion", {}).get("comentarios"),
                "posts_validos": e.get("limpieza", {}).get("posts_validos"),
                "comentarios_validos": e.get("limpieza", {}).get("comentarios_validos"),
                "rechazados": e.get("limpieza", {}).get("rechazados"),
                "usuario_mas_conectado": (e.get("kpis", {}).get("usuario_mas_conectado") or {}).get("usuario"),
                "ultima_ejecucion": r.get("actualizado_en"),
            })
        return pd.DataFrame(rows)


@st.cache_data(ttl=TTL)
def rechazados_por_motivo(subreddits: tuple, desde: str, hasta: str) -> pd.DataFrame:
    with mongo_db() as db:
        cur = db.rechazados.aggregate([
            {"$match": {"subreddit": {"$in": list(subreddits)}, "fecha": {"$gte": desde, "$lte": hasta}}},
            {"$group": {"_id": {"motivo": "$motivo", "subreddit": "$subreddit"}, "n": {"$sum": 1}}}])
        return pd.DataFrame([{**r["_id"], "registros": r["n"]} for r in cur])


# ---------------- Neo4j ----------------
Q_CONEXIONES = """
CALL () {
  MATCH (u:Usuario)-[:COMENTA]->(c:Comentario)-[:RESPONDE_A]->(:Comentario)<-[:COMENTA]-(v:Usuario)
  WHERE c.fecha >= date($desde) AND c.fecha <= date($hasta) AND c.subreddit IN $subs AND u <> v
  RETURN u, v
  UNION
  MATCH (u:Usuario)-[:COMENTA]->(c:Comentario {profundidad: 0})-[:PERTENECE_A]->(:Post)<-[:PUBLICA]-(v:Usuario)
  WHERE c.fecha >= date($desde) AND c.fecha <= date($hasta) AND c.subreddit IN $subs AND u <> v
  RETURN u, v
}
UNWIND [[u, v], [v, u]] AS par
WITH par[0] AS a, par[1] AS b
RETURN a.nombre AS usuario, count(DISTINCT b) AS conexiones
ORDER BY conexiones DESC, usuario ASC LIMIT $n
"""


@st.cache_data(ttl=TTL)
def usuarios_mas_conectados(subreddits: tuple, desde: str, hasta: str, n: int = 10) -> pd.DataFrame:
    with neo4j_driver() as d:
        recs = d.execute_query(Q_CONEXIONES, subs=list(subreddits), desde=desde, hasta=hasta, n=n).records
    return pd.DataFrame([r.data() for r in recs], columns=["usuario", "conexiones"])


@st.cache_data(ttl=TTL)
def red_ego(usuario: str, desde: str, hasta: str, limite: int = 40) -> pd.DataFrame:
    """Aristas usuario -> usuario (quién respondió a quién) alrededor de un usuario."""
    q = """
    MATCH (a:Usuario)-[:COMENTA]->(c:Comentario)-[:RESPONDE_A]->(:Comentario)<-[:COMENTA]-(b:Usuario)
    WHERE (a.nombre = $u OR b.nombre = $u) AND a <> b AND c.fecha >= date($desde) AND c.fecha <= date($hasta)
    RETURN a.nombre AS origen, b.nombre AS destino, count(*) AS respuestas
    UNION ALL
    MATCH (a:Usuario)-[:COMENTA]->(c:Comentario {profundidad: 0})-[:PERTENECE_A]->(:Post)<-[:PUBLICA]-(b:Usuario)
    WHERE (a.nombre = $u OR b.nombre = $u) AND a <> b AND c.fecha >= date($desde) AND c.fecha <= date($hasta)
    RETURN a.nombre AS origen, b.nombre AS destino, count(*) AS respuestas
    """
    with neo4j_driver() as d:
        recs = d.execute_query(q, u=usuario, desde=desde, hasta=hasta).records
    df = pd.DataFrame([r.data() for r in recs], columns=["origen", "destino", "respuestas"])
    if df.empty:
        return df
    df = df.groupby(["origen", "destino"], as_index=False)["respuestas"].sum()
    return df.sort_values("respuestas", ascending=False).head(limite)


@st.cache_data(ttl=TTL)
def resumen_grafo() -> dict:
    with neo4j_driver() as d:
        nodos = {r["l"]: r["n"] for r in d.execute_query("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS n").records}
        rels = {r["t"]: r["n"] for r in d.execute_query("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS n").records}
    return {"nodos": nodos, "relaciones": rels}
