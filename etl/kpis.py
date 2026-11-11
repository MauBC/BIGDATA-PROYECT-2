"""Etapa 6 – Generación automática de indicadores (KPIs).

Los KPIs se calculan por subreddit y globales ('ALL') y se guardan en Cassandra
(kpis_por_nombre y kpis_por_fecha). El resumen del día queda en MongoDB etl_runs.
KPI_DEFS es la fuente única de definición/fórmula/interpretación (la usa también el dashboard).
"""
import datetime as dt

import pandas as pd

from etl.common import cassandra_session, load_stage, log_run, neo4j_driver
from etl.config import SUBREDDITS

KPI_DEFS = {
    "registros_procesados": {
        "nombre": "Registros procesados por día",
        "definicion": "Cantidad de posts y comentarios válidos (tras la limpieza) cargados en el día.",
        "formula": "posts_validos(d) + comentarios_validos(d)",
        "interpretacion": "Mide el volumen que maneja el pipeline; caídas bruscas pueden indicar huecos en la fuente.",
    },
    "crecimiento_actividad_pct": {
        "nombre": "Crecimiento diario de la actividad (%)",
        "definicion": "Variación porcentual de la actividad (posts + comentarios) respecto al día anterior.",
        "formula": "(A(d) - A(d-1)) / A(d-1) × 100,  A = posts + comentarios",
        "interpretacion": "Valores positivos indican días de mayor actividad; picos señalan eventos o noticias.",
    },
    "comentarios_por_post": {
        "nombre": "Engagement (comentarios por post)",
        "definicion": "Comentarios publicados en el día por cada post publicado ese mismo día.",
        "formula": "comentarios(d) / posts(d)",
        "interpretacion": "Cuánta conversación genera la comunidad; mayor valor = comunidad más participativa.",
    },
    "actividad_por_usuario": {
        "nombre": "Promedio de actividad por usuario",
        "definicion": "Publicaciones (posts + comentarios) promedio por autor humano activo en el día.",
        "formula": "(posts_usuarios(d) + comentarios_usuarios(d)) / autores_unicos(d)",
        "interpretacion": "Valores altos = pocos usuarios muy activos; cercano a 1 = participación dispersa.",
    },
    "tasa_eliminados_pct": {
        "nombre": "Tasa de comentarios eliminados (%)",
        "definicion": "Porcentaje de comentarios del día cuyo cuerpo es [deleted] o [removed].",
        "formula": "comentarios_eliminados(d) / comentarios(d) × 100",
        "interpretacion": "Aproxima la moderación o la conflictividad del subreddit.",
    },
    "profundidad_media": {
        "nombre": "Profundidad media de los hilos",
        "definicion": "Nivel promedio de anidamiento de los comentarios del día (0 = respuesta directa al post).",
        "formula": "Σ depth(c) / comentarios(d)",
        "interpretacion": "Más profundidad = conversaciones más largas entre usuarios (debate).",
    },
    "score_medio_post": {
        "nombre": "Score medio por post",
        "definicion": "Puntuación (upvotes - downvotes) promedio de los posts publicados en el día.",
        "formula": "Σ score(p) / posts(d)",
        "interpretacion": "Calidad percibida del contenido publicado ese día.",
    },
    "max_conexiones_usuario": {
        "nombre": "Usuario con mayor número de conexiones",
        "definicion": "Máximo de usuarios distintos con los que un usuario interactuó (respondió o le respondieron), acumulado hasta el día. Calculado en Neo4j.",
        "formula": "max_u |{v ≠ u : u respondió a v, o v respondió a u}|",
        "interpretacion": "Identifica al usuario más central de la red; mide la concentración de la conversación.",
    },
}

Q_CONEXIONES = """
CALL () {
  MATCH (u:Usuario)-[:COMENTA]->(c:Comentario)-[:RESPONDE_A]->(:Comentario)<-[:COMENTA]-(v:Usuario)
  WHERE c.fecha <= date($fecha) AND u <> v
  RETURN u, v
  UNION
  MATCH (u:Usuario)-[:COMENTA]->(c:Comentario {profundidad: 0})-[:PERTENECE_A]->(:Post)<-[:PUBLICA]-(v:Usuario)
  WHERE c.fecha <= date($fecha) AND u <> v
  RETURN u, v
}
UNWIND [[u, v], [v, u]] AS par
WITH par[0] AS a, par[1] AS b
RETURN a.nombre AS usuario, count(DISTINCT b) AS conexiones
ORDER BY conexiones DESC, usuario ASC LIMIT 1
"""


def _kpis_basicos(posts: pd.DataFrame, comments: pd.DataFrame) -> dict:
    n_p, n_c = len(posts), len(comments)
    up = posts[posts["autor_tipo"] == "usuario"]
    uc = comments[comments["autor_tipo"] == "usuario"]
    autores = pd.concat([up["author"], uc["author"]]).nunique()
    k = {"registros_procesados": float(n_p + n_c)}
    if n_p:
        k["comentarios_por_post"] = n_c / n_p
        k["score_medio_post"] = float(posts["score"].mean())
    if autores:
        k["actividad_por_usuario"] = (len(up) + len(uc)) / autores
    if n_c:
        k["tasa_eliminados_pct"] = float(comments["eliminado"].mean() * 100)
        k["profundidad_media"] = float(comments["depth"].mean())
    return k


def _actividad_previa(s, dia: dt.date) -> dict:
    prev = dia - dt.timedelta(days=1)
    rows = s.execute("SELECT subreddit, posts, comentarios FROM actividad_diaria_por_fecha WHERE fecha = %s", (prev,))
    act = {r.subreddit: (r.posts or 0) + (r.comentarios or 0) for r in rows}
    if act:
        act["ALL"] = sum(act.values())
    return act


def run(fecha: str) -> dict:
    posts = load_stage(fecha, "posts_clean")
    comments = load_stage(fecha, "comments_clean")
    dia = dt.date.fromisoformat(fecha)

    grupos = {"ALL": (posts, comments)}
    grupos.update({s: (posts[posts["subreddit"] == s], comments[comments["subreddit"] == s]) for s in SUBREDDITS})
    kpis = {sub: _kpis_basicos(p, c) for sub, (p, c) in grupos.items()}

    with neo4j_driver() as d:
        rec = d.execute_query(Q_CONEXIONES, fecha=fecha).records
    top_usuario = {"usuario": rec[0]["usuario"], "conexiones": rec[0]["conexiones"]} if rec else None
    if top_usuario:
        kpis["ALL"]["max_conexiones_usuario"] = float(top_usuario["conexiones"])

    with cassandra_session() as s:
        previa = _actividad_previa(s, dia)
        for sub, (p, c) in grupos.items():
            hoy = len(p) + len(c)
            if previa.get(sub):
                kpis[sub]["crecimiento_actividad_pct"] = (hoy - previa[sub]) / previa[sub] * 100

        ins_nombre = s.prepare("INSERT INTO kpis_por_nombre (kpi, subreddit, fecha, valor) VALUES (?,?,?,?)")
        ins_fecha = s.prepare("INSERT INTO kpis_por_fecha (fecha, kpi, subreddit, valor) VALUES (?,?,?,?)")
        for sub, valores in kpis.items():
            for kpi, valor in valores.items():
                s.execute(ins_nombre, (kpi, sub, dia, valor))
                s.execute(ins_fecha, (dia, kpi, sub, valor))

    redondeo = {sub: {k: round(v, 3) for k, v in vals.items()} for sub, vals in kpis.items()}
    return log_run(fecha, "kpis", {"kpis": redondeo, "usuario_mas_conectado": top_usuario})
