"""Etapa 5 – Construcción del grafo en Neo4j.

(:Usuario)-[:PUBLICA]->(:Post)-[:EN]->(:Subreddit)
(:Usuario)-[:COMENTA]->(:Comentario)-[:PERTENECE_A]->(:Post)
(:Comentario)-[:RESPONDE_A]->(:Comentario)

Idempotencia: MERGE sobre las claves únicas (constraints) y sobre las relaciones.
Solo los autores con autor_tipo = 'usuario' se vuelven nodos Usuario (sin bots ni [deleted]).
RESPONDE_A se crea en una segunda pasada, cuando ya existen todos los comentarios del día;
si el padre fue descartado en la limpieza (p. ej. un bot) la relación no se crea.
"""
from etl.common import load_stage, log_run, neo4j_driver, records

BATCH = 1000

Q_POSTS = """
UNWIND $rows AS r
MERGE (p:Post {id: r.id})
SET p.titulo = r.title, p.score = r.score, p.num_comentarios = r.num_comments,
    p.upvote_ratio = r.upvote_ratio, p.flair = r.flair, p.nsfw = r.is_nsfw,
    p.subreddit = r.subreddit, p.fecha = date(r.fecha), p.creado = datetime({epochSeconds: r.created_utc})
WITH p, r
MATCH (s:Subreddit {nombre: r.subreddit})
MERGE (p)-[:EN]->(s)
WITH p, r WHERE r.autor_tipo = 'usuario'
MERGE (u:Usuario {nombre: r.author})
MERGE (u)-[:PUBLICA]->(p)
"""

Q_COMENTARIOS = """
UNWIND $rows AS r
MERGE (c:Comentario {id: r.id})
SET c.score = r.score, c.profundidad = r.depth, c.eliminado = r.eliminado, c.subreddit = r.subreddit,
    c.fecha = date(r.fecha), c.creado = datetime({epochSeconds: r.created_utc})
WITH c, r
MATCH (p:Post {id: r.link_id})
MERGE (c)-[:PERTENECE_A]->(p)
WITH c, r WHERE r.autor_tipo = 'usuario'
MERGE (u:Usuario {nombre: r.author})
MERGE (u)-[:COMENTA]->(c)
"""

Q_RESPUESTAS = """
UNWIND $rows AS r
MATCH (c:Comentario {id: r.id})
MATCH (padre:Comentario {id: r.parent_id})
MERGE (c)-[:RESPONDE_A]->(padre)
RETURN count(*) AS n
"""


def _lotes(rows):
    for i in range(0, len(rows), BATCH):
        yield rows[i:i + BATCH]


def run(fecha: str) -> dict:
    posts = load_stage(fecha, "posts_clean")
    comments = load_stage(fecha, "comments_clean")
    p_rows = records(posts[["id", "title", "score", "num_comments", "upvote_ratio", "flair", "is_nsfw", "subreddit",
                            "fecha", "created_utc", "author", "autor_tipo"]])
    c_rows = records(comments[["id", "link_id", "parent_id", "score", "depth", "eliminado", "subreddit", "fecha",
                               "created_utc", "author", "autor_tipo"]])
    r_rows = [{"id": r["id"], "parent_id": r["parent_id"]} for r in c_rows if r["depth"] > 0]

    respuestas = 0
    with neo4j_driver() as d:
        for lote in _lotes(p_rows):
            d.execute_query(Q_POSTS, rows=lote)
        for lote in _lotes(c_rows):
            d.execute_query(Q_COMENTARIOS, rows=lote)
        for lote in _lotes(r_rows):
            respuestas += d.execute_query(Q_RESPUESTAS, rows=lote).records[0]["n"]

    return log_run(fecha, "carga_neo4j", {
        "posts": len(p_rows),
        "comentarios": len(c_rows),
        "respuestas_enlazadas": respuestas,
        "respuestas_sin_padre": len(r_rows) - respuestas,
        "usuarios_en_lote": len({r["author"] for r in p_rows + c_rows if r["autor_tipo"] == "usuario"}),
    })
