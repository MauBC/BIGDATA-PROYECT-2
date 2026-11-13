"""Etapa 5 – Construcción del grafo en Neo4j.

(:Usuario)-[:PUBLICA]->(:Post)-[:EN]->(:Subreddit)
(:Usuario)-[:COMENTA]->(:Comentario)-[:PERTENECE_A]->(:Post)
(:Comentario)-[:RESPONDE_A]->(:Comentario)

Idempotencia: MERGE sobre las claves únicas (constraints) y sobre las relaciones. Cada ejecución
reemplaza la porción del día: elimina los Post/Comentario de esa fecha que ya no son válidos,
rehace las relaciones de autoría y pertenencia de los que siguen (por si cambió el autor) y
borra los Usuario que quedan sin relaciones.
Solo los autores con autor_tipo = 'usuario' se vuelven nodos Usuario (sin bots ni [deleted]).
Cada Comentario guarda post_id y parent_id, de modo que los enlaces se crean en los dos sentidos:
del comentario a su post/padre ya cargado y de los comentarios ya cargados al post/padre que
llega después (p. ej. un padre que deja de ser rechazado al corregir la fuente).
Si el padre fue descartado en la limpieza (p. ej. un bot) la relación RESPONDE_A no se crea.
"""
from etl.common import load_stage, log_run, neo4j_driver, records

BATCH = 1000

Q_BORRAR_OBSOLETOS = """
CALL () { MATCH (p:Post {fecha: date($fecha)}) WHERE NOT p.id IN $post_ids DETACH DELETE p RETURN count(*) AS a }
CALL () { MATCH (c:Comentario {fecha: date($fecha)}) WHERE NOT c.id IN $comment_ids DETACH DELETE c RETURN count(*) AS b }
RETURN a + b AS n
"""

Q_POSTS = """
UNWIND $rows AS r
MERGE (p:Post {id: r.id})
SET p.titulo = r.title, p.score = r.score, p.num_comentarios = r.num_comments,
    p.upvote_ratio = r.upvote_ratio, p.flair = r.flair, p.nsfw = r.is_nsfw,
    p.subreddit = r.subreddit, p.fecha = date(r.fecha), p.creado = datetime({epochSeconds: r.created_utc})
WITH p, r
CALL (p) { MATCH (p)<-[old:PUBLICA]-() DELETE old }
CALL (p) { MATCH (p)-[old:EN]->() DELETE old }
MATCH (s:Subreddit {nombre: r.subreddit})
MERGE (p)-[:EN]->(s)
WITH p, r
CALL (p) { MATCH (c:Comentario {post_id: p.id}) MERGE (c)-[:PERTENECE_A]->(p) }
WITH p, r WHERE r.autor_tipo = 'usuario'
MERGE (u:Usuario {nombre: r.author})
MERGE (u)-[:PUBLICA]->(p)
"""

Q_COMENTARIOS = """
UNWIND $rows AS r
MERGE (c:Comentario {id: r.id})
SET c.score = r.score, c.profundidad = r.depth, c.eliminado = r.eliminado, c.subreddit = r.subreddit,
    c.fecha = date(r.fecha), c.creado = datetime({epochSeconds: r.created_utc}),
    c.post_id = r.link_id, c.parent_id = r.parent_id
WITH c, r
CALL (c) { MATCH (c)<-[old:COMENTA]-() DELETE old }
CALL (c) { MATCH (c)-[old:PERTENECE_A|RESPONDE_A]->() DELETE old }
CALL (c, r) { MATCH (p:Post {id: r.link_id}) MERGE (c)-[:PERTENECE_A]->(p) }
WITH c, r WHERE r.autor_tipo = 'usuario'
MERGE (u:Usuario {nombre: r.author})
MERGE (u)-[:COMENTA]->(c)
"""

# Segunda pasada, con todos los comentarios del día ya creados: enlaza cada respuesta con su padre
# y cada comentario del día con las respuestas ya cargadas que lo esperaban.
Q_RESPUESTAS = """
UNWIND $ids AS id
MATCH (c:Comentario {id: id})
CALL (c) { MATCH (padre:Comentario {id: c.parent_id}) MERGE (c)-[:RESPONDE_A]->(padre) }
CALL (c) { MATCH (hijo:Comentario {parent_id: c.id}) MERGE (hijo)-[:RESPONDE_A]->(c) }
RETURN count(*) AS n
"""

Q_ENLAZADAS = """
UNWIND $ids AS id
MATCH (c:Comentario {id: id})-[:RESPONDE_A]->()
RETURN count(*) AS n
"""

Q_USUARIOS_HUERFANOS = "MATCH (u:Usuario) WHERE NOT (u)--() DELETE u RETURN count(*) AS n"


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
    c_ids = [r["id"] for r in c_rows]
    r_ids = [r["id"] for r in c_rows if r["depth"] > 0]

    respuestas = 0
    with neo4j_driver() as d:
        obsoletos = d.execute_query(Q_BORRAR_OBSOLETOS, fecha=fecha, post_ids=[r["id"] for r in p_rows],
                                    comment_ids=c_ids).records[0]["n"]
        for lote in _lotes(p_rows):
            d.execute_query(Q_POSTS, rows=lote)
        for lote in _lotes(c_rows):
            d.execute_query(Q_COMENTARIOS, rows=lote)
        for lote in _lotes(c_ids):
            d.execute_query(Q_RESPUESTAS, ids=lote)
        for lote in _lotes(r_ids):
            respuestas += d.execute_query(Q_ENLAZADAS, ids=lote).records[0]["n"]
        huerfanos = d.execute_query(Q_USUARIOS_HUERFANOS).records[0]["n"]

    return log_run(fecha, "carga_neo4j", {
        "posts": len(p_rows),
        "comentarios": len(c_rows),
        "respuestas_enlazadas": respuestas,
        "respuestas_sin_padre": len(r_ids) - respuestas,
        "usuarios_en_lote": len({r["author"] for r in p_rows + c_rows if r["autor_tipo"] == "usuario"}),
        "nodos_obsoletos_eliminados": obsoletos,
        "usuarios_huerfanos_eliminados": huerfanos,
    })
