"""CRUD en Neo4j (nodos Usuario/Post/Comentario y sus relaciones).

Uso:  docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_neo4j
La demo crea nodos con prefijo 'demo_' y los elimina al final.
"""
from crud.common import mostrar, paso, titulo
from etl.common import neo4j_driver


def q(d, cypher, **params):
    return [r.data() for r in d.execute_query(cypher, **params).records]


# ---------------- operaciones ----------------
def crear_usuario(d, nombre):
    return q(d, "MERGE (u:Usuario {nombre: $n}) RETURN u.nombre AS usuario", n=nombre)


def crear_post(d, post_id, titulo_post, subreddit):
    return q(d, """MERGE (p:Post {id: $id}) SET p.titulo = $t, p.score = 1, p.fecha = date()
                   WITH p MATCH (s:Subreddit {nombre: $s}) MERGE (p)-[:EN]->(s)
                   RETURN p.id AS post, s.nombre AS subreddit""", id=post_id, t=titulo_post, s=subreddit)


def crear_relacion_publica(d, usuario, post_id):
    return q(d, """MATCH (u:Usuario {nombre: $u}), (p:Post {id: $p}) MERGE (u)-[r:PUBLICA]->(p)
                   RETURN u.nombre AS usuario, type(r) AS relacion, p.id AS post""", u=usuario, p=post_id)


def crear_comentario(d, comment_id, autor, post_id, responde_a=None):
    cypher = """MATCH (u:Usuario {nombre: $u}), (p:Post {id: $p})
                MERGE (c:Comentario {id: $c}) SET c.score = 1, c.fecha = date(), c.profundidad = $prof
                MERGE (u)-[:COMENTA]->(c) MERGE (c)-[:PERTENECE_A]->(p)
                WITH c OPTIONAL MATCH (padre:Comentario {id: $padre})
                FOREACH (_ IN CASE WHEN padre IS NULL THEN [] ELSE [1] END | MERGE (c)-[:RESPONDE_A]->(padre))
                RETURN c.id AS comentario"""
    return q(d, cypher, u=autor, p=post_id, c=comment_id, padre=responde_a, prof=0 if responde_a is None else 1)


def leer_subgrafo(d, post_id):
    return q(d, """MATCH (p:Post {id: $p})-[:EN]->(s:Subreddit)
                   OPTIONAL MATCH (autor:Usuario)-[:PUBLICA]->(p)
                   OPTIONAL MATCH (u:Usuario)-[:COMENTA]->(c:Comentario)-[:PERTENECE_A]->(p)
                   OPTIONAL MATCH (c)-[:RESPONDE_A]->(padre:Comentario)
                   RETURN p.titulo AS titulo, p.score AS score, s.nombre AS subreddit, autor.nombre AS autor,
                          collect({comentario: c.id, por: u.nombre, responde_a: padre.id}) AS comentarios""", p=post_id)


def actualizar_post(d, post_id, **props):
    return q(d, "MATCH (p:Post {id: $id}) SET p += $props RETURN p.id AS post, p.titulo AS titulo, p.score AS score, p.flair AS flair",
             id=post_id, props=props)


def eliminar_relacion(d, comment_id):
    return q(d, "MATCH (:Comentario {id: $c})-[r:RESPONDE_A]->() DELETE r RETURN count(r) AS eliminadas", c=comment_id)


def eliminar_nodos(d, prefijo):
    return q(d, """MATCH (n) WHERE (n:Usuario AND n.nombre STARTS WITH $p) OR ((n:Post OR n:Comentario) AND n.id STARTS WITH $p)
                   DETACH DELETE n RETURN count(n) AS eliminados""", p=prefijo)


# ---------------- demo ----------------
def main():
    with neo4j_driver() as d:
        titulo("CRUD Neo4j – grafo Usuario / Post / Comentario / Subreddit")
        eliminar_nodos(d, "demo_")

        paso("CREATE nodo", "crear dos usuarios y un post en LocalLLaMA")
        mostrar(crear_usuario(d, "demo_ana") + crear_usuario(d, "demo_luis"))
        mostrar(crear_post(d, "demo_p1", "Post de demostración CRUD", "LocalLLaMA"))

        paso("CREATE relación", "(:Usuario)-[:PUBLICA]->(:Post) y comentarios con COMENTA / PERTENECE_A / RESPONDE_A")
        mostrar(crear_relacion_publica(d, "demo_ana", "demo_p1"))
        crear_comentario(d, "demo_c1", "demo_luis", "demo_p1")
        crear_comentario(d, "demo_c2", "demo_ana", "demo_p1", responde_a="demo_c1")

        paso("READ", "leer el subgrafo del post")
        mostrar(leer_subgrafo(d, "demo_p1"))

        paso("READ", "consulta sobre datos reales: 5 usuarios con más comentarios")
        mostrar(q(d, """MATCH (u:Usuario)-[:COMENTA]->(c) RETURN u.nombre AS usuario, count(c) AS comentarios
                        ORDER BY comentarios DESC LIMIT 5"""))

        paso("UPDATE", "actualizar propiedades del post (score, flair)")
        mostrar(actualizar_post(d, "demo_p1", score=100, flair="Demo"))

        paso("DELETE relación", "eliminar la relación RESPONDE_A de demo_c2")
        mostrar(eliminar_relacion(d, "demo_c2"))
        mostrar(leer_subgrafo(d, "demo_p1"))

        paso("DELETE nodos", "eliminar todos los nodos demo_ (DETACH DELETE borra también sus relaciones)")
        mostrar(eliminar_nodos(d, "demo_"))
        print("¿existe el post?", bool(q(d, "MATCH (p:Post {id: 'demo_p1'}) RETURN p")))


if __name__ == "__main__":
    main()
