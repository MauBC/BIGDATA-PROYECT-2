"""CRUD en MongoDB (colección `posts`, con comentarios anidados).

Uso:  docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_mongo
La demo trabaja sobre un post de prueba (_id = 'demo_post') y lo elimina al final,
así que puede ejecutarse cuantas veces se quiera sin alterar los datos del pipeline.
"""
import datetime as dt

from crud.common import mostrar, paso, titulo
from etl.common import mongo_db

UTC = dt.timezone.utc
DEMO_ID = "demo_post"


# ---------------- operaciones ----------------
def crear_post(db, post: dict):
    return db.posts.insert_one(post).inserted_id


def leer_post(db, post_id: str, campos=None):
    return db.posts.find_one({"_id": post_id}, campos)


def actualizar_post(db, post_id: str, cambios: dict) -> int:
    return db.posts.update_one({"_id": post_id}, {"$set": cambios}).modified_count


def agregar_comentario(db, post_id: str, comentario: dict) -> int:
    return db.posts.update_one({"_id": post_id}, {"$push": {"comments": comentario},
                                                  "$inc": {"num_comments": 1}}).modified_count


def eliminar_comentario(db, post_id: str, comment_id: str) -> int:
    return db.posts.update_one({"_id": post_id}, {"$pull": {"comments": {"id": comment_id}},
                                                  "$inc": {"num_comments": -1}}).modified_count


def eliminar_post(db, post_id: str) -> int:
    return db.posts.delete_one({"_id": post_id}).deleted_count


# ---------------- demo ----------------
def main():
    ahora = dt.datetime.now(tz=UTC).replace(microsecond=0)
    with mongo_db() as db:
        titulo("CRUD MongoDB – colección posts (documento con comentarios anidados)")
        eliminar_post(db, DEMO_ID)  # por si quedó de una ejecución interrumpida

        paso("CREATE", "insertar un post con un comentario anidado")
        crear_post(db, {
            "_id": DEMO_ID, "subreddit": "LocalLLaMA", "author": "demo_user", "autor_tipo": "usuario",
            "created_utc": ahora, "fecha": ahora.strftime("%Y-%m-%d"), "hora": ahora.hour,
            "title": "Post de demostración CRUD", "selftext": "Creado desde crud_mongo.py",
            "selftext_eliminado": False, "url": None, "score": 1, "upvote_ratio": 1.0, "num_comments": 1,
            "flair": "Demo", "is_nsfw": False, "permalink": None,
            "comments": [{"id": "demo_c1", "parent_id": DEMO_ID, "depth": 0, "author": "otro_user",
                          "autor_tipo": "usuario", "created_utc": ahora, "fecha": ahora.strftime("%Y-%m-%d"),
                          "hora": ahora.hour, "body": "Primer comentario", "eliminado": False, "score": 1,
                          "is_submitter": False, "permalink": None}],
        })
        mostrar(leer_post(db, DEMO_ID, {"title": 1, "score": 1, "num_comments": 1, "comments.body": 1}))

        paso("CREATE (validación)", "un documento que no cumple el esquema es rechazado por MongoDB")
        try:
            crear_post(db, {"_id": "demo_invalido", "subreddit": "otro_subreddit", "title": 123})
        except Exception as e:  # noqa: BLE001
            print(f"Rechazado: {type(e).__name__} (code {getattr(e, 'code', '?')})")

        paso("READ", "consultar el post por _id")
        mostrar(leer_post(db, DEMO_ID, {"title": 1, "author": 1, "fecha": 1}))

        paso("READ", "consulta sobre datos reales: 3 posts con más comentarios anidados en wallstreetbets")
        top = db.posts.aggregate([
            {"$match": {"subreddit": "wallstreetbets"}},
            {"$project": {"title": 1, "fecha": 1, "n": {"$size": "$comments"}}},
            {"$sort": {"n": -1}}, {"$limit": 3}])
        mostrar(list(top))

        paso("UPDATE", "modificar score y flair del post")
        actualizar_post(db, DEMO_ID, {"score": 42, "flair": "Demo actualizada"})
        mostrar(leer_post(db, DEMO_ID, {"score": 1, "flair": 1}))

        paso("UPDATE", "agregar un comentario anidado ($push)")
        agregar_comentario(db, DEMO_ID, {"id": "demo_c2", "parent_id": "demo_c1", "depth": 1, "author": "demo_user",
                                         "autor_tipo": "usuario", "created_utc": ahora, "fecha": ahora.strftime("%Y-%m-%d"),
                                         "hora": ahora.hour, "body": "Respuesta al primer comentario",
                                         "eliminado": False, "score": 1, "is_submitter": True, "permalink": None})
        mostrar(leer_post(db, DEMO_ID, {"num_comments": 1, "comments.id": 1, "comments.depth": 1, "comments.body": 1}))

        paso("DELETE", "eliminar un comentario anidado ($pull)")
        eliminar_comentario(db, DEMO_ID, "demo_c1")
        mostrar(leer_post(db, DEMO_ID, {"num_comments": 1, "comments.id": 1}))

        paso("DELETE", "eliminar el documento completo")
        print("documentos eliminados:", eliminar_post(db, DEMO_ID))
        print("¿existe todavía?", leer_post(db, DEMO_ID) is not None)


if __name__ == "__main__":
    main()
