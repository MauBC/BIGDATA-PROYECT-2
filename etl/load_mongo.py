"""Etapa 3 – Carga en MongoDB: posts como documentos con comentarios anidados.

Idempotencia:
- Post: upsert por _id con $set (los comentarios solo se inicializan en la inserción).
- Comentarios del día, agrupados por post: primero $pull de esos ids y luego $push.
  Re-ejecutar el día reemplaza los mismos comentarios en vez de duplicarlos.
Los comentarios pueden llegar días después que su post (se anidan en el documento existente).
"""
from pymongo import UpdateOne

from etl.common import load_stage, log_run, mongo_db, now_utc, records, to_datetime

POST_FIELDS = ["subreddit", "author", "autor_tipo", "fecha", "hora", "title", "selftext", "selftext_eliminado",
               "url", "score", "upvote_ratio", "num_comments", "flair", "is_nsfw", "permalink"]
COMMENT_FIELDS = ["id", "parent_id", "depth", "author", "autor_tipo", "fecha", "hora", "body", "eliminado",
                  "score", "is_submitter", "permalink"]


def post_ops(posts, cargado_en):
    for r in records(posts):
        doc = {k: r[k] for k in POST_FIELDS}
        doc["created_utc"] = to_datetime(r["created_utc"])
        doc["cargado_en"] = cargado_en
        yield UpdateOne({"_id": r["id"]}, {"$set": doc, "$setOnInsert": {"comments": []}}, upsert=True)


def comment_ops(comments):
    for link_id, grupo in comments.groupby("link_id"):
        docs = []
        for r in records(grupo):
            d = {k: r[k] for k in COMMENT_FIELDS}
            d["created_utc"] = to_datetime(r["created_utc"])
            docs.append(d)
        ids = [d["id"] for d in docs]
        yield UpdateOne({"_id": link_id}, {"$pull": {"comments": {"id": {"$in": ids}}}})
        yield UpdateOne({"_id": link_id}, {"$push": {"comments": {"$each": docs, "$sort": {"created_utc": 1}}}})


def run(fecha: str) -> dict:
    posts = load_stage(fecha, "posts_clean")
    comments = load_stage(fecha, "comments_clean")
    resumen = {"posts_insertados": 0, "posts_actualizados": 0, "comentarios_cargados": 0, "posts_destino_faltantes": 0}
    with mongo_db() as db:
        if len(posts):
            res = db.posts.bulk_write(list(post_ops(posts, now_utc())), ordered=False)
            resumen["posts_insertados"] = res.upserted_count
            resumen["posts_actualizados"] = res.matched_count
        if len(comments):
            ops = list(comment_ops(comments))
            res = db.posts.bulk_write(ops, ordered=True)  # ordered: $pull antes que $push
            n_posts = len(ops) // 2
            # matched_count cuenta pull+push; si el post no existe ninguno de los dos hace match
            resumen["posts_destino_faltantes"] = n_posts - res.matched_count // 2
            resumen["comentarios_cargados"] = len(comments)
    return log_run(fecha, "carga_mongodb", resumen)
