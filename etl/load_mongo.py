"""Etapa 3 – Carga en MongoDB: posts como documentos con comentarios anidados.

Idempotencia (cada ejecución reemplaza la "porción" del día procesado):
- Post: upsert por _id con $set (los comentarios solo se inicializan en la inserción). Los posts
  de esa fecha que ya no son válidos en la fuente se eliminan.
- Comentarios: se retiran ($pull) todos los comentarios anidados con esa fecha y se vuelven a
  anidar ($push) los válidos, agrupados por post. Re-ejecutar el día no duplica, y un comentario
  que pasa a rechazado deja de estar anidado.
Los comentarios pueden llegar días después que su post (se anidan en el documento existente).
Si el post destino todavía no está cargado (p. ej. se procesó un día sin haber procesado antes
el día de sus posts), la etapa falla en lugar de descartar comentarios en silencio.
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
        yield UpdateOne({"_id": link_id}, {"$push": {"comments": {"$each": docs, "$sort": {"created_utc": 1}}}})


def run(fecha: str) -> dict:
    posts = load_stage(fecha, "posts_clean")
    comments = load_stage(fecha, "comments_clean")
    resumen = {"posts_insertados": 0, "posts_actualizados": 0, "posts_eliminados": 0, "comentarios_cargados": 0,
               "comentarios_retirados": 0, "posts_destino_faltantes": 0, "comentarios_sin_post": 0}
    with mongo_db() as db:
        resumen["posts_eliminados"] = db.posts.delete_many(
            {"fecha": fecha, "_id": {"$nin": posts["id"].tolist()}}).deleted_count
        if len(posts):
            res = db.posts.bulk_write(list(post_ops(posts, now_utc())), ordered=False)
            resumen["posts_insertados"] = res.upserted_count
            resumen["posts_actualizados"] = res.matched_count

        # Se retiran los comentarios del día ya anidados (anteriores ejecuciones) antes de volver a anidarlos.
        retirados = next(db.posts.aggregate([
            {"$match": {"comments.fecha": fecha}},
            {"$group": {"_id": None, "n": {"$sum": {"$size": {"$filter": {
                "input": "$comments", "cond": {"$eq": ["$$this.fecha", fecha]}}}}}}}]), {"n": 0})["n"]
        db.posts.update_many({"comments.fecha": fecha}, {"$pull": {"comments": {"fecha": fecha}}})
        resumen["comentarios_retirados"] = retirados

        if len(comments):
            destinos = comments["link_id"].unique().tolist()
            existentes = {d["_id"] for d in db.posts.find({"_id": {"$in": destinos}}, {"_id": 1})}
            faltan = comments[~comments["link_id"].isin(existentes)]
            cargables = comments[comments["link_id"].isin(existentes)]
            if len(cargables):
                db.posts.bulk_write(list(comment_ops(cargables)), ordered=False)
            resumen["comentarios_cargados"] = len(cargables)
            resumen["posts_destino_faltantes"] = int(faltan["link_id"].nunique())
            resumen["comentarios_sin_post"] = len(faltan)

    log_run(fecha, "carga_mongodb", resumen)
    if resumen["comentarios_sin_post"]:
        raise RuntimeError(
            f"{resumen['comentarios_sin_post']} comentarios de {fecha} no se anidaron porque "
            f"{resumen['posts_destino_faltantes']} posts destino no están en MongoDB. Procese primero los días "
            "de esos posts (el histórico empieza el 2026-05-31) y vuelva a ejecutar este día.")
    return resumen
