"""Conteos esperados (parquet + reglas de limpieza) frente a lo cargado en MongoDB, Cassandra y Neo4j.

Uso:  docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.conteos [hasta]
`hasta` (por defecto 2026-09-12) es el último día UTC procesado; la fuente se recorta a ese día.
"""
import json
import sys

import pandas as pd

from etl import clean, extract
from etl.common import cassandra_session, day_bounds, mongo_db, neo4j_driver

CASSANDRA_TABLAS = ["actividad_diaria_por_subreddit", "actividad_diaria_por_fecha", "actividad_por_hora",
                    "top_autores_por_dia", "kpis_por_nombre", "kpis_por_fecha"]


def esperado(hasta: str) -> tuple[dict, pd.Series, pd.Series]:
    _, fin = day_bounds(hasta)
    posts_raw = extract._dataset("posts").to_table().to_pandas()
    coms_raw = extract._dataset("comments").to_table().to_pandas()
    posts_raw, coms_raw = posts_raw[posts_raw["created_utc"] < fin], coms_raw[coms_raw["created_utc"] < fin]
    rej = []
    p = clean.clean_posts(posts_raw.copy(), rej)
    c = clean.clean_comments(coms_raw.copy(), clean.valid_post_ids(), rej)
    r = pd.concat(rej) if rej else pd.DataFrame(columns=["motivo"])
    cids = set(c["id"])
    resp = c[c["depth"] > 0]
    usuarios = set(p.loc[p["autor_tipo"] == "usuario", "author"]) | set(c.loc[c["autor_tipo"] == "usuario", "author"])
    p_ts = dict(zip(p["id"], p["created_utc"]))
    c_ts = dict(zip(c["id"], c["created_utc"]))
    e = {
        "posts_validos": len(p), "comentarios_validos": len(c), "rechazados": len(r),
        "rechazados_por_motivo": r["motivo"].value_counts().to_dict(),
        "respuestas": len(resp),
        "respuestas_con_padre_valido": int(resp["parent_id"].isin(cids).sum()),
        "respuestas_padre_descartado": int((~resp["parent_id"].isin(cids)).sum()),
        "usuarios": len(usuarios),
        "publica": int((p["autor_tipo"] == "usuario").sum()),
        "comenta": int((c["autor_tipo"] == "usuario").sum()),
        "comentarios_anteriores_a_su_post": int((c["created_utc"] < c["link_id"].map(p_ts)).sum()),
        "respuestas_anteriores_a_su_padre": int((resp["created_utc"] < resp["parent_id"].map(c_ts)).sum()),
    }
    return e, p.groupby(["subreddit", "fecha"]).size(), c.groupby(["subreddit", "fecha"]).size()


def mongo() -> dict:
    etapas = ["extraccion", "limpieza", "carga_mongodb", "carga_cassandra", "carga_neo4j", "kpis"]
    with mongo_db() as db:
        dup = list(db.posts.aggregate([{"$unwind": "$comments"}, {"$group": {"_id": "$comments.id", "n": {"$sum": 1}}},
                                       {"$match": {"n": {"$gt": 1}}}, {"$count": "n"}]))
        runs = list(db.etl_runs.find())
        return {
            "posts": db.posts.count_documents({}),
            "comentarios_anidados": next(db.posts.aggregate(
                [{"$group": {"_id": None, "n": {"$sum": {"$size": "$comments"}}}}]), {"n": 0})["n"],
            "ids_comentario_duplicados": dup[0]["n"] if dup else 0,
            "rechazados": db.rechazados.count_documents({}),
            "rechazados_por_motivo": {d["_id"]: d["n"] for d in db.rechazados.aggregate(
                [{"$group": {"_id": "$motivo", "n": {"$sum": 1}}}])},
            "etl_runs": len(runs),
            "etl_runs_con_6_etapas": sum(all(e in r.get("etapas", {}) for e in etapas) for r in runs),
            "comentarios_sin_post_reportados": sum(
                r.get("etapas", {}).get("carga_mongodb", {}).get("comentarios_sin_post", 0) for r in runs),
        }


def cassandra(dias_p: pd.Series, dias_c: pd.Series) -> dict:
    with cassandra_session() as s:
        filas = {t: s.execute(f"SELECT count(*) FROM {t}").one()[0] for t in CASSANDRA_TABLAS}
        act = pd.DataFrame([r._asdict() for r in s.execute(
            "SELECT subreddit, fecha, posts, comentarios FROM actividad_diaria_por_subreddit")])
        kpis = pd.DataFrame([r._asdict() for r in s.execute("SELECT kpi, subreddit, fecha, valor FROM kpis_por_nombre")])
        ultimo = s.execute("SELECT fecha, valor FROM kpis_por_nombre WHERE kpi = 'max_conexiones_usuario' "
                           "AND subreddit = 'ALL' LIMIT 1").one()
    distintos = []
    if not act.empty:
        act["fecha"] = act["fecha"].astype(str)
        for r in act.itertuples():
            esperado_dia = (int(dias_p.get((r.subreddit, r.fecha), 0)), int(dias_c.get((r.subreddit, r.fecha), 0)))
            if (r.posts, r.comentarios) != esperado_dia:
                distintos.append({"subreddit": r.subreddit, "fecha": r.fecha,
                                  "cargado": [r.posts, r.comentarios], "esperado": list(esperado_dia)})
    return {
        "filas": filas,
        "totales": {} if act.empty else {sub: {"posts": int(g["posts"].sum()), "comentarios": int(g["comentarios"].sum()),
                                               "dias": len(g)} for sub, g in act.groupby("subreddit")},
        "dias_distintos_de_la_fuente": len(distintos), "ejemplos_distintos": distintos[:10],
        "kpis_por_nombre": {} if kpis.empty else kpis.groupby("kpi").size().to_dict(),
        "max_conexiones_ultimo_dia": [str(ultimo.fecha), ultimo.valor] if ultimo else None,
    }


def neo4j() -> dict:
    with neo4j_driver() as d:
        q = lambda cy: d.execute_query(cy).records  # noqa: E731
        n = lambda cy: q(cy)[0]["n"]  # noqa: E731
        return {
            "nodos": {r["l"]: r["n"] for r in q("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS n")},
            "relaciones": {r["t"]: r["n"] for r in q("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS n")},
            "comentarios_sin_post": n("MATCH (c:Comentario) WHERE NOT (c)-[:PERTENECE_A]->() RETURN count(c) AS n"),
            "posts_sin_subreddit": n("MATCH (p:Post) WHERE NOT (p)-[:EN]->() RETURN count(p) AS n"),
            "respuestas_sin_padre": n("MATCH (c:Comentario) WHERE c.profundidad > 0 AND NOT (c)-[:RESPONDE_A]->() "
                                      "RETURN count(c) AS n"),
            "usuarios_huerfanos": n("MATCH (u:Usuario) WHERE NOT (u)--() RETURN count(u) AS n"),
            "usuarios_en_mas_de_un_subreddit": n("""MATCH (u:Usuario)-[:PUBLICA|COMENTA]->(x)
                                                    WITH u, count(DISTINCT x.subreddit) AS s WHERE s > 1
                                                    RETURN count(u) AS n"""),
            "grado_medio_usuario": q("MATCH (u:Usuario) RETURN avg(COUNT { (u)--() }) AS n")[0]["n"],
            "top_comentaristas": [r.data() for r in q("""MATCH (u:Usuario)-[:COMENTA]->(c) RETURN u.nombre AS usuario,
                                                         count(c) AS comentarios ORDER BY comentarios DESC LIMIT 3""")],
        }


def main(argv: list[str]) -> None:
    hasta = argv[0] if argv else "2026-09-12"
    e, dias_p, dias_c = esperado(hasta)
    print(json.dumps({"hasta": hasta, "esperado": e, "mongo": mongo(), "cassandra": cassandra(dias_p, dias_c),
                      "neo4j": neo4j()}, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main(sys.argv[1:])
