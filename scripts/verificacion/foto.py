"""Foto de las tres bases (IDs, relaciones, KPIs y actividad) y comparación entre dos fotos.

Sirve para comprobar que reprocesar un día o ejecutar las demos CRUD no deja cambios:
  docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/antes.json
  ... (Clear en Airflow, demos CRUD, etc.)
  docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto guardar /tmp/despues.json
  docker compose exec -w /opt/airflow airflow-scheduler python -m scripts.verificacion.foto comparar /tmp/antes.json /tmp/despues.json
"""
import json
import sys

from etl.common import cassandra_session, mongo_db, neo4j_driver


def foto() -> dict:
    f = {}
    with mongo_db() as db:
        f["mongo_posts"] = sorted(d["_id"] for d in db.posts.find({}, {"_id": 1}))
        f["mongo_comentarios"] = sorted(c["id"] for d in db.posts.find({}, {"comments.id": 1}) for c in d["comments"])
        f["mongo_rechazados"] = sorted(d["_id"] for d in db.rechazados.find({}, {"_id": 1}))
    with neo4j_driver() as d:
        q = lambda cy: [r.data() for r in d.execute_query(cy).records]  # noqa: E731
        f["neo_posts"] = sorted(r["id"] for r in q("MATCH (p:Post) RETURN p.id AS id"))
        f["neo_comentarios"] = sorted(r["id"] for r in q("MATCH (c:Comentario) RETURN c.id AS id"))
        f["neo_usuarios"] = sorted(r["n"] for r in q("MATCH (u:Usuario) RETURN u.nombre AS n"))
        f["neo_rels"] = sorted(f"{r['a']}-{r['t']}->{r['b']}" for r in q(
            "MATCH (a)-[r]->(b) RETURN coalesce(a.id, a.nombre) AS a, type(r) AS t, coalesce(b.id, b.nombre) AS b"))
    with cassandra_session() as s:
        f["kpis_por_fecha"] = {f"{r.fecha}|{r.kpi}|{r.subreddit}": round(r.valor, 6)
                               for r in s.execute("SELECT fecha, kpi, subreddit, valor FROM kpis_por_fecha")}
        f["kpis_por_nombre"] = {f"{r.fecha}|{r.kpi}|{r.subreddit}": round(r.valor, 6)
                                for r in s.execute("SELECT fecha, kpi, subreddit, valor FROM kpis_por_nombre")}
        f["actividad_por_fecha"] = {f"{r.fecha}|{r.subreddit}": [r.posts, r.comentarios] for r in s.execute(
            "SELECT fecha, subreddit, posts, comentarios FROM actividad_diaria_por_fecha")}
        f["actividad_por_subreddit"] = {f"{r.subreddit}|{r.fecha}": [r.posts, r.comentarios] for r in s.execute(
            "SELECT subreddit, fecha, posts, comentarios FROM actividad_diaria_por_subreddit")}
    return f


def diff(a: dict, b: dict) -> dict:
    out = {}
    for k in a:
        if isinstance(a[k], list):
            sa, sb = set(a[k]), set(b[k])
            if sa != sb:
                out[k] = {"solo_antes": sorted(sa - sb)[:8], "solo_despues": sorted(sb - sa)[:8],
                          "n_antes": len(sa), "n_despues": len(sb)}
        else:
            cambios = {kk: [a[k].get(kk), b[k].get(kk)] for kk in set(a[k]) | set(b[k]) if a[k].get(kk) != b[k].get(kk)}
            if cambios:
                out[k] = dict(sorted(cambios.items()))
    return out


def main(argv: list[str]) -> None:
    if argv[0] == "guardar":
        f = foto()
        with open(argv[1], "w", encoding="utf-8") as fh:
            json.dump(f, fh)
        print({k: len(v) for k, v in f.items()})
    else:
        with open(argv[1], encoding="utf-8") as fa, open(argv[2], encoding="utf-8") as fb:
            d = diff(json.load(fa), json.load(fb))
        print(json.dumps(d, indent=1, ensure_ascii=False) if d else "SIN DIFERENCIAS")


if __name__ == "__main__":
    main(sys.argv[1:])
