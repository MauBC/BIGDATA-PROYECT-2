"""Etapa 4 – Transformación y carga en Cassandra (series temporales agregadas por día).

Idempotencia: todo es UPSERT por clave primaria; siempre se escriben las filas de los
3 subreddits (con ceros si no hubo actividad) y las 24 horas. Para el ranking de autores
se borra la partición (subreddit, fecha) antes de reinsertar.
"""
import datetime as dt

import pandas as pd

from etl.common import cassandra_session, load_stage, log_run, now_utc
from etl.config import SUBREDDITS

TOP_N = 20


def agregados(posts: pd.DataFrame, comments: pd.DataFrame, subreddit: str) -> dict:
    p = posts[posts["subreddit"] == subreddit]
    c = comments[comments["subreddit"] == subreddit]
    autores = pd.concat([p.loc[p["autor_tipo"] == "usuario", "author"], c.loc[c["autor_tipo"] == "usuario", "author"]])
    return {
        "posts": len(p),
        "comentarios": len(c),
        "autores_unicos": int(autores.nunique()),
        "score_posts": int(p["score"].sum()),
        "score_comentarios": int(c["score"].sum()),
        "avg_score_post": float(p["score"].mean()) if len(p) else 0.0,
        "avg_upvote_ratio": float(p["upvote_ratio"].mean()) if p["upvote_ratio"].notna().any() else 0.0,
        "posts_sin_comentarios": int((p["num_comments"] == 0).sum()),
        "comentarios_eliminados": int(c["eliminado"].sum()),
        "profundidad_max": int(c["depth"].max()) if len(c) else 0,
        "profundidad_media": float(c["depth"].mean()) if len(c) else 0.0,
    }


def top_autores(posts: pd.DataFrame, comments: pd.DataFrame, subreddit: str) -> pd.DataFrame:
    cols = ["author", "score"]
    p = posts.loc[(posts["subreddit"] == subreddit) & (posts["autor_tipo"] == "usuario"), cols].assign(posts=1, comentarios=0)
    c = comments.loc[(comments["subreddit"] == subreddit) & (comments["autor_tipo"] == "usuario"), cols].assign(posts=0, comentarios=1)
    df = pd.concat([p, c])
    if df.empty:
        return df
    g = df.groupby("author").agg(posts=("posts", "sum"), comentarios=("comentarios", "sum"), score_total=("score", "sum")).reset_index()
    g["actividad"] = g["posts"] + g["comentarios"]
    return g.sort_values(["actividad", "author"], ascending=[False, True]).head(TOP_N)


def por_hora(df: pd.DataFrame, subreddit: str) -> pd.Series:
    return df.loc[df["subreddit"] == subreddit, "hora"].value_counts().reindex(range(24), fill_value=0)


def run(fecha: str) -> dict:
    posts = load_stage(fecha, "posts_clean")
    comments = load_stage(fecha, "comments_clean")
    dia = dt.date.fromisoformat(fecha)
    ahora = now_utc()
    filas = 0
    with cassandra_session() as s:
        ins_sub = s.prepare("""INSERT INTO actividad_diaria_por_subreddit (subreddit, fecha, posts, comentarios,
            autores_unicos, score_posts, score_comentarios, avg_score_post, avg_upvote_ratio, posts_sin_comentarios,
            comentarios_eliminados, profundidad_max, profundidad_media, actualizado_en)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
        ins_fecha = s.prepare("""INSERT INTO actividad_diaria_por_fecha (fecha, subreddit, posts, comentarios,
            autores_unicos, actualizado_en) VALUES (?,?,?,?,?,?)""")
        ins_hora = s.prepare("INSERT INTO actividad_por_hora (subreddit, fecha, hora, posts, comentarios) VALUES (?,?,?,?,?)")
        del_top = s.prepare("DELETE FROM top_autores_por_dia WHERE subreddit = ? AND fecha = ?")
        ins_top = s.prepare("""INSERT INTO top_autores_por_dia (subreddit, fecha, actividad, autor, posts, comentarios,
            score_total) VALUES (?,?,?,?,?,?,?)""")

        for sub in SUBREDDITS:
            a = agregados(posts, comments, sub)
            s.execute(ins_sub, (sub, dia, a["posts"], a["comentarios"], a["autores_unicos"], a["score_posts"],
                                a["score_comentarios"], a["avg_score_post"], a["avg_upvote_ratio"],
                                a["posts_sin_comentarios"], a["comentarios_eliminados"], a["profundidad_max"],
                                a["profundidad_media"], ahora))
            s.execute(ins_fecha, (dia, sub, a["posts"], a["comentarios"], a["autores_unicos"], ahora))
            filas += 2

            hp, hc = por_hora(posts, sub), por_hora(comments, sub)
            for h in range(24):
                s.execute(ins_hora, (sub, dia, h, int(hp[h]), int(hc[h])))
            filas += 24

            s.execute(del_top, (sub, dia))
            for r in top_autores(posts, comments, sub).itertuples():
                s.execute(ins_top, (sub, dia, int(r.actividad), r.author, int(r.posts), int(r.comentarios), int(r.score_total)))
                filas += 1
    return log_run(fecha, "carga_cassandra", {"filas_upsert": filas, "posts": len(posts), "comentarios": len(comments)})
