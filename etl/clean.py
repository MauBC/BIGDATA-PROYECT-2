"""Etapa 2 – Validación y limpieza.

Reglas (posts y comentarios):
- Rechazo: id duplicado, campos obligatorios nulos, subreddit fuera del alcance.
- Rechazo (comentarios): post inexistente o rechazado en el dataset, jerarquía inconsistente
  (depth 0 <=> parent_id == link_id) y comentarios escritos por bots.
- Normalización: autor "[deleted]"/vacío -> None, con autor_tipo = usuario | bot | eliminado.
  Posts de bots se conservan (alojan comentarios humanos) pero su autor no será nodo Usuario.
- Contenido borrado: body "[deleted]"/"[removed]" se conserva marcado `eliminado` (mantiene
  la estructura del hilo); selftext borrado -> None con `selftext_eliminado`.
Los rechazados se guardan con su motivo en MongoDB (colección `rechazados`). Al reprocesar un
día se reemplazan los rechazados de esa fecha, de modo que un registro corregido deja de figurar.
"""
import pandas as pd

from etl.common import load_stage, log_run, save_stage
from etl.config import SUBREDDITS
from etl.extract import all_posts

BOTS = {"AutoModerator", "VisualMod", "RemindMeBot", "wsb-earnings"}
BORRADO = {"[deleted]", "[removed]"}


def autor_tipo(author) -> str:
    if author is None or pd.isna(author) or str(author).strip() in ("", "[deleted]"):
        return "eliminado"
    if author in BOTS or str(author).endswith("-ModTeam"):
        return "bot"
    return "usuario"


def _rechazar(df: pd.DataFrame, mask: pd.Series, tipo: str, motivo: str, out: list) -> pd.DataFrame:
    if mask.any():
        r = df.loc[mask, ["id", "subreddit", "created_utc"]].copy()
        r["tipo"], r["motivo"] = tipo, motivo
        out.append(r)
    return df.loc[~mask]


def _comunes(df: pd.DataFrame) -> pd.DataFrame:
    df["autor_tipo"] = df["author"].map(autor_tipo)
    df["author"] = df["author"].where(df["autor_tipo"] != "eliminado", None)
    fechas = pd.to_datetime(df["created_utc"], unit="s", utc=True)
    df["fecha"] = fechas.dt.strftime("%Y-%m-%d")
    df["hora"] = fechas.dt.hour.astype("int8")
    return df


def clean_posts(df: pd.DataFrame, rej: list) -> pd.DataFrame:
    df = _rechazar(df, df["id"].duplicated(), "post", "duplicado", rej)
    df = _rechazar(df, df[["id", "created_utc", "title", "subreddit"]].isna().any(axis=1), "post", "campos_obligatorios_nulos", rej)
    df = _rechazar(df, ~df["subreddit"].isin(SUBREDDITS), "post", "subreddit_invalido", rej)
    df = _comunes(df.copy())
    df["title"] = df["title"].str.strip()
    df["selftext_eliminado"] = df["selftext"].isin(BORRADO)
    df["selftext"] = df["selftext"].where(~df["selftext_eliminado"] & df["selftext"].fillna("").str.strip().ne(""), None)
    df["flair"] = df["flair"].where(df["flair"].fillna("").str.strip().ne(""), None)
    df["upvote_ratio"] = df["upvote_ratio"].astype("float64")
    return df.drop(columns=["crawled_at", "batch_id"])


def valid_post_ids() -> set[str]:
    """Ids de los posts que superan las reglas de rechazo de clean_posts (integridad referencial)."""
    df = all_posts()
    ok = df[["id", "created_utc", "title", "subreddit"]].notna().all(axis=1) & df["subreddit"].isin(SUBREDDITS)
    return set(df.loc[ok, "id"])


def clean_comments(df: pd.DataFrame, post_ids: set, rej: list) -> pd.DataFrame:
    df = _rechazar(df, df["id"].duplicated(), "comentario", "duplicado", rej)
    df = _rechazar(df, df[["id", "link_id", "parent_id", "created_utc", "body", "depth"]].isna().any(axis=1),
                   "comentario", "campos_obligatorios_nulos", rej)
    df = _rechazar(df, ~df["subreddit"].isin(SUBREDDITS), "comentario", "subreddit_invalido", rej)
    df = _rechazar(df, ~df["link_id"].isin(post_ids), "comentario", "post_inexistente", rej)
    jerarquia_ok = (df["depth"] == 0) == (df["parent_id"] == df["link_id"])
    df = _rechazar(df, ~jerarquia_ok, "comentario", "jerarquia_inconsistente", rej)
    df = _rechazar(df, df["author"].map(autor_tipo) == "bot", "comentario", "autor_bot", rej)
    df = _comunes(df.copy())
    df["eliminado"] = df["body"].isin(BORRADO)
    df["depth"] = df["depth"].astype("int64")
    df["is_submitter"] = df["is_submitter"].map(lambda v: None if v is None or pd.isna(v) else bool(v)).astype(object)
    return df.drop(columns=["crawled_at", "batch_id"])


def run(fecha: str) -> dict:
    rej: list[pd.DataFrame] = []
    posts = clean_posts(load_stage(fecha, "posts_raw"), rej)
    comments = clean_comments(load_stage(fecha, "comments_raw"), valid_post_ids(), rej)
    rechazados = pd.concat(rej, ignore_index=True) if rej else pd.DataFrame(columns=["id", "subreddit", "created_utc", "tipo", "motivo"])
    save_stage(posts, fecha, "posts_clean")
    save_stage(comments, fecha, "comments_clean")
    save_stage(rechazados, fecha, "rechazados")

    # Los rechazados del día se reemplazan en MongoDB (borrado por fecha + upsert por tipo:id => idempotente
    # también cuando la fuente se corrige y un registro deja de ser rechazado).
    from pymongo import UpdateOne
    from etl.common import mongo_db, records
    with mongo_db() as db:
        db.rechazados.delete_many({"fecha": fecha})
        if len(rechazados):
            ops = [UpdateOne({"_id": f"{r['tipo']}:{r['id']}"},
                             {"$set": {"tipo": r["tipo"], "id_origen": r["id"], "subreddit": r["subreddit"],
                                       "fecha": fecha, "motivo": r["motivo"]}}, upsert=True)
                   for r in records(rechazados)]
            db.rechazados.bulk_write(ops, ordered=False)

    return log_run(fecha, "limpieza", {
        "posts_validos": len(posts),
        "comentarios_validos": len(comments),
        "rechazados": len(rechazados),
        "rechazados_por_motivo": rechazados["motivo"].value_counts().to_dict() if len(rechazados) else {},
        "comentarios_eliminados": int(comments["eliminado"].sum()),
    })
