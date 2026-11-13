"""Etapa 1 – Extracción: lee de los parquet solo los registros cuyo created_utc cae en el día."""
import glob
import os

import pandas as pd
import pyarrow.dataset as ds

from etl.common import day_bounds, log_run, save_stage
from etl.config import DATA_DIR


def _dataset(kind: str) -> ds.Dataset:
    files = sorted(glob.glob(os.path.join(DATA_DIR, kind, "subreddit=*", "part-*.parquet")))
    if not files:
        raise FileNotFoundError(f"No hay archivos parquet de {kind} en {DATA_DIR}")
    return ds.dataset(files, format="parquet")  # todos los part-*.parquet de todos los subreddits


def read_day(kind: str, fecha: str) -> pd.DataFrame:
    start, end = day_bounds(fecha)
    filtro = (ds.field("created_utc") >= start) & (ds.field("created_utc") < end)  # predicate pushdown
    return _dataset(kind).to_table(filter=filtro).to_pandas()


def all_posts() -> pd.DataFrame:
    """Campos obligatorios de todos los posts del dataset (para validar integridad referencial de los comentarios)."""
    return _dataset("posts").to_table(columns=["id", "created_utc", "title", "subreddit"]).to_pandas()


def run(fecha: str) -> dict:
    posts = read_day("posts", fecha)
    comments = read_day("comments", fecha)
    save_stage(posts, fecha, "posts_raw")
    save_stage(comments, fecha, "comments_raw")
    return log_run(fecha, "extraccion", {"posts": len(posts), "comentarios": len(comments)})
