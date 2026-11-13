"""Prueba de reproceso con la fuente corregida. DESTRUCTIVA: solo en un proyecto Compose aislado.

1. Carga 2026-05-31 → 2026-06-02 con la fuente original y toma una foto.
2. En una copia de la fuente, un comentario del 2026-06-01 con respuestas pasa a ser de un bot y un
   post del 2026-06-01 con comentarios queda sin título (ambos deben pasar a `rechazados`).
3. Reprocesa SOLO el 2026-06-01 con la copia y compara con la foto.
4. Reprocesa el 2026-06-01 con la fuente original y compara (efecto sobre días dependientes).
5. Reprocesa también el 2026-06-02 y compara: debe quedar igual que la foto inicial.

Uso (desde la raíz del proyecto, con volúmenes propios del proyecto bigdata-test):
  docker compose -p bigdata-test -f docker-compose.yml -f scripts/verificacion/compose.prueba.yml up -d db-init
  docker compose -p bigdata-test -f docker-compose.yml -f scripts/verificacion/compose.prueba.yml run --rm --no-deps \
      --entrypoint python db-init -m scripts.verificacion.correccion
  docker compose -p bigdata-test -f docker-compose.yml -f scripts/verificacion/compose.prueba.yml down -v
"""
import glob
import json
import os
import shutil
import subprocess
import sys

import pandas as pd

from etl.clean import autor_tipo
from etl.common import day_bounds, mongo_db
from etl.config import DATA_DIR
from scripts.verificacion.foto import diff, foto

DIA = "2026-06-01"
COPIA = "/tmp/fuente_corregida"
STAGING = "/tmp/staging_prueba_correccion"


def pipeline(desde: str, hasta: str, data_dir: str) -> None:
    env = {**os.environ, "DATA_DIR": data_dir, "STAGING_DIR": STAGING}
    subprocess.run([sys.executable, "-m", "etl.pipeline", desde, hasta], env=env, check=True,
                   stdout=subprocess.DEVNULL)


def preparar_copia() -> dict:
    shutil.rmtree(COPIA, ignore_errors=True)
    shutil.copytree(DATA_DIR, COPIA)
    ini, fin = day_bounds(DIA)
    archivos = sorted(glob.glob(f"{COPIA}/comments/subreddit=*/part-*.parquet"))
    todos = pd.concat([pd.read_parquet(p) for p in archivos])
    padres = set(todos["parent_id"])
    cambios = {}
    for path in archivos:
        df = pd.read_parquet(path)
        cand = df[(df["created_utc"] >= ini) & (df["created_utc"] < fin) & df["id"].isin(padres)
                  & (df["author"].map(autor_tipo) == "usuario")]
        if len(cand):
            c = cand.iloc[0]
            df.loc[df["id"] == c["id"], "author"] = "AutoModerator"
            df.to_parquet(path, index=False)
            cambios["comentario_a_bot"] = c["id"]
            cambios["respuestas_al_comentario"] = todos.loc[todos["parent_id"] == c["id"], "id"].tolist()
            break
    for path in sorted(glob.glob(f"{COPIA}/posts/subreddit=*/part-*.parquet")):
        df = pd.read_parquet(path)
        cand = df[(df["created_utc"] >= ini) & (df["created_utc"] < fin) & (df["num_comments"] > 0)]
        if len(cand):
            p = cand.iloc[0]
            df.loc[df["id"] == p["id"], "title"] = None
            df.to_parquet(path, index=False)
            cambios["post_sin_titulo"] = p["id"]
            cambios["comentarios_del_post"] = todos.loc[todos["link_id"] == p["id"], ["id", "created_utc"]].assign(
                fecha=lambda x: pd.to_datetime(x["created_utc"], unit="s").dt.strftime("%Y-%m-%d"))[["id", "fecha"]
                                                                                                    ].values.tolist()
            break
    return cambios


def main() -> None:
    with mongo_db() as db:
        if db.posts.count_documents({}) and "--forzar" not in sys.argv:
            sys.exit("La base MongoDB ya tiene posts: esta prueba es destructiva y debe correr en un proyecto aislado "
                     "(ver el docstring). Use --forzar solo si sabe lo que hace.")
    pipeline("2026-05-31", "2026-06-02", DATA_DIR)
    inicial = foto()
    cambios = preparar_copia()
    pipeline(DIA, DIA, COPIA)
    corregida = foto()
    pipeline(DIA, DIA, DATA_DIR)
    restaurada = foto()
    pipeline("2026-06-02", "2026-06-02", DATA_DIR)
    final = foto()
    print(json.dumps({
        "cambios_en_la_copia": cambios,
        "1_tras_reprocesar_con_la_copia": diff(inicial, corregida),
        "2_tras_reprocesar_con_la_fuente_original": diff(inicial, restaurada) or "SIN DIFERENCIAS",
        "3_tras_reprocesar_tambien_el_dia_siguiente": diff(inicial, final) or "SIN DIFERENCIAS",
    }, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
