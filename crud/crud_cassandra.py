"""CRUD en Cassandra (tabla `actividad_diaria_por_subreddit`, serie temporal).

Uso:  docker compose exec -w /opt/airflow airflow-scheduler python -m crud.crud_cassandra
La demo usa la partición de prueba subreddit = 'demo_crud' y la elimina al final.
"""
import datetime as dt
import time

from crud.common import paso, titulo
from etl.common import cassandra_session

SUB = "demo_crud"
DIAS = [dt.date(2026, 12, 1) + dt.timedelta(days=i) for i in range(3)]


def imprimir(rows) -> None:
    rows = list(rows)
    if not rows:
        print("(sin filas)")
    for r in rows:
        print("  " + " | ".join(f"{k}={v}" for k, v in r._asdict().items()))


# ---------------- operaciones ----------------
def insertar(s, subreddit, fecha, posts, comentarios, ttl=None):
    cql = ("INSERT INTO actividad_diaria_por_subreddit (subreddit, fecha, posts, comentarios, actualizado_en) "
           "VALUES (%s, %s, %s, %s, toTimestamp(now()))") + (f" USING TTL {int(ttl)}" if ttl else "")
    s.execute(cql, (subreddit, fecha, posts, comentarios))


def consultar(s, subreddit, desde=None, hasta=None):
    cql = "SELECT subreddit, fecha, posts, comentarios FROM actividad_diaria_por_subreddit WHERE subreddit = %s"
    params = [subreddit]
    if desde and hasta:
        cql += " AND fecha >= %s AND fecha <= %s"
        params += [desde, hasta]
    return s.execute(cql, params)


def actualizar(s, subreddit, fecha, posts, comentarios):
    s.execute("UPDATE actividad_diaria_por_subreddit SET posts = %s, comentarios = %s, actualizado_en = toTimestamp(now()) "
              "WHERE subreddit = %s AND fecha = %s", (posts, comentarios, subreddit, fecha))


def eliminar_fila(s, subreddit, fecha):
    s.execute("DELETE FROM actividad_diaria_por_subreddit WHERE subreddit = %s AND fecha = %s", (subreddit, fecha))


def eliminar_particion(s, subreddit):
    s.execute("DELETE FROM actividad_diaria_por_subreddit WHERE subreddit = %s", (subreddit,))


# ---------------- demo ----------------
def main():
    with cassandra_session() as s:
        titulo("CRUD Cassandra – tabla actividad_diaria_por_subreddit (PK = (subreddit), fecha)")
        eliminar_particion(s, SUB)

        paso("INSERT", f"insertar 3 días en la partición '{SUB}'")
        for i, d in enumerate(DIAS):
            insertar(s, SUB, d, posts=10 + i, comentarios=50 + 5 * i)
        imprimir(consultar(s, SUB))
        print("(las filas salen ordenadas por fecha DESC: es el orden de clustering de la tabla)")

        paso("INSERT (upsert)", "insertar otra vez la misma clave no duplica: sobrescribe")
        insertar(s, SUB, DIAS[0], posts=99, comentarios=99)
        imprimir(consultar(s, SUB, DIAS[0], DIAS[0]))

        paso("SELECT", "consulta sobre datos reales: últimos 5 días de LocalLLaMA (una sola partición)")
        imprimir(s.execute("SELECT subreddit, fecha, posts, comentarios, autores_unicos FROM actividad_diaria_por_subreddit "
                           "WHERE subreddit = 'LocalLLaMA' LIMIT 5"))

        paso("SELECT", "KPI 'comentarios_por_post' global en un rango de fechas (tabla kpis_por_nombre)")
        imprimir(s.execute("SELECT fecha, valor FROM kpis_por_nombre WHERE kpi = 'comentarios_por_post' AND subreddit = 'ALL' "
                           "AND fecha >= '2026-07-01' AND fecha <= '2026-07-05'"))

        paso("UPDATE", f"actualizar el día {DIAS[1]}")
        actualizar(s, SUB, DIAS[1], posts=20, comentarios=200)
        imprimir(consultar(s, SUB, DIAS[1], DIAS[1]))

        paso("INSERT con TTL", "una fila que expira sola a los 3 segundos")
        insertar(s, SUB, dt.date(2026, 12, 31), 1, 1, ttl=3)
        print("TTL restante:", s.execute("SELECT TTL(posts) FROM actividad_diaria_por_subreddit "
                                         "WHERE subreddit = %s AND fecha = '2026-12-31'", (SUB,)).one()[0], "s")
        time.sleep(4)
        print("4 s después, ¿sigue la fila?",
              s.execute("SELECT * FROM actividad_diaria_por_subreddit WHERE subreddit = %s AND fecha = '2026-12-31'",
                        (SUB,)).one() is not None)

        paso("DELETE", f"eliminar una fila ({DIAS[2]})")
        eliminar_fila(s, SUB, DIAS[2])
        imprimir(consultar(s, SUB))

        paso("DELETE", f"eliminar la partición completa '{SUB}'")
        eliminar_particion(s, SUB)
        imprimir(consultar(s, SUB))


if __name__ == "__main__":
    main()
