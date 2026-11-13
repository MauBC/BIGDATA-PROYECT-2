"""Dashboard analítico – Reddit multimodelo (MongoDB + Cassandra + Neo4j).

Ejecutar: docker compose up -d dashboard  ->  http://localhost:8501
"""
import datetime as dt
import math

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import consultas as q
from etl.config import SUBREDDITS
from etl.kpis import KPI_DEFS

st.set_page_config(page_title="Reddit Big Data", page_icon="📊", layout="wide")

# ---------------- tema y paleta (categórica fija por subreddit, validada para daltonismo) ----------------
OSCURO = (getattr(getattr(st.context, "theme", None), "type", None) == "dark")
PALETA = {
    False: {"LocalLLaMA": "#2a78d6", "wallstreetbets": "#eb6834", "SkincareAddiction": "#1baf7a", "ALL": "#52514e",
            "texto": "#0b0b0b", "texto2": "#52514e", "grid": "#e4e3df", "neutro": "#9b9a94"},
    True: {"LocalLLaMA": "#3987e5", "wallstreetbets": "#d95926", "SkincareAddiction": "#199e70", "ALL": "#c3c2b7",
           "texto": "#f0efec", "texto2": "#c3c2b7", "grid": "#33332f", "neutro": "#6f6e69"},
}[OSCURO]
SECUENCIAL = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
if OSCURO:
    SECUENCIAL = SECUENCIAL[::-1]
COLORES = {k: PALETA[k] for k in [*SUBREDDITS, "ALL"]}
FECHA_MIN, FECHA_MAX = dt.date(2026, 5, 31), dt.date(2026, 9, 12)


def estilo(fig: go.Figure, alto: int = 300, leyenda: bool = True, titulo_x: str = "") -> go.Figure:
    fig.update_layout(
        template="plotly_dark" if OSCURO else "plotly_white",
        height=alto, margin=dict(l=8, r=8, t=36 if leyenda else 12, b=8),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=PALETA["texto2"], size=12), hoverlabel=dict(font_size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title_text="", font_color=PALETA["texto"]),
        showlegend=leyenda, barcornerradius=4, hovermode="x unified" if fig.data and fig.data[0].type == "scatter" else "closest",
    )
    fig.update_xaxes(showgrid=False, linecolor=PALETA["grid"], tickfont_color=PALETA["texto2"], title_text=titulo_x)
    fig.update_yaxes(gridcolor=PALETA["grid"], zeroline=False, tickfont_color=PALETA["texto2"], title_text="")
    for t in fig.data:  # orden fijo de la leyenda = orden de la paleta categórica
        if t.name in SUBREDDITS:
            t.legendrank = SUBREDDITS.index(t.name)
    fig.update_traces(selector=dict(type="scatter"), line_width=2)
    return fig


def grafico(fig, **kw):
    st.plotly_chart(fig, use_container_width=True, theme=None, config={"displayModeBar": False}, **kw)


# Cómo se resume cada KPI en el período: razones de sumas (ponderadas por volumen), no promedio de promedios diarios.
RESUMEN = {
    "registros_procesados": "suma del período",
    "crecimiento_actividad_pct": "mediana diaria de la actividad conjunta de las comunidades elegidas",
    "comentarios_por_post": "Σ comentarios / Σ posts, en días con posts",
    "actividad_por_usuario": "promedio diario ponderado por autores únicos",
    "tasa_eliminados_pct": "Σ eliminados / Σ comentarios",
    "profundidad_media": "Σ profundidad / Σ comentarios",
    "score_medio_post": "Σ score / Σ posts",
    "max_conexiones_usuario": "acumulado al último día del rango · global, sin filtro de comunidad",
}


def valor_periodo(clave: str, serie: pd.DataFrame, act: pd.DataFrame) -> str:
    def razon(num, den, escala=1.0):
        return f"{num / den * escala:,.2f}" if den else "–"

    if clave == "registros_procesados":
        return f"{serie['valor'].sum():,.0f}"
    if clave == "max_conexiones_usuario":
        return f"{serie.sort_values('fecha')['valor'].iloc[-1]:,.0f} usuarios"
    if clave == "crecimiento_actividad_pct":
        diario = act.groupby("fecha")[["posts", "comentarios"]].sum().sum(axis=1).asfreq("D")
        g = (diario.pct_change(fill_method=None) * 100).replace([float("inf"), -float("inf")], float("nan")).dropna()
        return f"{g.median():+,.1f} %" if len(g) else "–"
    if clave == "comentarios_por_post":
        con_posts = act[act["posts"] > 0]
        return razon(con_posts["comentarios"].sum(), con_posts["posts"].sum())
    if clave == "score_medio_post":
        return razon(act["score_posts"].sum(), act["posts"].sum())
    if clave == "tasa_eliminados_pct":
        return razon(act["comentarios_eliminados"].sum(), act["comentarios"].sum(), 100) + " %"
    if clave == "profundidad_media":
        return razon((act["profundidad_media"] * act["comentarios"]).sum(), act["comentarios"].sum())
    if clave == "actividad_por_usuario":
        m = serie.merge(act[["fecha", "subreddit", "autores_unicos"]], on=["fecha", "subreddit"])
        return razon((m["valor"] * m["autores_unicos"]).sum(), m["autores_unicos"].sum())
    return f"{serie['valor'].mean():,.2f}"


# ---------------- filtros ----------------
with st.sidebar:
    st.header("Filtros")
    rango = st.date_input("Rango de fechas (UTC)", (FECHA_MIN, FECHA_MAX), min_value=FECHA_MIN, max_value=FECHA_MAX)
    subs = st.multiselect("Subreddits", SUBREDDITS, default=SUBREDDITS)
    st.divider()
    st.caption("**Origen de cada vista**\n\n"
               "🟦 Cassandra: series diarias, horas, ranking y KPIs\n\n"
               "🟩 MongoDB: posts con comentarios anidados y bitácora ETL\n\n"
               "🟪 Neo4j: red de interacciones entre usuarios")
    if st.button("Recargar datos"):
        st.cache_data.clear()

if not isinstance(rango, (tuple, list)) or len(rango) != 2 or not subs:
    st.info("Selecciona un rango de dos fechas y al menos un subreddit.")
    st.stop()
desde, hasta = rango
D, H, S = desde.isoformat(), hasta.isoformat(), tuple(subs)

st.title("Reddit · pipeline multimodelo")
st.caption(f"r/{', r/'.join(subs)} · {desde:%d %b} → {hasta:%d %b %Y} · datos cargados por el DAG `reddit_etl` de Airflow")

# ---------------- datos base ----------------
act = q.actividad_diaria(desde, hasta)
act = act[act["subreddit"].isin(subs)] if not act.empty else act
kpis = q.kpi_series(tuple(KPI_DEFS), desde, hasta)

if act.empty:
    st.warning("No hay datos cargados en Cassandra para el rango elegido. ¿Ya corrió el DAG?")
    st.stop()

# ---------------- tarjetas resumen ----------------
n_posts, n_coms = int(act["posts"].sum()), int(act["comentarios"].sum())
conv = q.conversacion_por_post(S, D, H)
c = st.columns(5)
c[0].metric("Registros procesados", f"{n_posts + n_coms:,}", help=KPI_DEFS["registros_procesados"]["definicion"])
c[1].metric("Posts", f"{n_posts:,}")
c[2].metric("Comentarios", f"{n_coms:,}")
c[3].metric("Comentarios por post publicado", f"{conv['comentarios'] / conv['posts']:.2f}" if conv["posts"] else "–",
            help="MongoDB: comentarios válidos anidados (recibidos hasta el 12-sep) en los posts publicados en el "
                 "rango, dividido entre esos posts. A diferencia de la razón diaria, sigue a cada publicación.")
c[4].metric("Comentarios eliminados", f"{act['comentarios_eliminados'].sum() / n_coms * 100:.1f} %" if n_coms else "–",
            help=KPI_DEFS["tasa_eliminados_pct"]["definicion"])

tab_kpi, tab_act, tab_cont, tab_red, tab_etl = st.tabs(
    ["📈 KPIs", "🗓️ Actividad · Cassandra", "📄 Contenido · MongoDB", "🕸️ Red · Neo4j", "⚙️ Pipeline"])

# ---------------- KPIs ----------------
with tab_kpi:
    st.caption("Cada KPI se calcula automáticamente en la etapa 6 del DAG y se guarda en Cassandra "
               "(`kpis_por_nombre`). El valor del período pondera por volumen (razón de sumas sobre "
               "`actividad_diaria_por_subreddit`), no promedia los valores diarios; el método figura bajo cada valor.")
    for clave, meta in KPI_DEFS.items():
        serie = kpis[kpis["kpi"] == clave]
        solo_global = clave == "max_conexiones_usuario"
        serie = serie[serie["subreddit"] == "ALL"] if solo_global else serie[serie["subreddit"].isin(subs)]
        with st.container(border=True):
            izq, der = st.columns([2, 3], gap="large")
            with izq:
                st.subheader(meta["nombre"])
                st.metric("Valor del período", "–" if serie.empty else valor_periodo(clave, serie, act))
                st.caption(RESUMEN[clave])
                if solo_global and len(subs) < len(SUBREDDITS):
                    st.caption("⚠️ Este KPI no responde al filtro de subreddits: siempre es el de la red completa.")
                st.markdown(f"**Definición.** {meta['definicion']}")
                st.markdown(f"**Fórmula.** `{meta['formula']}`")
                st.markdown(f"**Interpretación.** {meta['interpretacion']}")
            with der:
                if not serie.empty:
                    fig = px.line(serie.sort_values("fecha"), x="fecha", y="valor", color="subreddit",
                                  color_discrete_map=COLORES, labels={"valor": meta["nombre"], "fecha": ""})
                    if clave == "crecimiento_actividad_pct":
                        fig.add_hline(y=0, line_color=PALETA["neutro"], line_width=1)
                    grafico(estilo(fig, alto=280, leyenda=not solo_global), key=f"kpi_{clave}")

# ---------------- Actividad (Cassandra) ----------------
with tab_act:
    st.caption("Fuente: Cassandra · tablas `actividad_diaria_por_subreddit`, `actividad_por_hora`, `top_autores_por_dia`")
    a, b = st.columns(2)
    with a:
        st.markdown("**Posts publicados por día**")
        grafico(estilo(px.line(act.sort_values("fecha"), x="fecha", y="posts", color="subreddit",
                               color_discrete_map=COLORES, labels={"posts": "posts"})), key="act_posts")
    with b:
        st.markdown("**Comentarios publicados por día**")
        grafico(estilo(px.line(act.sort_values("fecha"), x="fecha", y="comentarios", color="subreddit",
                               color_discrete_map=COLORES, labels={"comentarios": "comentarios"})), key="act_coms")
    st.caption("Los posts terminan el 31-ago: los comentarios posteriores son respuestas tardías a posts ya publicados.")

    st.markdown("**¿A qué hora (UTC) se publica?** · % de la actividad (posts + comentarios) de cada subreddit")
    horas = q.actividad_por_hora(S, desde, hasta)
    if not horas.empty:
        horas["actividad"] = horas["posts"] + horas["comentarios"]
        mapa = horas.groupby(["subreddit", "hora"])["actividad"].sum().unstack(fill_value=0).reindex(subs)
        pct = mapa.div(mapa.sum(axis=1).replace(0, 1), axis=0) * 100
        fig = go.Figure(go.Heatmap(z=pct.values, x=[f"{h:02d}h" for h in pct.columns], y=pct.index,
                                   customdata=mapa.values, colorscale=SECUENCIAL, xgap=2, ygap=2,
                                   colorbar=dict(title="%", thickness=10),
                                   hovertemplate="%{y} · %{x}<br>%{z:.1f} % del día<br>%{customdata:,} registros<extra></extra>"))
        grafico(estilo(fig, alto=80 + 45 * len(subs), leyenda=False), key="heat")

    st.markdown("**Top 10 autores del día**")
    f1, f2 = st.columns([1, 1])
    dia_top = f1.date_input("Día", max(desde, min(hasta, dt.date(2026, 8, 31))),
                            min_value=desde, max_value=hasta, key="dia_top")
    sub_top = f2.selectbox("Subreddit", subs, key="sub_top")
    top = q.top_autores(sub_top, dia_top)
    if top.empty:
        st.info("Sin autores humanos ese día.")
    else:
        fig = px.bar(top.sort_values("actividad"), x="actividad", y="autor", orientation="h",
                     hover_data=["posts", "comentarios", "score_total"], color_discrete_sequence=[COLORES[sub_top]])
        grafico(estilo(fig, alto=60 + 28 * len(top), leyenda=False), key="top_aut")

# ---------------- Contenido (MongoDB) ----------------
with tab_cont:
    st.caption("Fuente: MongoDB · colección `posts` (comentarios anidados) y `rechazados`")
    st.markdown("**Posts con más comentarios** (tamaño del arreglo `comments` del documento)")
    tp = q.top_posts(S, D, H)
    if not tp.empty:
        st.dataframe(tp[["subreddit", "fecha", "title", "comentarios", "profundidad_max", "score"]],
                     hide_index=True, use_container_width=True,
                     column_config={"title": st.column_config.TextColumn("título", width="large"),
                                    "comentarios": st.column_config.ProgressColumn(
                                        "comentarios", format="%d", min_value=0, max_value=int(tp["comentarios"].max()))})
    else:
        st.info("No hay posts publicados en el rango (los posts del dataset terminan el 31-ago).")
    a, b = st.columns(2)
    with a:
        st.markdown("**Profundidad de los hilos** · % de comentarios por nivel")
        prof = q.distribucion_profundidad(S, D, H)
        if not prof.empty:
            prof["pct"] = prof["comentarios"] / prof.groupby("subreddit")["comentarios"].transform("sum") * 100
            prof = prof[prof["depth"] <= 8].sort_values("depth")
            fig = px.bar(prof, x="depth", y="pct", color="subreddit", barmode="group", color_discrete_map=COLORES,
                         labels={"depth": "nivel (0 = respuesta directa al post)", "pct": "% de comentarios"},
                         hover_data={"comentarios": ":,", "pct": ":.1f"})
            fig.update_layout(bargap=0.25, bargroupgap=0.08)
            grafico(estilo(fig, titulo_x="nivel de anidamiento (0 = respuesta directa al post)"), key="prof")
    with b:
        st.markdown("**Flairs más usados** (top 10)")
        fl = q.flairs(S, D, H)
        if not fl.empty:
            total = fl.groupby("flair")["posts"].sum().nlargest(10)
            fl = fl[fl["flair"].isin(total.index)]
            fig = px.bar(fl, x="posts", y="flair", color="subreddit", orientation="h", color_discrete_map=COLORES,
                         category_orders={"flair": total.index.tolist()})
            fig.update_layout(barmode="stack")
            grafico(estilo(fig), key="flairs")
        else:
            st.info("Sin posts con flair en el rango.")

# ---------------- Red (Neo4j) ----------------
with tab_red:
    st.caption("Fuente: Neo4j · (:Usuario)-[:COMENTA]->(:Comentario)-[:RESPONDE_A|PERTENECE_A]->(...)<-[:COMENTA|PUBLICA]-(:Usuario)")
    rg = q.resumen_grafo()
    cols = st.columns(6)
    for col, (et, n) in zip(cols, [("Usuarios", rg["nodos"].get("Usuario", 0)), ("Posts", rg["nodos"].get("Post", 0)),
                                    ("Comentarios", rg["nodos"].get("Comentario", 0)),
                                    ("PUBLICA", rg["relaciones"].get("PUBLICA", 0)),
                                    ("COMENTA", rg["relaciones"].get("COMENTA", 0)),
                                    ("RESPONDE_A", rg["relaciones"].get("RESPONDE_A", 0))]):
        col.metric(et, f"{n:,}")
    st.caption("Totales del grafo completo (no dependen de los filtros). El ranking y la red de abajo sí se "
               "filtran por rango y subreddit.")

    a, b = st.columns([2, 3], gap="large")
    with a:
        st.markdown("**Usuarios con más conexiones** · usuarios distintos con los que interactuaron")
        con = q.usuarios_mas_conectados(S, D, H)
        if con.empty:
            st.info("Sin interacciones en el rango.")
        else:
            fig = px.bar(con.sort_values("conexiones"), x="conexiones", y="usuario", orientation="h",
                         color_discrete_sequence=[PALETA["LocalLLaMA"]])
            grafico(estilo(fig, alto=60 + 28 * len(con), leyenda=False), key="con")
    with b:
        if not con.empty:
            centro = st.selectbox("Red de respuestas de:", con["usuario"].tolist())
            ego = q.red_ego(centro, D, H)
            if not ego.empty:
                otros = sorted((set(ego["origen"]) | set(ego["destino"])) - {centro})
                pos = {centro: (0.0, 0.0)}
                for i, u in enumerate(otros):
                    ang = 2 * math.pi * i / len(otros)
                    pos[u] = (math.cos(ang), math.sin(ang))
                fig = go.Figure()
                maxr = ego["respuestas"].max()
                for r in ego.itertuples():
                    (x0, y0), (x1, y1) = pos[r.origen], pos[r.destino]
                    fig.add_trace(go.Scatter(x=[x0, x1], y=[y0, y1], mode="lines", hoverinfo="skip", showlegend=False,
                                             line=dict(color=PALETA["neutro"], width=1 + 3 * r.respuestas / maxr)))
                peso = pd.concat([ego.groupby("origen")["respuestas"].sum(), ego.groupby("destino")["respuestas"].sum()]
                                 ).groupby(level=0).sum()
                xs, ys = zip(*(pos[u] for u in otros))
                fig.add_trace(go.Scatter(x=xs, y=ys, mode="markers", text=otros, showlegend=False,
                                         customdata=[int(peso.get(u, 0)) for u in otros],
                                         hovertemplate="%{text}<br>%{customdata} interacciones con " + centro + "<extra></extra>",
                                         marker=dict(size=12, color=PALETA["wallstreetbets"],
                                                     line=dict(width=2, color="rgba(0,0,0,0)"))))
                fig.add_trace(go.Scatter(x=[0], y=[0], mode="markers+text", text=[centro], textposition="top center",
                                         textfont=dict(color=PALETA["texto"], size=13), showlegend=False,
                                         hovertemplate=centro + "<extra></extra>",
                                         marker=dict(size=22, color=PALETA["LocalLLaMA"])))
                fig.update_xaxes(visible=False)
                fig.update_yaxes(visible=False, scaleanchor="x")
                grafico(estilo(fig, alto=420, leyenda=False), key="ego")
                st.caption(f"{len(otros)} usuarios conectados a **{centro}** (máx. 40 aristas). "
                           "Grosor de la línea = cantidad de respuestas.")

# ---------------- Pipeline ----------------
with tab_etl:
    st.caption("Fuente: MongoDB · colección `etl_runs` (una entrada por día procesado por Airflow) y `rechazados`")
    runs = q.etl_runs()
    if not runs.empty:
        runs = runs[(runs["fecha"] >= D) & (runs["fecha"] <= H)]
        largo = runs.melt(id_vars="fecha", value_vars=["comentarios_extraidos", "comentarios_validos"],
                          var_name="serie", value_name="registros")
        largo["serie"] = largo["serie"].map({"comentarios_extraidos": "Comentarios extraídos",
                                             "comentarios_validos": "Comentarios válidos (tras limpieza)"})
        fig = px.line(largo, x="fecha", y="registros", color="serie",
                      color_discrete_sequence=[PALETA["neutro"], PALETA["LocalLLaMA"]])
        st.markdown("**Efecto de la limpieza por día** (la diferencia son comentarios de bots descartados)")
        grafico(estilo(fig), key="etl")
        a, b = st.columns([1, 2])
        with a:
            st.markdown("**Rechazados por motivo**")
            rech = q.rechazados_por_motivo(S, D, H)
            if not rech.empty:
                st.dataframe(rech.pivot_table(index="subreddit", columns="motivo", values="registros", fill_value=0),
                             use_container_width=True)
        with b:
            st.markdown("**Bitácora de ejecuciones**")
            st.dataframe(runs.sort_values("fecha", ascending=False), hide_index=True, use_container_width=True, height=300)
