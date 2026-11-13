"""Ejecuta el dashboard sin navegador (streamlit AppTest) con varios filtros y resume lo que muestra.

Uso:  docker compose exec -w /app dashboard python dashboard/prueba_dashboard.py
"""
import datetime as dt
import json
import os
import sys

from streamlit.testing.v1 import AppTest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # `streamlit run` añade la carpeta; AppTest no

CASOS = {
    "completo": {},
    "solo_LocalLLaMA": {"subs": ["LocalLLaMA"]},
    "solo_septiembre": {"rango": (dt.date(2026, 9, 1), dt.date(2026, 9, 12))},
    "julio_wsb_skincare": {"rango": (dt.date(2026, 7, 1), dt.date(2026, 7, 31)),
                           "subs": ["wallstreetbets", "SkincareAddiction"]},
}


def ejecutar(rango=None, subs=None) -> AppTest:
    at = AppTest.from_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py"), default_timeout=180)
    at.run()
    if rango:
        at.sidebar.date_input[0].set_value(rango).run()
    if subs:
        at.sidebar.multiselect[0].set_value(subs).run()
    return at


def resumen(at: AppTest) -> dict:
    return {
        "excepciones": [e.value for e in at.exception],
        "avisos": [w.value for w in at.warning] + [i.value for i in at.info],
        "pestanas": [t.label for t in at.tabs],
        "metricas": [f"{m.label}: {m.value}" for m in at.metric],  # en orden de aparición (hay etiquetas repetidas)
        "kpis": [h.value for h in at.subheader],
        "avisos_kpi_global": sum("no responde al filtro" in c.value for c in at.caption),
        "graficos": len(at.get("plotly_chart")),
        "tablas": len(at.dataframe),
    }


if __name__ == "__main__":
    casos = {k: v for k, v in CASOS.items() if len(sys.argv) < 2 or k in sys.argv[1:]}
    print(json.dumps({nombre: resumen(ejecutar(**kw)) for nombre, kw in casos.items()},
                     indent=1, ensure_ascii=False, default=str))
