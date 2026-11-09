import json

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import DOCS, GOLD

AZUL = "#2a78d6"
LARANJA = "#eb6834"
CINZA = "#c3c2b7"
REGIOES = ["Sul", "Sudeste", "Centro-Oeste", "Nordeste", "Norte"]
CORES_REGIAO = dict(zip(REGIOES, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]))
AZUIS = [[0, "#cde2fb"], [0.35, "#86b6ef"], [0.6, "#3987e5"], [0.8, "#1c5cab"], [1, "#0d366b"]]
LAYOUT = dict(
    font=dict(family="system-ui, -apple-system, Segoe UI, sans-serif", size=12, color="#0b0b0b"),
    paper_bgcolor="#fcfcfb", plot_bgcolor="#fcfcfb", margin=dict(l=10, r=10, t=50, b=10),
    hoverlabel=dict(bgcolor="#ffffff", font_color="#0b0b0b"),
)


def br(v: float, casas: int = 0) -> str:
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def figura_mapa(m: pd.DataFrame) -> go.Figure:
    m = m.dropna(subset=["lat", "lon"]).copy()
    m["w_cor"] = m["watts_por_habitante"].clip(lower=10, upper=3000)
    texto = (m["municipio"] + " (" + m["uf"] + ")<br>" + m["watts_por_habitante"].map(br) + " W/hab<br>"
             + m["telhados_solares_por_mil_dom"].map(lambda v: br(v, 1)) + " telhados por mil domicílios<br>"
             + "irradiação " + m["irradiacao_kwh_m2_dia"].map(lambda v: br(v, 2)) + " kWh/m²/dia<br>"
             + "PIB per capita R$ " + m["pib_per_capita"].map(br))
    fig = go.Figure(go.Scattergeo(
        lon=m["lon"], lat=m["lat"], text=texto, hoverinfo="text", mode="markers",
        marker=dict(size=4, color=m["w_cor"], colorscale=AZUIS, cmin=10, cmax=1500,
                    colorbar=dict(title="W/hab", thickness=10, len=0.6)),
    ))
    fig.update_geos(fitbounds="locations", showcountries=True, countrycolor="#e1e0d9", showland=True,
                    landcolor="#f4f4f1", showocean=False, bgcolor="#fcfcfb", showframe=False)
    fig.update_layout(**LAYOUT, title="Potência solar por habitante, por município", height=520)
    return fig


def figura_evolucao() -> go.Figure:
    s = pd.read_parquet(GOLD / "serie_mensal.parquet")
    s["mes"] = pd.to_datetime(s["mes"])
    s = s[s["mes"] >= pd.Timestamp("2018-01-01")]
    g = s.groupby(["mes", "regiao"], as_index=False)["potencia_kw"].sum()
    fig = go.Figure()
    for reg in REGIOES:
        d = g[g["regiao"] == reg]
        fig.add_trace(go.Scatter(x=d["mes"], y=d["potencia_kw"] / 1000, name=reg, mode="lines",
                                 line=dict(width=2, color=CORES_REGIAO[reg]),
                                 hovertemplate="%{y:,.0f} MW<extra>" + reg + "</extra>"))
    fig.update_layout(**LAYOUT, title="Potência conectada por mês e região (MW)", height=380, hovermode="x unified",
                      legend=dict(orientation="h", y=-0.15))
    fig.update_yaxes(gridcolor="#e1e0d9")
    return fig


def figura_ufs() -> go.Figure:
    u = pd.read_parquet(GOLD / "ufs.parquet").sort_values("watts_por_habitante")
    fig = make_subplots(rows=1, cols=2, shared_yaxes=True, horizontal_spacing=0.04,
                        subplot_titles=("W por habitante", "Irradiação média (kWh/m²/dia)"))
    cores = [LARANJA if r == "Nordeste" else AZUL for r in u["regiao"]]
    fig.add_trace(go.Bar(y=u["uf"], x=u["watts_por_habitante"], orientation="h", marker_color=cores,
                         hovertemplate="%{y}: %{x:,.0f} W/hab<extra></extra>"), 1, 1)
    fig.add_trace(go.Bar(y=u["uf"], x=u["irradiacao_media_kwh_m2_dia"], orientation="h", marker_color=cores,
                         hovertemplate="%{y}: %{x:.2f} kWh/m²/dia<extra></extra>"), 1, 2)
    fig.update_layout(**LAYOUT, title="UFs: adoção x potencial solar (laranja = Nordeste)", height=640, showlegend=False)
    fig.update_xaxes(gridcolor="#e1e0d9")
    return fig


def tabela_lacuna() -> str:
    lac = pd.read_parquet(GOLD / "lacuna_adocao.parquet")
    top = lac[lac["populacao"] >= 100_000].nlargest(15, "telhados_faltantes")
    linhas = "".join(
        f"<tr><td>{r.municipio} ({r.uf})</td><td>{br(r.telhados_solares_por_mil_dom, 1)}</td>"
        f"<td>{br(r.telhados_esperados_por_mil_dom, 1)}</td><td>{br(r.telhados_faltantes)}</td>"
        f"<td>R$ {br(r.economia_nao_capturada_r_ano / 1e6, 1)} mi</td></tr>"
        for r in top.itertuples()
    )
    return ("<table><thead><tr><th>Município</th><th>Telhados/mil dom.</th><th>Esperado</th>"
            "<th>Telhados faltantes</th><th>Economia não capturada/ano</th></tr></thead><tbody>" + linhas + "</tbody></table>")


def main() -> None:
    m = pd.read_parquet(GOLD / "municipios.parquet")
    res = json.loads((DOCS / "resultados.json").read_text())
    fim = json.loads((GOLD / "_checagens.json").read_text())["periodo_estavel_ate_exclusivo"]
    corte = (pd.Timestamp(fim) - pd.DateOffset(months=1)).strftime("%m/%Y")
    total_gw = m["potencia_kw"].sum() / 1e6
    sistemas = m["sistemas"].sum()
    kpis = [
        (br(sistemas), "sistemas solares conectados"),
        (br(total_gw, 1) + " GW", "potência instalada"),
        (br(m["potencia_kw"].sum() * 1000 / m["populacao"].sum()), "W por habitante (Brasil)"),
        (br(res["modelo"]["telhados_faltantes_brasil"]), "telhados residenciais abaixo do esperado"),
        ("R$ " + br(res["modelo"]["economia_nao_capturada_bilhoes_r_ano"], 2) + " bi", "economia anual não capturada"),
    ]
    cards = "".join(f'<div class="kpi"><div class="v">{v}</div><div class="l">{l}</div></div>' for v, l in kpis)
    graficos = [figura_mapa(m), figura_evolucao(), figura_ufs()]
    corpo = "".join(f'<section class="card">{f.to_html(full_html=False, include_plotlyjs=False, config={"displaylogo": False})}</section>' for f in graficos)
    html = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Painel Energia Solar</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist-min@2.35.2/plotly.min.js"></script>
<style>
:root {{ --bg:#f9f9f7; --card:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e; --line:#e1e0d9; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:system-ui,-apple-system,"Segoe UI",sans-serif; }}
header {{ padding:24px 16px 8px; max-width:1200px; margin:auto; }}
h1 {{ font-size:22px; margin:0 0 4px; }} header p {{ color:var(--ink2); margin:0; }}
main {{ max-width:1200px; margin:auto; padding:8px 16px 32px; display:grid; gap:16px; }}
.kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }}
.kpi, .card {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; }}
.kpi .v {{ font-size:24px; font-weight:700; }} .kpi .l {{ color:var(--ink2); font-size:13px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; font-variant-numeric:tabular-nums; }}
th, td {{ text-align:right; padding:6px 8px; border-bottom:1px solid var(--line); }}
th:first-child, td:first-child {{ text-align:left; }} th {{ color:var(--ink2); font-weight:600; }}
.nota {{ color:var(--ink2); font-size:12px; }}
</style></head><body>
<header><h1>Energia solar distribuída no Brasil: onde o sol não vira painel</h1>
<p>Fontes: ANEEL (geração distribuída e tarifas), INPE/LABREN (Atlas Solar), IBGE (Censo 2022 e PIB 2023). Série mensal até {corte}.</p></header>
<main><div class="kpis">{cards}</div>{corpo}
<section class="card"><h3>Maiores lacunas de adoção residencial (municípios com 100 mil+ habitantes)</h3>{tabela_lacuna()}
<p class="nota">Esperado: previsão fora da amostra de um modelo Gradient Boosting com renda, irradiação, tarifa, % de apartamentos, porte e densidade.
Economia não capturada: telhados faltantes × 5 kWp (mediana residencial) × economia anual por kWp do município.</p></section>
</main></body></html>"""
    (DOCS / "dashboard.html").write_text(html)
    print("docs/dashboard.html")


if __name__ == "__main__":
    main()
