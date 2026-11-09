import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap, LogNorm

from config import DOCS, GOLD, IMG

SUPERFICIE = "#fcfcfb"
TEXTO = "#0b0b0b"
TEXTO_2 = "#52514e"
MUDO = "#898781"
GRADE = "#e1e0d9"
EIXO = "#c3c2b7"
AZUL = "#2a78d6"
LARANJA = "#eb6834"
CINZA = "#c3c2b7"
AZUIS = LinearSegmentedColormap.from_list("azuis", ["#cde2fb", "#86b6ef", "#3987e5", "#256abf", "#104281", "#0d366b"])
LARANJAS = LinearSegmentedColormap.from_list("laranjas", ["#fde4d6", "#f6b08f", "#eb6834", "#c24a1c", "#7a2c0c"])

plt.rcParams.update({
    "font.family": ["Helvetica Neue", "Arial", "DejaVu Sans"],
    "font.size": 10,
    "figure.facecolor": SUPERFICIE,
    "axes.facecolor": SUPERFICIE,
    "axes.edgecolor": EIXO,
    "axes.labelcolor": TEXTO_2,
    "axes.titlecolor": TEXTO,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.color": GRADE,
    "grid.linewidth": 0.6,
    "xtick.color": MUDO,
    "ytick.color": MUDO,
    "legend.frameon": False,
    "savefig.dpi": 160,
    "savefig.bbox": "tight",
})


def fmt(v: float, casas: int = 0) -> str:
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def salvar(fig, nome: str) -> None:
    fig.savefig(IMG / f"{nome}.png")
    plt.close(fig)


def evolucao(res: dict) -> None:
    ev = pd.DataFrame(res["evolucao_anual"])
    ev = ev[ev["ano"] >= 2015]
    fig, ax = plt.subplots(figsize=(9, 4))
    cores = [AZUL if a < ev["ano"].max() else "#86b6ef" for a in ev["ano"]]
    ax.bar(ev["ano"].astype(str), ev["potencia_mw"] / 1000, color=cores, width=0.7)
    ax.grid(axis="x", visible=False)
    ax.set_ylabel("GW conectados no ano")
    ax.set_title("Potência solar distribuída conectada por ano (GW)")
    for x, y in zip(ev["ano"].astype(str), ev["potencia_mw"] / 1000):
        if y > 1:
            ax.text(x, y + 0.15, fmt(y, 1), ha="center", color=TEXTO_2, fontsize=9)
    ax.annotate("Corrida antes da Lei 14.300\n(fim da isenção integral)", xy=("2022", 8.9), xytext=("2017", 8.2),
                color=TEXTO_2, fontsize=9, arrowprops={"arrowstyle": "-", "color": MUDO})
    ax.text(ev["ano"].astype(str).iloc[-1], 0.3, "parcial", ha="center", color=TEXTO, fontsize=8, rotation=90)
    salvar(fig, "01_evolucao_anual")


def mapas(m: pd.DataFrame) -> None:
    m = m.dropna(subset=["lat", "lon"])
    fig, axes = plt.subplots(1, 2, figsize=(11, 5.6))
    for ax in axes:
        ax.set_aspect("equal")
        ax.axis("off")
    a = axes[0].scatter(m["lon"], m["lat"], c=m["irradiacao_kwh_m2_dia"], cmap=LARANJAS, s=3, linewidths=0)
    axes[0].set_title("Onde há sol\nirradiação no plano inclinado (kWh/m²/dia)")
    fig.colorbar(a, ax=axes[0], shrink=0.6, orientation="horizontal", pad=0.02)
    w = m["watts_por_habitante"].clip(lower=10)
    b = axes[1].scatter(m["lon"], m["lat"], c=w, cmap=AZUIS, norm=LogNorm(10, 3000), s=3, linewidths=0)
    axes[1].set_title("Onde há painéis\npotência instalada por habitante (W/hab, escala log)")
    fig.colorbar(b, ax=axes[1], shrink=0.6, orientation="horizontal", pad=0.02)
    salvar(fig, "02_mapas_sol_vs_paineis")


def regioes(res: dict) -> None:
    r = pd.DataFrame(res["regioes"]).sort_values("irradiacao_media", ascending=False)
    paineis = [
        ("irradiacao_media", "Irradiação média (kWh/m²/dia)", 2),
        ("pib_per_capita", "PIB per capita (R$)", 0),
        ("watts_por_habitante", "Potência solar por habitante (W)", 0),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.6), sharey=True)
    for ax, (col, titulo, casas) in zip(axes, paineis):
        cores = [LARANJA if reg == "Nordeste" else AZUL for reg in r["regiao"]]
        ax.barh(r["regiao"], r[col], color=cores, height=0.65)
        ax.invert_yaxis()
        ax.grid(axis="y", visible=False)
        ax.set_title(titulo, fontsize=10)
        for y, v in enumerate(r[col]):
            ax.text(v, y, " " + fmt(v, casas), va="center", color=TEXTO_2, fontsize=9)
        ax.set_xlim(0, r[col].max() * 1.25)
    fig.suptitle("Nordeste: mais sol do país, menor renda e menor adoção por habitante", x=0.01, y=1.04, ha="left", fontweight="bold", color=TEXTO)
    salvar(fig, "03_regioes")


def ufs() -> None:
    u = pd.read_parquet(GOLD / "ufs.parquet").sort_values("watts_por_habitante")
    fig, ax = plt.subplots(figsize=(8, 7))
    cores = [LARANJA if reg == "Nordeste" else AZUL for reg in u["regiao"]]
    ax.barh(u["uf"], u["watts_por_habitante"], color=cores, height=0.7)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("W por habitante")
    ax.set_title("Potência solar distribuída por habitante, por UF")
    for y, v in enumerate(u["watts_por_habitante"]):
        ax.text(v, y, " " + fmt(v), va="center", color=TEXTO_2, fontsize=8)
    ax.scatter([], [], color=LARANJA, marker="s", label="Nordeste")
    ax.scatter([], [], color=AZUL, marker="s", label="Demais regiões")
    ax.legend(loc="lower right")
    salvar(fig, "04_ufs")


def dispersao(m: pd.DataFrame, res: dict) -> None:
    corr = {(c["fator"], c["indicador"]): c["spearman"] for c in res["correlacoes"]}
    d = m[m["watts_por_habitante"] > 0]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    pares = [("pib_per_capita", "PIB per capita (R$, escala log)", True), ("irradiacao_kwh_m2_dia", "Irradiação (kWh/m²/dia)", False)]
    for ax, (col, rotulo, log) in zip(axes, pares):
        ax.scatter(d[col], d["watts_por_habitante"], s=4, alpha=0.25, color=AZUL, linewidths=0)
        ax.set_yscale("log")
        if log:
            ax.set_xscale("log")
        ax.set_xlabel(rotulo)
        rho = corr[(col, "watts_por_habitante")]
        ax.set_title(f"Spearman ρ = {fmt(rho, 2)}")
    axes[0].set_ylabel("W por habitante (log)")
    fig.suptitle("Cada ponto é um município: a adoção acompanha a renda, não o sol", x=0.01, y=1.03, ha="left", fontweight="bold", color=TEXTO)
    salvar(fig, "05_renda_vs_sol")


def importancia(res: dict) -> None:
    imp = pd.DataFrame(res["modelo"]["importancia_permutacao"]).iloc[::-1]
    fig, ax = plt.subplots(figsize=(7, 3.4))
    ax.barh(imp["fator"], imp["queda_r2"], color=AZUL, height=0.6)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Queda no R² ao embaralhar o fator")
    ax.set_title("O que explica a adoção residencial (importância por permutação)")
    for y, v in enumerate(imp["queda_r2"]):
        ax.text(v, y, " " + fmt(v, 2), va="center", color=TEXTO_2, fontsize=9)
    salvar(fig, "06_importancia_fatores")


def clusters(res: dict) -> None:
    c = pd.read_parquet(GOLD / "clusters.parquet").dropna(subset=["lat", "lon"])
    perfis = pd.DataFrame(res["clusters"]["perfil"]).sort_values("watts_por_habitante", ascending=False)
    fig, axes = plt.subplots(1, len(perfis), figsize=(15, 4.2))
    for ax, (_, p) in zip(axes, perfis.iterrows()):
        ax.set_aspect("equal")
        ax.axis("off")
        ax.scatter(c["lon"], c["lat"], s=1.5, color="#e1e0d9", linewidths=0)
        sel = c[c["perfil"] == p["perfil"]]
        ax.scatter(sel["lon"], sel["lat"], s=2.5, color=AZUL, linewidths=0)
        ax.set_title(f"{p['perfil']}\n{fmt(p['municipios'])} municípios · {fmt(p['watts_por_habitante'])} W/hab", fontsize=9.5)
    fig.suptitle("Perfis de municípios (KMeans, k = 5)", x=0.01, ha="left", fontweight="bold", color=TEXTO)
    salvar(fig, "07_clusters")


def lacuna() -> None:
    lac = pd.read_parquet(GOLD / "lacuna_adocao.parquet")
    top = lac[lac["populacao"] >= 100_000].nlargest(12, "telhados_faltantes").iloc[::-1]
    y = np.arange(len(top))
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    ax.barh(y + 0.2, top["telhados_esperados_por_mil_dom"], height=0.38, color=CINZA, label="Esperado pelo modelo")
    ax.barh(y - 0.2, top["telhados_solares_por_mil_dom"], height=0.38, color=AZUL, label="Observado")
    ax.set_yticks(y, [f"{m} ({u})" for m, u in zip(top["municipio"], top["uf"])])
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Telhados solares residenciais por mil domicílios")
    ax.set_title("Maiores lacunas de adoção (municípios com 100 mil+ habitantes)")
    limite = top["telhados_esperados_por_mil_dom"].max() * 1.3
    ax.set_xlim(0, limite)
    for yy, f in zip(y, top["telhados_faltantes"]):
        ax.text(limite, yy, f"faltam {fmt(f)}", ha="right", va="center", color=TEXTO_2, fontsize=8.5)
    ax.legend(loc="lower right", bbox_to_anchor=(1, -0.22), ncol=2)
    salvar(fig, "08_lacuna_municipios")


def main() -> None:
    IMG.mkdir(parents=True, exist_ok=True)
    res = json.loads((DOCS / "resultados.json").read_text())
    m = pd.read_parquet(GOLD / "municipios.parquet")
    evolucao(res)
    mapas(m)
    regioes(res)
    ufs()
    dispersao(m, res)
    importancia(res)
    clusters(res)
    lacuna()
    print(sorted(p.name for p in IMG.glob("*.png")))


if __name__ == "__main__":
    main()
