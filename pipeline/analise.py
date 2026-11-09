import json

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.cluster import KMeans
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score, silhouette_score
from sklearn.model_selection import GroupKFold, KFold, cross_val_predict, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from config import DOCS, GOLD, SILVER

SEMENTE = 42
FATORES = {
    "log_pib_per_capita": "PIB per capita (log)",
    "irradiacao_kwh_m2_dia": "Irradiação solar",
    "tarifa_b1_r_kwh": "Tarifa residencial",
    "pct_apartamentos": "% de apartamentos",
    "log_populacao": "População (log)",
    "log_densidade": "Densidade demográfica (log)",
}
PERFIL = ["log_watts_hab", "log_pib_per_capita", "irradiacao_kwh_m2_dia", "pct_kw_rural", "pct_kw_autoconsumo_remoto", "pct_kw_12m"]


def base() -> pd.DataFrame:
    m = pd.read_parquet(GOLD / "municipios.parquet")
    m = m.dropna(subset=["irradiacao_kwh_m2_dia", "tarifa_b1_r_kwh"]).copy()
    m["log_pib_per_capita"] = np.log(m["pib_per_capita"])
    m["log_populacao"] = np.log(m["populacao"])
    m["log_densidade"] = np.log(m["populacao"] / m["area_km2"])
    m["log_watts_hab"] = np.log1p(m["watts_por_habitante"])
    m["alvo"] = np.log1p(m["telhados_solares_por_mil_dom"])
    m["pct_kw_12m"] = 100 * m["kw_ultimos_12m"] / m["potencia_kw"].where(m["potencia_kw"] > 0)
    return m


def correlacoes(m: pd.DataFrame) -> list[dict]:
    linhas = []
    for col in ["pib_per_capita", "pct_apartamentos", "irradiacao_kwh_m2_dia", "tarifa_b1_r_kwh", "economia_r_por_kwp_ano", "pct_kw_rural", "populacao"]:
        for alvo in ["watts_por_habitante", "telhados_solares_por_mil_dom"]:
            r = spearmanr(m[col], m[alvo])
            linhas.append({"fator": col, "indicador": alvo, "spearman": round(float(r.statistic), 3), "p_valor": float(r.pvalue)})
    return linhas


def regioes(m: pd.DataFrame) -> list[dict]:
    g = m.groupby("regiao").apply(lambda d: pd.Series({
        "pct_populacao": d["populacao"].sum(),
        "pct_potencia": d["potencia_kw"].sum(),
        "irradiacao_media": np.average(d["irradiacao_kwh_m2_dia"], weights=d["populacao"]),
        "watts_por_habitante": d["potencia_kw"].sum() * 1000 / d["populacao"].sum(),
        "pib_per_capita": np.average(d["pib_per_capita"], weights=d["populacao"]),
    }), include_groups=False)
    g["pct_populacao"] = 100 * g["pct_populacao"] / g["pct_populacao"].sum()
    g["pct_potencia"] = 100 * g["pct_potencia"] / g["pct_potencia"].sum()
    return g.round(2).reset_index().to_dict("records")


def evolucao() -> list[dict]:
    return duckdb.sql(f"""
        select year(mes) as ano, sum(sistemas) as sistemas, round(sum(potencia_kw) / 1000, 1) as potencia_mw
        from '{GOLD}/serie_mensal.parquet' group by 1 order by 1
    """).df().to_dict("records")


def avaliar(modelo, X, y, cv, grupos=None) -> dict:
    pred = cross_val_predict(modelo, X, y, cv=cv, groups=grupos)
    return {
        "r2": round(float(r2_score(y, pred)), 3),
        "mae_log": round(float(mean_absolute_error(y, pred)), 3),
        "mae_telhados_por_mil_dom": round(float(mean_absolute_error(np.expm1(y), np.expm1(pred))), 1),
    }, pred


def modelo_lacuna(m: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    X, y = m[list(FATORES)], m["alvo"]
    kfold = KFold(5, shuffle=True, random_state=SEMENTE)
    por_uf = GroupKFold(5)
    gbm = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, min_samples_leaf=30, random_state=SEMENTE)
    modelos = {
        "baseline_media": DummyRegressor(),
        "regressao_linear": make_pipeline(StandardScaler(), LinearRegression()),
        "gradient_boosting": gbm,
    }
    metricas = {}
    for nome, modelo in modelos.items():
        metricas[nome] = {"kfold": avaliar(modelo, X, y, kfold)[0], "validacao_por_uf": avaliar(modelo, X, y, por_uf, m["uf"])[0]}
    _, pred = avaliar(gbm, X, y, kfold)

    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=SEMENTE)
    imp = permutation_importance(gbm.fit(Xtr, ytr), Xte, yte, n_repeats=10, random_state=SEMENTE, scoring="r2")
    importancia = sorted(
        [{"fator": FATORES[c], "queda_r2": round(float(v), 3)} for c, v in zip(FATORES, imp.importances_mean)],
        key=lambda d: -d["queda_r2"],
    )

    kwp_residencial = duckdb.sql(f"""
        select median(potencia_kw) from '{SILVER}/instalacoes_solares.parquet'
        where classe like 'Residencial%' and modalidade = 'Própria UC'
    """).fetchall()[0][0]
    m = m.copy()
    m["telhados_esperados_por_mil_dom"] = np.expm1(pred)
    m["razao_adocao"] = m["telhados_solares_por_mil_dom"] / m["telhados_esperados_por_mil_dom"]
    m["telhados_faltantes"] = ((m["telhados_esperados_por_mil_dom"] - m["telhados_solares_por_mil_dom"]).clip(lower=0) * m["domicilios"] / 1000).round()
    m["economia_nao_capturada_r_ano"] = m["telhados_faltantes"] * kwp_residencial * m["economia_r_por_kwp_ano"]
    resumo = {
        "metricas": metricas,
        "importancia_permutacao": importancia,
        "kwp_mediano_residencial": round(float(kwp_residencial), 2),
        "telhados_faltantes_brasil": int(m["telhados_faltantes"].sum()),
        "economia_nao_capturada_bilhoes_r_ano": round(float(m["economia_nao_capturada_r_ano"].sum() / 1e9), 2),
        "faltantes_por_regiao": m.groupby("regiao")["telhados_faltantes"].sum().astype(int).to_dict(),
    }
    return m, resumo


def nomear(perfil: pd.DataFrame) -> dict:
    p = perfil.set_index("cluster")
    nomes = {p["pct_kw_12m"].idxmax(): "Expansão recente", p["pct_kw_rural"].idxmax(): "Agro solar"}
    resto = p.drop(index=list(nomes))
    nomes[resto["watts_por_habitante"].idxmax()] = "Consolidados de alta renda"
    resto = p.drop(index=list(nomes))
    nomes[resto["pib_per_capita"].idxmin()] = "Muito sol, baixa renda"
    for c in p.index.difference(list(nomes)):
        nomes[c] = "Urbanos de baixa adoção"
    return {int(k): v for k, v in nomes.items()}


def clusters(m: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    d = m[m["sistemas"] >= 10].dropna(subset=PERFIL).copy()
    X = StandardScaler().fit_transform(d[PERFIL])
    silhuetas = {k: float(silhouette_score(X, KMeans(k, n_init=10, random_state=SEMENTE).fit_predict(X), sample_size=3000, random_state=SEMENTE)) for k in range(3, 8)}
    k = max(silhuetas, key=silhuetas.get)
    d["cluster"] = KMeans(k, n_init=10, random_state=SEMENTE).fit_predict(X)
    perfil = d.groupby("cluster").agg(
        municipios=("cod_municipio", "size"),
        populacao_mi=("populacao", lambda s: round(s.sum() / 1e6, 1)),
        watts_por_habitante=("watts_por_habitante", "median"),
        pib_per_capita=("pib_per_capita", "median"),
        irradiacao=("irradiacao_kwh_m2_dia", "median"),
        pct_kw_rural=("pct_kw_rural", "median"),
        pct_kw_remoto=("pct_kw_autoconsumo_remoto", "median"),
        pct_kw_12m=("pct_kw_12m", "median"),
        regiao_principal=("regiao", lambda s: s.mode().iat[0]),
        pct_nordeste=("regiao", lambda s: round(100 * (s == "Nordeste").mean(), 1)),
    ).round(2).reset_index()
    nomes = nomear(perfil)
    perfil["perfil"] = perfil["cluster"].map(nomes)
    d["perfil"] = d["cluster"].map(nomes)
    return d, {"silhuetas": {k: round(v, 3) for k, v in silhuetas.items()}, "k": k, "perfil": perfil.to_dict("records")}


def main() -> None:
    m = base()
    resultados = {
        "correlacoes": correlacoes(m),
        "regioes": regioes(m),
        "evolucao_anual": evolucao(),
    }
    lac, resultados["modelo"] = modelo_lacuna(m)
    cl, resultados["clusters"] = clusters(lac)

    cols = ["cod_municipio", "municipio", "uf", "regiao", "populacao", "domicilios", "pct_apartamentos", "lat", "lon", "pib_per_capita",
            "irradiacao_kwh_m2_dia", "economia_r_por_kwp_ano", "watts_por_habitante", "telhados_solares_por_mil_dom",
            "telhados_esperados_por_mil_dom", "razao_adocao", "telhados_faltantes", "economia_nao_capturada_r_ano"]
    lac[cols].to_parquet(GOLD / "lacuna_adocao.parquet", index=False)
    cl[["cod_municipio", "municipio", "uf", "regiao", "lat", "lon", "populacao", "cluster", "perfil"] + PERFIL].to_parquet(GOLD / "clusters.parquet", index=False)
    DOCS.mkdir(exist_ok=True)
    (DOCS / "resultados.json").write_text(json.dumps(resultados, indent=2, ensure_ascii=False, default=str))
    print(json.dumps({k: resultados[k] for k in ["regioes", "modelo", "clusters"]}, indent=1, ensure_ascii=False, default=str))
    print(pd.DataFrame(resultados["correlacoes"]).to_string())
    top = lac[lac["populacao"] >= 50_000].sort_values("telhados_faltantes", ascending=False).head(15)
    print(top[["municipio", "uf", "populacao", "pct_apartamentos", "telhados_solares_por_mil_dom", "telhados_esperados_por_mil_dom", "razao_adocao", "telhados_faltantes", "economia_r_por_kwp_ano"]].round(2).to_string())


if __name__ == "__main__":
    main()
