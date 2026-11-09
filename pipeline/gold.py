import json

import duckdb
import pandas as pd

from config import GOLD, PERFORMANCE_RATIO, SILVER

LIMIAR_ATRASO = 0.8


def corte_estavel(con) -> str:
    meses = con.sql("""
        select date_trunc('month', data_conexao)::date as mes, count(*) as n
        from gd where data_conexao is not null group by 1 order by 1
    """).df()
    meses["referencia"] = meses["n"].rolling(12).median().shift(1)
    while meses["n"].iloc[-1] < LIMIAR_ATRASO * meses["referencia"].iloc[-1]:
        meses = meses.iloc[:-1]
    return str(meses["mes"].iloc[-1] + pd.DateOffset(months=1))[:10]


def main() -> None:
    GOLD.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"create view gd as select * from '{SILVER}/instalacoes_solares.parquet'")
    con.sql(f"create view mun as select * from '{SILVER}/municipios.parquet'")
    fim = corte_estavel(con)
    con.sql(f"set variable fim = date '{fim}'")

    con.sql(f"""
        create table municipios as
        with agg as (
            select cod_municipio,
                count(*) as sistemas,
                sum(potencia_kw) as potencia_kw,
                count(*) filter (classe in ('Residencial', 'Residencial baixa renda')) as sistemas_residenciais,
                count(*) filter (classe in ('Residencial', 'Residencial baixa renda') and modalidade = 'Própria UC') as telhados_residenciais,
                count(*) filter (classe = 'Residencial baixa renda') as sistemas_baixa_renda,
                sum(potencia_kw) filter (modalidade = 'Autoconsumo remoto') as kw_remoto,
                sum(potencia_kw) filter (classe = 'Rural') as kw_rural,
                sum(potencia_kw) filter (data_conexao >= getvariable('fim') - interval 12 month) as kw_12m,
                sum(potencia_kw) filter (data_conexao >= getvariable('fim') - interval 24 month
                                         and data_conexao < getvariable('fim') - interval 12 month) as kw_12m_anterior,
                min(year(data_conexao)) as ano_primeira_conexao
            from gd group by 1
        )
        select m.cod_municipio, m.municipio, m.uf, m.regiao, m.regiao_intermediaria,
            m.populacao, m.domicilios, m.pct_apartamentos, m.area_km2, m.lat, m.lon,
            m.distribuidora_principal, m.tarifa_b1_r_kwh,
            m.irradiacao_wh_m2_dia / 1000 as irradiacao_kwh_m2_dia,
            m.pib_mil_reais * 1000 / m.populacao as pib_per_capita,
            coalesce(a.sistemas, 0) as sistemas,
            coalesce(a.potencia_kw, 0) as potencia_kw,
            coalesce(a.sistemas_residenciais, 0) as sistemas_residenciais,
            coalesce(a.telhados_residenciais, 0) as telhados_residenciais,
            coalesce(a.sistemas_baixa_renda, 0) as sistemas_baixa_renda,
            a.ano_primeira_conexao,
            coalesce(a.potencia_kw, 0) * 1000 / m.populacao as watts_por_habitante,
            coalesce(a.telhados_residenciais, 0) * 1000.0 / m.domicilios as telhados_solares_por_mil_dom,
            a.potencia_kw / a.sistemas as potencia_media_kw,
            100.0 * a.sistemas_baixa_renda / nullif(a.sistemas_residenciais, 0) as pct_baixa_renda,
            100.0 * coalesce(a.kw_remoto, 0) / a.potencia_kw as pct_kw_autoconsumo_remoto,
            100.0 * coalesce(a.kw_rural, 0) / a.potencia_kw as pct_kw_rural,
            coalesce(a.kw_12m, 0) as kw_ultimos_12m,
            coalesce(a.kw_12m_anterior, 0) as kw_12m_anterior,
            100.0 * (coalesce(a.kw_12m, 0) - a.kw_12m_anterior) / nullif(a.kw_12m_anterior, 0) as crescimento_12m_pct,
            m.irradiacao_wh_m2_dia / 1000 * 365 * {PERFORMANCE_RATIO} as geracao_kwh_por_kwp_ano,
            m.irradiacao_wh_m2_dia / 1000 * 365 * {PERFORMANCE_RATIO} * m.tarifa_b1_r_kwh as economia_r_por_kwp_ano,
            coalesce(a.potencia_kw, 0) * m.irradiacao_wh_m2_dia / 1000 * 365 * {PERFORMANCE_RATIO} / 1000 as geracao_estimada_mwh_ano
        from mun m left join agg a using (cod_municipio)
        where m.populacao is not null
    """)

    con.sql("""
        create table serie_mensal as
        select date_trunc('month', data_conexao)::date as mes, m.uf, m.regiao, classe, modalidade,
               count(*) as sistemas, sum(potencia_kw) as potencia_kw
        from gd join mun m using (cod_municipio)
        where data_conexao is not null and data_conexao < getvariable('fim')
        group by all
    """)

    con.sql("""
        create table ufs as
        select uf, regiao,
            sum(populacao) as populacao,
            sum(sistemas) as sistemas,
            sum(potencia_kw) / 1000 as potencia_mw,
            sum(potencia_kw) * 1000 / sum(populacao) as watts_por_habitante,
            sum(telhados_solares_por_mil_dom * domicilios) / sum(domicilios) as telhados_solares_por_mil_dom,
            100.0 * sum(sistemas_baixa_renda) / sum(sistemas_residenciais) as pct_baixa_renda,
            sum(irradiacao_kwh_m2_dia * populacao) / sum(populacao) as irradiacao_media_kwh_m2_dia,
            sum(pib_per_capita * populacao) / sum(populacao) as pib_per_capita,
            sum(tarifa_b1_r_kwh * populacao) / sum(populacao) as tarifa_media_r_kwh,
            sum(economia_r_por_kwp_ano * populacao) / sum(populacao) as economia_r_por_kwp_ano,
            100.0 * (sum(kw_ultimos_12m)) / sum(potencia_kw) as pct_kw_ultimos_12m,
            100.0 * (sum(kw_ultimos_12m) - sum(kw_12m_anterior)) / sum(kw_12m_anterior) as crescimento_12m_pct
        from municipios group by all order by watts_por_habitante desc
    """)

    for tabela in ["municipios", "serie_mensal", "ufs"]:
        con.sql(f"copy {tabela} to '{GOLD}/{tabela}.parquet' (format parquet, compression zstd)")

    total_silver = con.sql("select count(*), sum(potencia_kw) from gd").fetchall()[0]
    total_gold = con.sql("select sum(sistemas), sum(potencia_kw) from municipios").fetchall()[0]
    checagens = {
        "periodo_estavel_ate_exclusivo": fim,
        "sistemas_silver": total_silver[0],
        "sistemas_gold": int(total_gold[0]),
        "sistemas_fora_da_gold": total_silver[0] - int(total_gold[0]),
        "potencia_confere_mw": [round(total_silver[1] / 1000, 1), round(total_gold[1] / 1000, 1)],
        "linhas": {t: con.sql(f"select count(*) from {t}").fetchall()[0][0] for t in ["municipios", "serie_mensal", "ufs"]},
    }
    (GOLD / "_checagens.json").write_text(json.dumps(checagens, indent=2, ensure_ascii=False))
    print(json.dumps(checagens, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
