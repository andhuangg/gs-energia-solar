import difflib
import json
import re
import unicodedata

import duckdb
import pandas as pd

from config import BRONZE, SILVER


ALIASES = {
    "riograndedosul|portoalegre": "riograndedonorte|portoalegre",
    "riograndedonorte|campogrande": "riograndedonorte|augustosevero",
    "riograndedonorte|assu": "riograndedonorte|acu",
    "riograndedonorte|arez": "riograndedonorte|ares",
    "tocantins|saovalerio": "tocantins|saovaleriodanatividade",
    "tocantins|tabocao": "tocantins|fortalezadotabocao",
    "roraima|saoluizdoanaua": "roraima|saoluiz",
}


def normalizar(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", texto.lower())


def escalar(con, sql: str):
    return con.sql(sql).fetchall()[0][0]


def sidra(con, nome: str, variaveis: dict) -> pd.DataFrame:
    df = con.sql(f"select D1C, D2C, V from '{BRONZE}/{nome}.parquet'").df()
    df = df.pivot(index="D1C", columns="D2C", values="V").rename(columns=variaveis)
    df.index = df.index.astype(int)
    df.index.name = "cod_municipio"
    return df.replace("-", "0").apply(pd.to_numeric, errors="coerce")


def irradiacao_por_municipio(con, municipios: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    lab = con.sql(f"select NAME, STATE, LAT, LON, ANNUAL from '{BRONZE}/labren_irradiacao.parquet'").df()
    lab["chave"] = lab["STATE"].map(normalizar) + "|" + lab["NAME"].map(normalizar)
    lab = lab.drop_duplicates("chave")
    ref = municipios[["cod_municipio", "municipio", "uf_nome"]].copy()
    ref["chave"] = ref["uf_nome"].map(normalizar) + "|" + ref["municipio"].map(normalizar)
    ref["chave"] = ref["chave"].map(ALIASES).fillna(ref["chave"])

    exato = ref.merge(lab, on="chave", how="inner")
    pendentes = ref[~ref["cod_municipio"].isin(exato["cod_municipio"])]
    livres = lab[~lab["chave"].isin(exato["chave"])]
    aproximados = []
    usados = set()
    for _, linha in pendentes.iterrows():
        estado, nome = linha["chave"].split("|")
        do_estado = livres[livres["chave"].str.startswith(estado + "|") & ~livres["chave"].isin(usados)]
        nomes = {c.split("|")[1]: c for c in do_estado["chave"]}
        achou = difflib.get_close_matches(nome, list(nomes), n=1, cutoff=0.85)
        if achou:
            chave = nomes[achou[0]]
            usados.add(chave)
            alvo = do_estado[do_estado["chave"] == chave].iloc[0]
            aproximados.append({**linha.to_dict(), **alvo.drop("chave").to_dict(), "chave_labren": chave})
    aprox = pd.DataFrame(aproximados)
    exato["match"] = exato["chave"].isin(ALIASES.values()).map({True: "manual", False: "exato"})
    junto = pd.concat([exato, aprox.assign(match="aproximado")], ignore_index=True)
    junto = junto.rename(columns={"LAT": "lat", "LON": "lon", "ANNUAL": "irradiacao_wh_m2_dia"})
    junto[["lat", "lon", "irradiacao_wh_m2_dia"]] = junto[["lat", "lon", "irradiacao_wh_m2_dia"]].astype(float)
    qualidade = {
        "labren_match_exato": int((junto["match"] == "exato").sum()),
        "labren_match_manual": int((junto["match"] == "manual").sum()),
        "labren_match_aproximado": int((junto["match"] == "aproximado").sum()),
        "labren_sem_match": int(len(ref) - len(junto)),
        "labren_exemplos_aproximados": aprox[["municipio", "NAME"]].head(10).values.tolist() if len(aprox) else [],
    }
    return junto[["cod_municipio", "lat", "lon", "irradiacao_wh_m2_dia", "match"]], qualidade


def instalacoes(con) -> dict:
    con.sql(f"""
        create or replace table gd as
        with base as (
            select
                CodEmpreendimento as id_empreendimento,
                cast(NumCNPJDistribuidora as varchar) as cnpj_distribuidora,
                trim(SigAgente) as distribuidora,
                cast(CodMunicipioIbge as integer) as cod_municipio,
                SigUF as uf,
                case trim(DscClasseConsumo)
                    when 'REBR' then 'Residencial baixa renda'
                    when 'Consumo Próprio' then 'Outros'
                    when 'Serviço Público' then 'Poder Público'
                    when 'Iluminação pública' then 'Poder Público'
                    else trim(DscClasseConsumo)
                end as classe,
                case DscModalidadeHabilitado
                    when 'Geracao na propria UC' then 'Própria UC'
                    when 'Auto consumo remoto' then 'Autoconsumo remoto'
                    when 'Compartilhada' then 'Compartilhada'
                    when 'Condomínio' then 'Múltiplas UC'
                    else 'Não informada'
                end as modalidade,
                DscPorte as porte,
                SigTipoConsumidor as tipo_consumidor,
                cast(MdaPotenciaInstaladaKW as double) as potencia_kw,
                cast(QtdUCRecebeCredito as integer) as ucs_credito,
                case when year(DthAtualizaCadastralEmpreend) >= 2009 then DthAtualizaCadastralEmpreend end as data_conexao,
                AnmPeriodoReferencia as periodo_referencia,
                _ingerido_em
            from '{BRONZE}/aneel_gd.parquet'
            where SigTipoGeracao = 'UFV'
        )
        select * from base
        where potencia_kw > 0
        qualify row_number() over (partition by id_empreendimento order by data_conexao desc nulls last) = 1
    """)
    total = escalar(con, f"select count(*) from '{BRONZE}/aneel_gd.parquet'")
    solar = escalar(con, f"select count(*) from '{BRONZE}/aneel_gd.parquet' where SigTipoGeracao = 'UFV'")
    final = escalar(con, "select count(*) from gd")
    sem_data = escalar(con, "select count(*) from gd where data_conexao is null")
    return {
        "gd_bronze": total,
        "gd_nao_solar_removidos": total - solar,
        "gd_potencia_invalida_ou_duplicado": solar - final,
        "gd_silver": final,
        "gd_sem_data_conexao": sem_data,
    }


def tarifas(con) -> pd.DataFrame:
    return con.sql(f"""
        with b1 as (
            select cast(NumCNPJDistribuidora as varchar) as cnpj_distribuidora, SigAgente,
                   DatInicioVigencia, (VlrTUSD + VlrTE) / 1000 as tarifa_r_kwh
            from '{BRONZE}/aneel_tarifas.parquet'
            where DscBaseTarifaria = 'Tarifa de Aplicação' and DscSubGrupo = 'B1'
              and DscModalidadeTarifaria = 'Convencional' and DscClasse = 'Residencial'
              and DscSubClasse = 'Residencial' and DscDetalhe = 'Não se aplica'
              and DscUnidadeTerciaria = 'MWh'
              and DatInicioVigencia <= current_date
        )
        select cnpj_distribuidora, any_value(SigAgente) as distribuidora_tarifa,
               arg_max(tarifa_r_kwh, DatInicioVigencia) as tarifa_b1_r_kwh,
               max(DatInicioVigencia) as tarifa_vigente_desde
        from b1 group by 1
    """).df()


def municipios(con) -> pd.DataFrame:
    loc = con.sql(f"""
        select distinct cast("municipio-id" as integer) as cod_municipio, "municipio-nome" as municipio,
               "UF-sigla" as uf, "UF-nome" as uf_nome, "regiao-nome" as regiao,
               "regiao-intermediaria-nome" as regiao_intermediaria
        from '{BRONZE}/ibge_municipios.parquet'
    """).df().drop_duplicates("cod_municipio")
    pop = sidra(con, "ibge_populacao", {"93": "populacao", "6318": "area_km2"})
    dom = sidra(con, "ibge_domicilios", {"381": "domicilios"})
    apto = sidra(con, "ibge_apartamentos", {"1000381": "pct_apartamentos"})
    pib = sidra(con, "ibge_pib", {"37": "pib_mil_reais"})
    for tabela in [pop, dom, apto, pib]:
        loc = loc.merge(tabela, on="cod_municipio", how="left")
    return loc


def main() -> None:
    SILVER.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    qualidade = instalacoes(con)

    mun = municipios(con)
    irr, q_irr = irradiacao_por_municipio(con, mun)
    qualidade.update(q_irr)
    mun = mun.merge(irr, on="cod_municipio", how="left")

    principal = con.sql("""
        select cod_municipio, arg_max(cnpj_distribuidora, n) as cnpj_distribuidora, arg_max(distribuidora, n) as distribuidora_principal
        from (select cod_municipio, cnpj_distribuidora, distribuidora, count(*) n from gd group by all)
        group by 1
    """).df()
    tar = tarifas(con)
    mun = mun.merge(principal, on="cod_municipio", how="left").merge(tar, on="cnpj_distribuidora", how="left")

    con.register("mun", mun)
    gd_sem_municipio = escalar(con, "select count(*) from gd anti join mun using (cod_municipio)")
    qualidade.update({
        "municipios_ibge": len(mun),
        "gd_sem_municipio_ibge": gd_sem_municipio,
        "municipios_sem_populacao": int(mun["populacao"].isna().sum()),
        "municipios_sem_pib": int(mun["pib_mil_reais"].isna().sum()),
        "municipios_sem_irradiacao": int(mun["irradiacao_wh_m2_dia"].isna().sum()),
        "municipios_sem_tarifa": int(mun["tarifa_b1_r_kwh"].isna().sum()),
        "tarifa_b1_min_max": [round(float(tar["tarifa_b1_r_kwh"].min()), 3), round(float(tar["tarifa_b1_r_kwh"].max()), 3)],
    })

    con.sql(f"copy gd to '{SILVER}/instalacoes_solares.parquet' (format parquet, compression zstd)")
    mun.to_parquet(SILVER / "municipios.parquet", index=False)
    (SILVER / "_qualidade.json").write_text(json.dumps(qualidade, indent=2, ensure_ascii=False, default=str))
    for k, v in qualidade.items():
        print(f"{k:<36} {v}")


if __name__ == "__main__":
    main()
