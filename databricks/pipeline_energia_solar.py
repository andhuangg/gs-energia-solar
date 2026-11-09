# Databricks notebook source
# MAGIC %md
# MAGIC # Energia solar distribuída no Brasil: pipeline Medallion em PySpark
# MAGIC
# MAGIC Reimplementação do pipeline local (`pipeline/bronze.py`, `silver.py`, `gold.py`) no Databricks Free Edition, com Apache Spark e Delta Lake.
# MAGIC
# MAGIC | Etapa | O que acontece | Tabelas Delta |
# MAGIC |---|---|---|
# MAGIC | Bronze | download das 8 fontes públicas para um Volume do Unity Catalog e carga fiel com colunas de linhagem | `bronze_*` |
# MAGIC | Silver | filtro de usinas solares, tipagem, padronização de categorias, deduplicação, integração IBGE + INPE + tarifas | `silver_instalacoes_solares`, `silver_municipios` |
# MAGIC | Gold | indicadores por município, série mensal e consolidado por UF | `gold_municipios`, `gold_serie_mensal`, `gold_ufs` |
# MAGIC
# MAGIC Execute com **Run all**. O notebook é idempotente: arquivos já baixados não são baixados de novo e as tabelas são sobrescritas.

# COMMAND ----------

CATALOGO = "workspace"
SCHEMA = "energia_solar"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.raw")
RAW = f"/Volumes/{CATALOGO}/{SCHEMA}/raw"


def tabela(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: aquisição e carga fiel
# MAGIC
# MAGIC Os arquivos originais ficam no Volume `raw` (camada de pouso). A Bronze guarda os dados como vieram da fonte, com três exceções documentadas:
# MAGIC colunas pessoais do titular (nome, CPF/CNPJ e CEP) são descartadas por minimização de dados (LGPD), colunas que a ANEEL descontinuou e vêm sempre nulas são removidas,
# MAGIC e a primeira linha das respostas da API SIDRA (cabeçalho descritivo) é filtrada.

# COMMAND ----------

import os
import zipfile

import pandas as pd
import requests
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import NullType

ANEEL = "https://dadosabertos.aneel.gov.br/dataset"
SIDRA = "https://apisidra.ibge.gov.br/values"
FONTES = {
    "aneel_gd": (f"{ANEEL}/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/cd29f6eb-e08d-4db7-b6fb-ed6e3b682d27/download/empreendimento-geracao-distribuida.parquet", "parquet"),
    "aneel_tarifas": (f"{ANEEL}/5a583f3e-1646-4f67-bf0f-69db4203e89e/resource/9af99ef4-376a-4493-a075-5e48ab4da51b/download/tarifas-homologadas-distribuidoras-energia-eletrica.parquet", "parquet"),
    "labren_irradiacao": ("https://labren.ccst.inpe.br/projetos/atlas_2017/TILTED_LATITUDE_sedes-munic_%28csv%29.zip", "zip"),
    "ibge_populacao": (f"{SIDRA}/t/4714/n6/all/v/93,6318/p/2022", "json"),
    "ibge_domicilios": (f"{SIDRA}/t/4712/n6/all/v/381/p/2022", "json"),
    "ibge_apartamentos": (f"{SIDRA}/t/6326/n6/all/v/1000381/p/2022/c125/3247", "json"),
    "ibge_pib": (f"{SIDRA}/t/5938/n6/all/v/37/p/2023", "json"),
    "ibge_municipios": ("https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado", "json"),
}
COLUNAS_PESSOAIS = ["NumCPFCNPJ", "NomTitularEmpreendimento", "CodCEP"]


def baixar(nome: str, url: str, extensao: str) -> str:
    destino = f"{RAW}/{nome}.{extensao}"
    if not os.path.exists(destino):
        resposta = requests.get(url, timeout=600)
        resposta.raise_for_status()
        with open(destino, "wb") as arquivo:
            arquivo.write(resposta.content)
    return destino


def ler(nome: str, caminho: str, extensao: str):
    if extensao == "parquet":
        df = spark.read.parquet(caminho)
        vazias = [c.name for c in df.schema if isinstance(c.dataType, NullType)]
        return df.drop(*[c for c in COLUNAS_PESSOAIS + vazias if c in df.columns])
    if extensao == "zip":
        pasta = f"{RAW}/{nome}"
        with zipfile.ZipFile(caminho) as z:
            z.extractall(pasta)
        return spark.read.options(header=True, sep=";", encoding="UTF-8").csv(f"{pasta}/*/*.csv")
    df = spark.read.option("multiLine", True).json(caminho)
    if "NC" in df.columns:
        df = df.filter(F.col("NC") != "Nível Territorial (Código)")
    return df


for nome, (url, extensao) in FONTES.items():
    caminho = baixar(nome, url, extensao)
    df = (ler(nome, caminho, extensao)
          .withColumn("_fonte", F.lit(url))
          .withColumn("_arquivo", F.col("_metadata.file_path"))
          .withColumn("_ingerido_em", F.current_timestamp()))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela(f"bronze_{nome}"))
    print(f"bronze_{nome:<20} {spark.table(tabela(f'bronze_{nome}')).count():>10,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: instalações solares
# MAGIC
# MAGIC Filtro de usinas fotovoltaicas (`UFV`), tipagem, padronização das classes de consumo e modalidades, descarte de potência inválida, datas sentinela (ano 1900) viram nulo
# MAGIC e deduplicação pelo código do empreendimento com `Window` + `row_number`.

# COMMAND ----------

CLASSES = {"REBR": "Residencial baixa renda", "Consumo Próprio": "Outros", "Serviço Público": "Poder Público", "Iluminação pública": "Poder Público"}
MODALIDADES = {"Geracao na propria UC": "Própria UC", "Auto consumo remoto": "Autoconsumo remoto", "Compartilhada": "Compartilhada", "Condomínio": "Múltiplas UC"}


def mapa(dicionario: dict):
    return F.create_map(*[F.lit(x) for par in dicionario.items() for x in par])


classe = F.trim("DscClasseConsumo")
mais_recente = Window.partitionBy("id_empreendimento").orderBy(F.col("data_conexao").desc_nulls_last())

instalacoes = (spark.table(tabela("bronze_aneel_gd"))
    .filter(F.col("SigTipoGeracao") == "UFV")
    .select(
        F.col("CodEmpreendimento").alias("id_empreendimento"),
        F.col("NumCNPJDistribuidora").cast("string").alias("cnpj_distribuidora"),
        F.trim("SigAgente").alias("distribuidora"),
        F.col("CodMunicipioIbge").cast("int").alias("cod_municipio"),
        F.col("SigUF").alias("uf"),
        F.coalesce(mapa(CLASSES)[classe], classe).alias("classe"),
        F.coalesce(mapa(MODALIDADES)[F.col("DscModalidadeHabilitado")], F.lit("Não informada")).alias("modalidade"),
        F.col("DscPorte").alias("porte"),
        F.col("MdaPotenciaInstaladaKW").cast("double").alias("potencia_kw"),
        F.when(F.year("DthAtualizaCadastralEmpreend") >= 2009, F.col("DthAtualizaCadastralEmpreend")).alias("data_conexao"),
        "_ingerido_em")
    .filter(F.col("potencia_kw") > 0)
    .withColumn("_ordem", F.row_number().over(mais_recente))
    .filter("_ordem = 1").drop("_ordem"))

instalacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela("silver_instalacoes_solares"))
instalacoes = spark.table(tabela("silver_instalacoes_solares"))
print(f"{instalacoes.count():,} instalações solares na Silver")
instalacoes.groupBy("classe").agg(F.count("*").alias("sistemas"), F.round(F.sum("potencia_kw") / 1e6, 2).alias("gw")).orderBy(F.desc("sistemas")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: municípios (integração IBGE + INPE + ANEEL)
# MAGIC
# MAGIC - IBGE: `pivot` das tabelas SIDRA (uma linha por município e variável vira uma coluna por variável). Na SIDRA, `-` significa zero e `...` significa dado indisponível.
# MAGIC - INPE/LABREN: o Atlas Solar não tem código IBGE. A junção é feita por nome normalizado (sem acento, minúsculo) + UF, com uma tabela de exceções
# MAGIC   (por exemplo, Porto Alegre aparece no arquivo do INPE com o estado errado) e, para o que sobra, a menor distância de Levenshtein dentro da mesma UF.
# MAGIC - ANEEL: a distribuidora principal de cada município (a que tem mais conexões) define a tarifa residencial B1 vigente.

# COMMAND ----------

ALIASES = {
    "riograndedosul|portoalegre": "riograndedonorte|portoalegre",
    "riograndedonorte|campogrande": "riograndedonorte|augustosevero",
    "riograndedonorte|assu": "riograndedonorte|acu",
    "riograndedonorte|arez": "riograndedonorte|ares",
    "tocantins|saovalerio": "tocantins|saovaleriodanatividade",
    "tocantins|tabocao": "tocantins|fortalezadotabocao",
    "roraima|saoluizdoanaua": "roraima|saoluiz",
}


def normalizar(coluna):
    sem_acento = F.translate(F.lower(coluna), "áàâãäéèêëíìîïóòôõöúùûüçñ", "aaaaaeeeeiiiiooooouuuucn")
    return F.regexp_replace(sem_acento, "[^a-z0-9]", "")


def sidra(nome: str, variaveis: dict):
    valor = F.expr("try_cast(CASE WHEN V = '-' THEN '0' ELSE V END AS double)")
    return (spark.table(tabela(f"bronze_{nome}"))
            .groupBy(F.col("D1C").cast("int").alias("cod_municipio"))
            .pivot("D2C", list(variaveis)).agg(F.first(valor))
            .withColumnsRenamed(variaveis))


localidades = (spark.table(tabela("bronze_ibge_municipios"))
    .select(F.col("`municipio-id`").cast("int").alias("cod_municipio"), F.col("`municipio-nome`").alias("municipio"),
            F.col("`UF-sigla`").alias("uf"), F.col("`UF-nome`").alias("uf_nome"), F.col("`regiao-nome`").alias("regiao"),
            F.col("`regiao-intermediaria-nome`").alias("regiao_intermediaria"))
    .dropDuplicates(["cod_municipio"]))

ibge = (localidades
    .join(sidra("ibge_populacao", {"93": "populacao", "6318": "area_km2"}), "cod_municipio", "left")
    .join(sidra("ibge_domicilios", {"381": "domicilios"}), "cod_municipio", "left")
    .join(sidra("ibge_apartamentos", {"1000381": "pct_apartamentos"}), "cod_municipio", "left")
    .join(sidra("ibge_pib", {"37": "pib_mil_reais"}), "cod_municipio", "left"))

labren = (spark.table(tabela("bronze_labren_irradiacao"))
    .select(F.concat_ws("|", normalizar(F.col("STATE")), normalizar(F.col("NAME"))).alias("chave_labren"),
            F.col("LAT").cast("double").alias("lat"), F.col("LON").cast("double").alias("lon"),
            F.col("ANNUAL").cast("double").alias("irradiacao_wh_m2_dia"))
    .dropDuplicates(["chave_labren"]))

aliases = spark.createDataFrame(list(ALIASES.items()), "chave string, chave_alias string")
referencia = (localidades
    .select("cod_municipio", F.concat_ws("|", normalizar(F.col("uf_nome")), normalizar(F.col("municipio"))).alias("chave"))
    .join(aliases, "chave", "left")
    .withColumn("chave_labren", F.coalesce("chave_alias", "chave")))

exato = (referencia.join(labren, "chave_labren")
    .withColumn("match", F.when(F.col("chave_alias").isNotNull(), "manual").otherwise("exato")))
pendentes = referencia.join(exato.select("cod_municipio"), "cod_municipio", "left_anti")
livres = labren.join(exato.select("chave_labren"), "chave_labren", "left_anti")

estado = lambda c: F.split(c, "\\|").getItem(0)
nome = lambda c: F.split(c, "\\|").getItem(1)
aproximado = (pendentes.alias("p")
    .join(livres.alias("l"), estado(F.col("p.chave_labren")) == estado(F.col("l.chave_labren")))
    .withColumn("distancia", F.levenshtein(nome(F.col("p.chave_labren")), nome(F.col("l.chave_labren"))))
    .filter("distancia <= 3")
    .withColumn("_ordem", F.row_number().over(Window.partitionBy("p.cod_municipio").orderBy("distancia")))
    .filter("_ordem = 1")
    .select("p.cod_municipio", "l.lat", "l.lon", "l.irradiacao_wh_m2_dia", F.lit("aproximado").alias("match")))

irradiacao = exato.select("cod_municipio", "lat", "lon", "irradiacao_wh_m2_dia", "match").unionByName(aproximado)
irradiacao.groupBy("match").count().show()

# COMMAND ----------

ultima_tarifa = Window.partitionBy("cnpj_distribuidora").orderBy(F.desc("DatInicioVigencia"))
tarifas = (spark.table(tabela("bronze_aneel_tarifas"))
    .filter("DscBaseTarifaria = 'Tarifa de Aplicação' AND DscSubGrupo = 'B1' AND DscModalidadeTarifaria = 'Convencional' "
            "AND DscClasse = 'Residencial' AND DscSubClasse = 'Residencial' AND DscDetalhe = 'Não se aplica' "
            "AND DscUnidadeTerciaria = 'MWh' AND DatInicioVigencia <= current_date()")
    .select(F.col("NumCNPJDistribuidora").cast("string").alias("cnpj_distribuidora"), "DatInicioVigencia",
            ((F.col("VlrTUSD") + F.col("VlrTE")) / 1000).alias("tarifa_b1_r_kwh"))
    .withColumn("_ordem", F.row_number().over(ultima_tarifa))
    .filter("_ordem = 1")
    .select("cnpj_distribuidora", "tarifa_b1_r_kwh", F.col("DatInicioVigencia").alias("tarifa_vigente_desde")))

mais_conexoes = Window.partitionBy("cod_municipio").orderBy(F.desc("n"))
principal = (instalacoes.groupBy("cod_municipio", "cnpj_distribuidora", "distribuidora").agg(F.count("*").alias("n"))
    .withColumn("_ordem", F.row_number().over(mais_conexoes)).filter("_ordem = 1")
    .select("cod_municipio", "cnpj_distribuidora", F.col("distribuidora").alias("distribuidora_principal")))

municipios = (ibge
    .join(irradiacao.drop("match"), "cod_municipio", "left")
    .join(principal, "cod_municipio", "left")
    .join(tarifas, "cnpj_distribuidora", "left"))
municipios.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela("silver_municipios"))

spark.sql(f"""
    SELECT count(*) AS municipios,
           count_if(populacao IS NULL) AS sem_populacao,
           count_if(irradiacao_wh_m2_dia IS NULL) AS sem_irradiacao,
           count_if(tarifa_b1_r_kwh IS NULL) AS sem_tarifa
    FROM {tabela('silver_municipios')}
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: indicadores
# MAGIC
# MAGIC O registro da ANEEL tem atraso: os últimos meses aparecem incompletos. O corte estável é o último mês com pelo menos 80% da mediana dos 12 meses anteriores,
# MAGIC e todas as janelas de 12 meses usam esse corte. A Gold é escrita em Spark SQL sobre as tabelas Silver.

# COMMAND ----------

LIMIAR_ATRASO = 0.8
PERFORMANCE_RATIO = 0.75

meses = (instalacoes.filter(F.col("data_conexao").isNotNull())
    .groupBy(F.trunc("data_conexao", "month").alias("mes")).count().orderBy("mes").toPandas())
meses["referencia"] = meses["count"].rolling(12).median().shift(1)
while meses["count"].iloc[-1] < LIMIAR_ATRASO * meses["referencia"].iloc[-1]:
    meses = meses.iloc[:-1]
FIM = (pd.Timestamp(meses["mes"].iloc[-1]) + pd.DateOffset(months=1)).date().isoformat()
print("período estável até (exclusivo):", FIM)

# COMMAND ----------

S, M = tabela("silver_instalacoes_solares"), tabela("silver_municipios")

spark.sql(f"""
CREATE OR REPLACE TABLE {tabela('gold_municipios')} AS
WITH agg AS (
    SELECT cod_municipio,
        count(*) AS sistemas,
        sum(potencia_kw) AS potencia_kw,
        count(*) FILTER (WHERE classe IN ('Residencial', 'Residencial baixa renda')) AS sistemas_residenciais,
        count(*) FILTER (WHERE classe IN ('Residencial', 'Residencial baixa renda') AND modalidade = 'Própria UC') AS telhados_residenciais,
        count(*) FILTER (WHERE classe = 'Residencial baixa renda') AS sistemas_baixa_renda,
        sum(potencia_kw) FILTER (WHERE modalidade = 'Autoconsumo remoto') AS kw_remoto,
        sum(potencia_kw) FILTER (WHERE classe = 'Rural') AS kw_rural,
        sum(potencia_kw) FILTER (WHERE data_conexao >= add_months(DATE'{FIM}', -12) AND data_conexao < DATE'{FIM}') AS kw_12m,
        sum(potencia_kw) FILTER (WHERE data_conexao >= add_months(DATE'{FIM}', -24) AND data_conexao < add_months(DATE'{FIM}', -12)) AS kw_12m_anterior,
        min(year(data_conexao)) AS ano_primeira_conexao
    FROM {S} GROUP BY cod_municipio
)
SELECT m.cod_municipio, m.municipio, m.uf, m.regiao, m.regiao_intermediaria,
    m.populacao, m.domicilios, m.pct_apartamentos, m.area_km2, m.lat, m.lon,
    m.distribuidora_principal, m.tarifa_b1_r_kwh,
    m.irradiacao_wh_m2_dia / 1000 AS irradiacao_kwh_m2_dia,
    m.pib_mil_reais * 1000 / m.populacao AS pib_per_capita,
    coalesce(a.sistemas, 0) AS sistemas,
    coalesce(a.potencia_kw, 0) AS potencia_kw,
    coalesce(a.sistemas_residenciais, 0) AS sistemas_residenciais,
    coalesce(a.telhados_residenciais, 0) AS telhados_residenciais,
    coalesce(a.sistemas_baixa_renda, 0) AS sistemas_baixa_renda,
    a.ano_primeira_conexao,
    coalesce(a.potencia_kw, 0) * 1000 / m.populacao AS watts_por_habitante,
    coalesce(a.telhados_residenciais, 0) * 1000.0 / m.domicilios AS telhados_solares_por_mil_dom,
    a.potencia_kw / a.sistemas AS potencia_media_kw,
    100.0 * a.sistemas_baixa_renda / nullif(a.sistemas_residenciais, 0) AS pct_baixa_renda,
    100.0 * coalesce(a.kw_remoto, 0) / a.potencia_kw AS pct_kw_autoconsumo_remoto,
    100.0 * coalesce(a.kw_rural, 0) / a.potencia_kw AS pct_kw_rural,
    coalesce(a.kw_12m, 0) AS kw_ultimos_12m,
    coalesce(a.kw_12m_anterior, 0) AS kw_12m_anterior,
    100.0 * (coalesce(a.kw_12m, 0) - a.kw_12m_anterior) / nullif(a.kw_12m_anterior, 0) AS crescimento_12m_pct,
    m.irradiacao_wh_m2_dia / 1000 * 365 * {PERFORMANCE_RATIO} AS geracao_kwh_por_kwp_ano,
    m.irradiacao_wh_m2_dia / 1000 * 365 * {PERFORMANCE_RATIO} * m.tarifa_b1_r_kwh AS economia_r_por_kwp_ano,
    coalesce(a.potencia_kw, 0) * m.irradiacao_wh_m2_dia / 1000 * 365 * {PERFORMANCE_RATIO} / 1000 AS geracao_estimada_mwh_ano
FROM {M} m LEFT JOIN agg a USING (cod_municipio)
WHERE m.populacao IS NOT NULL
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {tabela('gold_serie_mensal')} AS
SELECT trunc(s.data_conexao, 'month') AS mes, m.uf, m.regiao, s.classe, s.modalidade,
       count(*) AS sistemas, sum(s.potencia_kw) AS potencia_kw
FROM {S} s JOIN {M} m USING (cod_municipio)
WHERE s.data_conexao IS NOT NULL AND s.data_conexao < DATE'{FIM}'
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {tabela('gold_ufs')} AS
SELECT uf, regiao,
    sum(populacao) AS populacao,
    sum(sistemas) AS sistemas,
    sum(potencia_kw) / 1000 AS potencia_mw,
    sum(potencia_kw) * 1000 / sum(populacao) AS watts_por_habitante,
    sum(telhados_solares_por_mil_dom * domicilios) / sum(domicilios) AS telhados_solares_por_mil_dom,
    100.0 * sum(sistemas_baixa_renda) / sum(sistemas_residenciais) AS pct_baixa_renda,
    sum(irradiacao_kwh_m2_dia * populacao) / sum(populacao) AS irradiacao_media_kwh_m2_dia,
    sum(pib_per_capita * populacao) / sum(populacao) AS pib_per_capita,
    sum(tarifa_b1_r_kwh * populacao) / sum(populacao) AS tarifa_media_r_kwh,
    sum(economia_r_por_kwp_ano * populacao) / sum(populacao) AS economia_r_por_kwp_ano,
    100.0 * sum(kw_ultimos_12m) / sum(potencia_kw) AS pct_kw_ultimos_12m,
    100.0 * (sum(kw_ultimos_12m) - sum(kw_12m_anterior)) / sum(kw_12m_anterior) AS crescimento_12m_pct
FROM {tabela('gold_municipios')}
GROUP BY ALL
ORDER BY watts_por_habitante DESC
""")

for nome in ["gold_municipios", "gold_serie_mensal", "gold_ufs"]:
    print(f"{nome:<20} {spark.table(tabela(nome)).count():>8,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checagens de qualidade e conferência com o pipeline local
# MAGIC Os totais devem bater com `data/silver/_qualidade.json` e `data/gold/_checagens.json` do pipeline local.

# COMMAND ----------

conferencia = spark.sql(f"""
    SELECT (SELECT count(*) FROM {S}) AS sistemas_silver,
           (SELECT sum(sistemas) FROM {tabela('gold_municipios')}) AS sistemas_gold,
           (SELECT round(sum(potencia_kw) / 1000, 1) FROM {S}) AS mw_silver,
           (SELECT round(sum(potencia_kw) / 1000, 1) FROM {tabela('gold_municipios')}) AS mw_gold,
           (SELECT count(*) FROM {S} s LEFT ANTI JOIN {M} m USING (cod_municipio)) AS instalacoes_sem_municipio,
           (SELECT count(*) - count(DISTINCT id_empreendimento) FROM {S}) AS duplicados
""")
conferencia.show()
linha = conferencia.first()
assert linha.duplicados == 0
assert linha.instalacoes_sem_municipio == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas analíticas em Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao,
# MAGIC        round(sum(potencia_kw) * 1000 / sum(populacao)) AS watts_por_habitante,
# MAGIC        round(sum(irradiacao_kwh_m2_dia * populacao) / sum(populacao), 2) AS irradiacao_media,
# MAGIC        round(sum(pib_per_capita * populacao) / sum(populacao)) AS pib_per_capita,
# MAGIC        round(100 * sum(potencia_kw) / sum(sum(potencia_kw)) OVER (), 1) AS pct_potencia_brasil,
# MAGIC        round(100 * sum(populacao) / sum(sum(populacao)) OVER (), 1) AS pct_populacao_brasil
# MAGIC FROM workspace.energia_solar.gold_municipios
# MAGIC GROUP BY regiao
# MAGIC ORDER BY watts_por_habitante DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(mes) AS ano, sum(sistemas) AS sistemas, round(sum(potencia_kw) / 1000) AS mw
# MAGIC FROM workspace.energia_solar.gold_serie_mensal
# MAGIC GROUP BY ALL
# MAGIC ORDER BY ano

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT municipio, uf, populacao, round(irradiacao_kwh_m2_dia, 2) AS irradiacao,
# MAGIC        round(pib_per_capita) AS pib_per_capita, round(watts_por_habitante) AS w_hab,
# MAGIC        round(economia_r_por_kwp_ano) AS economia_r_kwp_ano
# MAGIC FROM workspace.energia_solar.gold_municipios
# MAGIC WHERE populacao >= 100000 AND irradiacao_kwh_m2_dia >= 5.4
# MAGIC ORDER BY watts_por_habitante ASC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como o Spark executa: plano físico
# MAGIC O `explain` mostra o filtro `SigTipoGeracao = 'UFV'` empurrado para a leitura da tabela Delta (predicate pushdown), a leitura só das colunas usadas (column pruning)
# MAGIC e a agregação em duas fases (parcial em cada partição, depois final após o shuffle por `cod_municipio`).

# COMMAND ----------

(spark.table(tabela("bronze_aneel_gd"))
    .filter(F.col("SigTipoGeracao") == "UFV")
    .groupBy("CodMunicipioIbge")
    .agg(F.sum("MdaPotenciaInstaladaKW").alias("kw"))
    .explain(mode="formatted"))
