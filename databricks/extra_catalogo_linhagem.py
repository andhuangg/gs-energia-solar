# Databricks notebook source
# MAGIC %md
# MAGIC # Além do proposto: catálogo e linhagem de dados
# MAGIC
# MAGIC Contribuição adicional ao escopo da atividade. Este notebook **não altera nenhum dado**: ele apenas documenta as 13 tabelas Delta
# MAGIC criadas por `pipeline_energia_solar` no Unity Catalog, com descrições de tabelas e colunas e tags de governança.
# MAGIC A linhagem (de onde cada tabela vem) é registrada automaticamente pelo Unity Catalog quando o pipeline roda.
# MAGIC
# MAGIC Execute **depois** do pipeline principal: recriar as tabelas apaga os comentários.

# COMMAND ----------

CATALOGO = "workspace"
SCHEMA = "energia_solar"


def tabela(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def texto(valor: str) -> str:
    return valor.replace("'", "\\'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descrição das tabelas e tags de governança

# COMMAND ----------

TABELAS = {
    "bronze_aneel_gd": ("bronze", "ANEEL", "Cópia fiel da relação de empreendimentos de micro e minigeração distribuída da ANEEL, sem nome, CPF/CNPJ e CEP do titular (minimização LGPD)."),
    "bronze_aneel_tarifas": ("bronze", "ANEEL", "Cópia fiel das tarifas homologadas das distribuidoras de energia (TUSD e TE por subgrupo, classe e vigência)."),
    "bronze_labren_irradiacao": ("bronze", "INPE", "Atlas Brasileiro de Energia Solar 2017 (INPE/LABREN): irradiação média no plano inclinado por sede municipal."),
    "bronze_ibge_populacao": ("bronze", "IBGE", "Censo 2022, tabela SIDRA 4714: população residente e área por município."),
    "bronze_ibge_domicilios": ("bronze", "IBGE", "Censo 2022, tabela SIDRA 4712: domicílios particulares permanentes ocupados por município."),
    "bronze_ibge_apartamentos": ("bronze", "IBGE", "Censo 2022, tabela SIDRA 6326: percentual de domicílios do tipo apartamento por município."),
    "bronze_ibge_pib": ("bronze", "IBGE", "PIB dos Municípios 2023, tabela SIDRA 5938: PIB a preços correntes."),
    "bronze_ibge_municipios": ("bronze", "IBGE", "API de Localidades do IBGE: municípios com UF, região e região intermediária."),
    "silver_instalacoes_solares": ("silver", "ANEEL", "Uma linha por sistema solar fotovoltaico conectado: tipado, categorias padronizadas, deduplicado e com datas sentinela tratadas."),
    "silver_municipios": ("silver", "IBGE, INPE, ANEEL", "Uma linha por município integrando população, domicílios, apartamentos, PIB (IBGE), irradiação (INPE) e tarifa residencial da distribuidora principal (ANEEL)."),
    "gold_municipios": ("gold", "ANEEL, IBGE, INPE", "Indicadores de adoção solar por município: potência por habitante, telhados por mil domicílios, economia por kWp e crescimento em 12 meses."),
    "gold_serie_mensal": ("gold", "ANEEL, IBGE", "Sistemas e potência conectados por mês, UF, região, classe de consumo e modalidade, limitados ao período estável do registro da ANEEL."),
    "gold_ufs": ("gold", "ANEEL, IBGE, INPE", "Consolidado dos indicadores por UF, ponderado por população."),
}

for nome, (camada, fonte, descricao) in TABELAS.items():
    spark.sql(f"COMMENT ON TABLE {tabela(nome)} IS '{texto(descricao)}'")
    spark.sql(f"ALTER TABLE {tabela(nome)} SET TAGS ('camada' = '{camada}', 'fonte' = '{fonte}', 'lgpd' = 'sem_dados_pessoais', 'projeto' = 'gs_energia_solar')")
    print(f"{nome:<28} {camada:<7} {fonte}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dicionário de dados: descrição das colunas da Silver e da Gold

# COMMAND ----------

COLUNAS = {
    "silver_instalacoes_solares": {
        "id_empreendimento": "Código do empreendimento na ANEEL (chave única)",
        "cnpj_distribuidora": "CNPJ da distribuidora à qual o sistema está conectado",
        "distribuidora": "Sigla da distribuidora",
        "cod_municipio": "Código IBGE do município (7 dígitos)",
        "uf": "Sigla da UF",
        "classe": "Classe de consumo padronizada (Residencial, Residencial baixa renda, Comercial, Rural, Industrial, Poder Público, Outros)",
        "modalidade": "Própria UC, Autoconsumo remoto, Compartilhada ou Múltiplas UC",
        "porte": "Microgeração (até 75 kW) ou Minigeração",
        "potencia_kw": "Potência instalada em kW",
        "data_conexao": "Data da última atualização cadastral, usada como data de conexão (nulo quando anterior a 2009)",
        "_ingerido_em": "Momento da ingestão na Bronze (linhagem)",
    },
    "silver_municipios": {
        "cod_municipio": "Código IBGE do município (chave única)",
        "municipio": "Nome do município",
        "uf": "Sigla da UF",
        "uf_nome": "Nome da UF",
        "regiao": "Grande região",
        "regiao_intermediaria": "Região geográfica intermediária do IBGE",
        "populacao": "População residente, Censo 2022",
        "area_km2": "Área territorial em km²",
        "domicilios": "Domicílios particulares permanentes ocupados, Censo 2022",
        "pct_apartamentos": "Percentual de domicílios do tipo apartamento, Censo 2022",
        "pib_mil_reais": "PIB 2023 em mil reais",
        "lat": "Latitude da sede municipal (INPE)",
        "lon": "Longitude da sede municipal (INPE)",
        "irradiacao_wh_m2_dia": "Irradiação média anual no plano inclinado, Wh/m²/dia (INPE)",
        "cnpj_distribuidora": "CNPJ da distribuidora com mais conexões no município",
        "distribuidora_principal": "Sigla da distribuidora com mais conexões no município",
        "tarifa_b1_r_kwh": "Tarifa residencial B1 convencional vigente (TUSD + TE), R$/kWh, sem impostos",
        "tarifa_vigente_desde": "Início da vigência da tarifa",
    },
    "gold_municipios": {
        "cod_municipio": "Código IBGE do município (chave única)",
        "municipio": "Nome do município",
        "uf": "Sigla da UF",
        "regiao": "Grande região",
        "populacao": "População residente, Censo 2022",
        "domicilios": "Domicílios particulares permanentes ocupados",
        "pct_apartamentos": "Percentual de domicílios do tipo apartamento",
        "irradiacao_kwh_m2_dia": "Irradiação média no plano inclinado, kWh/m²/dia",
        "pib_per_capita": "PIB 2023 dividido pela população 2022, em reais",
        "tarifa_b1_r_kwh": "Tarifa residencial B1 vigente, R$/kWh",
        "sistemas": "Quantidade de sistemas solares conectados",
        "potencia_kw": "Potência solar total instalada, kW",
        "telhados_residenciais": "Sistemas residenciais com geração na própria unidade (telhado próprio)",
        "watts_por_habitante": "Potência instalada por habitante, W",
        "telhados_solares_por_mil_dom": "Telhados solares residenciais por mil domicílios",
        "pct_baixa_renda": "Percentual de sistemas residenciais da classe baixa renda (subnotificado: só 27 distribuidoras registram)",
        "pct_kw_autoconsumo_remoto": "Percentual da potência em autoconsumo remoto (fazendas solares)",
        "pct_kw_rural": "Percentual da potência na classe rural",
        "kw_ultimos_12m": "Potência conectada nos 12 meses do período estável",
        "crescimento_12m_pct": "Variação da potência conectada nos últimos 12 meses contra os 12 anteriores, %",
        "geracao_kwh_por_kwp_ano": "Geração anual estimada por kWp instalado (irradiação x 365 x 0,75)",
        "economia_r_por_kwp_ano": "Economia anual estimada por kWp instalado, R$ (geração x tarifa)",
        "geracao_estimada_mwh_ano": "Geração anual estimada do parque solar do município, MWh",
    },
    "gold_ufs": {
        "uf": "Sigla da UF (chave única)",
        "watts_por_habitante": "Potência instalada por habitante, W",
        "telhados_solares_por_mil_dom": "Telhados solares residenciais por mil domicílios",
        "irradiacao_media_kwh_m2_dia": "Irradiação média ponderada pela população",
        "pib_per_capita": "PIB per capita ponderado pela população",
        "economia_r_por_kwp_ano": "Economia anual por kWp ponderada pela população, R$",
        "crescimento_12m_pct": "Variação da potência conectada nos últimos 12 meses, %",
    },
}

total = 0
for nome, colunas in COLUNAS.items():
    for coluna, descricao in colunas.items():
        spark.sql(f"ALTER TABLE {tabela(nome)} ALTER COLUMN {coluna} COMMENT '{texto(descricao)}'")
        total += 1
print(f"{total} colunas documentadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultando o catálogo com SQL
# MAGIC Os metadados ficam disponíveis no `information_schema`, que pode ser consultado como qualquer tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.table_name, t.comment, collect_list(concat(g.tag_name, '=', g.tag_value)) AS tags
# MAGIC FROM workspace.information_schema.tables t
# MAGIC LEFT JOIN workspace.information_schema.table_tags g
# MAGIC   ON g.schema_name = t.table_schema AND g.table_name = t.table_name
# MAGIC WHERE t.table_schema = 'energia_solar'
# MAGIC GROUP BY t.table_name, t.comment
# MAGIC ORDER BY t.table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name, data_type, comment
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema = 'energia_solar' AND table_name = 'gold_municipios' AND comment IS NOT NULL
# MAGIC ORDER BY ordinal_position

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linhagem registrada pelo Unity Catalog
# MAGIC A tabela de sistema `system.access.table_lineage` guarda, para cada leitura e escrita, qual tabela ou arquivo originou cada tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT coalesce(source_table_name, source_path) AS origem, target_table_name AS destino
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_schema = 'energia_solar'
# MAGIC ORDER BY destino, origem
