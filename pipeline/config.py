from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DADOS = RAIZ / "data"
RAW = DADOS / "raw"
BRONZE = DADOS / "bronze"
SILVER = DADOS / "silver"
GOLD = DADOS / "gold"
DOCS = RAIZ / "docs"
IMG = DOCS / "img"

ANEEL = "https://dadosabertos.aneel.gov.br/dataset"
SIDRA = "https://apisidra.ibge.gov.br/values"
LABREN = "https://labren.ccst.inpe.br/projetos/atlas_2017"

FONTES = {
    "aneel_gd": {
        "url": f"{ANEEL}/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/cd29f6eb-e08d-4db7-b6fb-ed6e3b682d27/download/empreendimento-geracao-distribuida.parquet",
        "formato": "parquet",
        "descricao": "ANEEL, relação de empreendimentos de micro e minigeração distribuída",
    },
    "aneel_tarifas": {
        "url": f"{ANEEL}/5a583f3e-1646-4f67-bf0f-69db4203e89e/resource/9af99ef4-376a-4493-a075-5e48ab4da51b/download/tarifas-homologadas-distribuidoras-energia-eletrica.parquet",
        "formato": "parquet",
        "descricao": "ANEEL, tarifas homologadas das distribuidoras",
    },
    "labren_irradiacao": {
        "url": f"{LABREN}/TILTED_LATITUDE_sedes-munic_%28csv%29.zip",
        "formato": "zip_csv",
        "descricao": "INPE/LABREN, Atlas Brasileiro de Energia Solar 2017, irradiação no plano inclinado por sede municipal",
    },
    "ibge_populacao": {
        "url": f"{SIDRA}/t/4714/n6/all/v/93,6318/p/2022",
        "formato": "sidra",
        "descricao": "IBGE Censo 2022, população residente e área (tabela 4714)",
    },
    "ibge_domicilios": {
        "url": f"{SIDRA}/t/4712/n6/all/v/381/p/2022",
        "formato": "sidra",
        "descricao": "IBGE Censo 2022, domicílios particulares permanentes ocupados (tabela 4712)",
    },
    "ibge_apartamentos": {
        "url": f"{SIDRA}/t/6326/n6/all/v/1000381/p/2022/c125/3247",
        "formato": "sidra",
        "descricao": "IBGE Censo 2022, percentual de domicílios do tipo apartamento (tabela 6326)",
    },
    "ibge_pib": {
        "url": f"{SIDRA}/t/5938/n6/all/v/37/p/2023",
        "formato": "sidra",
        "descricao": "IBGE PIB dos Municípios 2023 (tabela 5938)",
    },
    "ibge_municipios": {
        "url": "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado",
        "formato": "json",
        "descricao": "IBGE API de Localidades, municípios com UF e regiões",
    },
}

COLUNAS_PESSOAIS = ["NumCPFCNPJ", "NomTitularEmpreendimento", "CodCEP"]
PERFORMANCE_RATIO = 0.75
