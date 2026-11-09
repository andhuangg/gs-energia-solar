import argparse
import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from config import BRONZE, COLUNAS_PESSOAIS, FONTES, RAW

EXTENSOES = {"parquet": "parquet", "zip_csv": "zip", "sidra": "json", "json": "json"}


def baixar(nome: str, fonte: dict, atualizar: bool) -> tuple[bytes, str]:
    RAW.mkdir(parents=True, exist_ok=True)
    arquivo = RAW / f"{nome}.{EXTENSOES[fonte['formato']]}"
    if atualizar or not arquivo.exists():
        resp = requests.get(fonte["url"], timeout=600)
        resp.raise_for_status()
        arquivo.write_bytes(resp.content)
    conteudo = arquivo.read_bytes()
    return conteudo, hashlib.sha256(conteudo).hexdigest()


def para_tabela(nome: str, fonte: dict, conteudo: bytes) -> pa.Table:
    formato = fonte["formato"]
    if formato == "parquet":
        tabela = pq.read_table(io.BytesIO(conteudo))
        vazias = [f.name for f in tabela.schema if pa.types.is_null(f.type)]
        remover = [c for c in COLUNAS_PESSOAIS if c in tabela.column_names] + vazias
        return tabela.drop_columns(remover)
    if formato == "zip_csv":
        with zipfile.ZipFile(io.BytesIO(conteudo)) as z:
            csv = next(n for n in z.namelist() if n.endswith(".csv"))
            df = pd.read_csv(z.open(csv), sep=";", dtype=str)
        return pa.Table.from_pandas(df, preserve_index=False)
    registros = json.loads(conteudo)
    if formato == "sidra":
        registros = registros[1:]
    df = pd.DataFrame(registros).astype(str)
    return pa.Table.from_pandas(df, preserve_index=False)


def ingerir(nome: str, fonte: dict, atualizar: bool, ingerido_em: str) -> dict:
    conteudo, sha = baixar(nome, fonte, atualizar)
    tabela = para_tabela(nome, fonte, conteudo)
    n = tabela.num_rows
    tabela = tabela.append_column("_fonte", pa.array([fonte["url"]] * n, pa.string()))
    tabela = tabela.append_column("_ingerido_em", pa.array([ingerido_em] * n, pa.string()))
    tabela = tabela.append_column("_sha256_origem", pa.array([sha] * n, pa.string()))
    destino = BRONZE / f"{nome}.parquet"
    pq.write_table(tabela, destino, compression="zstd")
    return {
        "fonte": nome,
        "descricao": fonte["descricao"],
        "url": fonte["url"],
        "sha256_origem": sha,
        "bytes_origem": len(conteudo),
        "linhas": n,
        "colunas": tabela.num_columns,
        "ingerido_em": ingerido_em,
        "arquivo_bronze": str(destino.relative_to(BRONZE.parent.parent)),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--atualizar", action="store_true")
    args = parser.parse_args()

    BRONZE.mkdir(parents=True, exist_ok=True)
    ingerido_em = datetime.now(timezone.utc).isoformat(timespec="seconds")
    manifesto = []
    for nome, fonte in FONTES.items():
        info = ingerir(nome, fonte, args.atualizar, ingerido_em)
        manifesto.append(info)
        print(f"bronze {nome:<20} {info['linhas']:>10,} linhas")
    (BRONZE / "_manifesto.json").write_text(json.dumps(manifesto, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
