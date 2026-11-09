import re
import subprocess

import markdown

from config import RAIZ

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
CSS = """
@page { size: A4; margin: 16mm 14mm; }
body { font-family: "Helvetica Neue", Arial, sans-serif; font-size: 10.5pt; line-height: 1.45; color: #0b0b0b; }
h1 { font-size: 20pt; margin: 0 0 6pt; } h2 { font-size: 14pt; margin: 18pt 0 6pt; border-bottom: 1px solid #e1e0d9; padding-bottom: 3pt; break-after: avoid; }
h3 { font-size: 12pt; margin: 12pt 0 4pt; break-after: avoid; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 10pt; font-size: 9pt; break-inside: avoid; }
th, td { border: 1px solid #e1e0d9; padding: 3pt 5pt; text-align: left; vertical-align: top; }
th { background: #f4f4f1; }
img { max-width: 100%; display: block; margin: 8pt auto; break-inside: avoid; }
code { font-family: Menlo, monospace; font-size: 8.5pt; background: #f4f4f1; padding: 0 2pt; border-radius: 3px; }
pre { background: #f4f4f1; padding: 8pt; border-radius: 6px; font-size: 8.5pt; white-space: pre-wrap; break-inside: avoid; }
pre code { background: none; padding: 0; }
a { color: #1c5cab; text-decoration: none; }
.capa { height: 250mm; display: flex; flex-direction: column; justify-content: center; break-after: page; }
.capa .tag { color: #eb6834; font-weight: 700; letter-spacing: 1px; font-size: 11pt; }
.capa h1 { font-size: 30pt; line-height: 1.15; margin: 8pt 0; }
.capa .sub { font-size: 14pt; color: #52514e; margin-bottom: 28pt; }
.capa .dados { font-size: 11pt; line-height: 1.8; }
.quebra { break-before: page; }
"""

CAPA = """<div class="capa">
<div class="tag">GLOBAL SOLUTION 2026.2 · BIG DATA &amp; ANALYTICS</div>
<h1>CitizenTech: onde o sol não vira painel</h1>
<div class="sub">Energia solar distribuída no Brasil: quem está ficando de fora da transição energética</div>
<img src="docs/img/02_mapas_sol_vs_paineis.png" style="width:85%;margin:0 0 24pt">
<div class="dados"><b>Aluno:</b> Anderson Huang<br><b>RM:</b> 565920<br><b>Tema:</b> Sustentabilidade<br>
<b>Repositório:</b> <a href="{repo}">{repo}</a></div>
</div>"""


def html_de(arquivo: str) -> str:
    texto = (RAIZ / arquivo).read_text()
    return markdown.markdown(texto, extensions=["tables", "fenced_code", "toc"])


def main(repo: str = "https://github.com/andhuangg/gs-energia-solar") -> None:
    readme = html_de("README.md")
    readme = re.sub(r"^<h1.*?</h1>\s*<p><strong>.*?</strong></p>\s*", "", readme, count=1, flags=re.S)
    corpo = CAPA.format(repo=repo) + readme + '<div class="quebra"></div>' + html_de("relatorio-executivo.md")
    pagina = RAIZ / "entrega.html"
    pagina.write_text(f'<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><base href="file://{RAIZ}/"><style>{CSS}</style></head><body>{corpo}</body></html>')
    saida = RAIZ / "entrega.pdf"
    subprocess.run([CHROME, "--headless", "--disable-gpu", "--no-pdf-header-footer", "--virtual-time-budget=10000",
                    f"--print-to-pdf={saida}", f"file://{pagina}"], check=True, capture_output=True)
    pagina.unlink()
    print(saida)


if __name__ == "__main__":
    main()
