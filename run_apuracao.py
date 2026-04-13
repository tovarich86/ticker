#!/usr/bin/env python3
# run_apuracao.py
"""
CLI para apuração LTI — TSR IBrX-50 TIM.

Uso:
  python run_apuracao.py                          # todas as outorgas
  python run_apuracao.py --outorga 2024           # apenas outorga 2024
  python run_apuracao.py --outorga 2023 2025      # outorgas 2023 e 2025
  python run_apuracao.py --output ./resultados/   # pasta de output customizada
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.lti.config import OUTORGAS
from src.lti.engine import calcular_todas_outorgas
from src.lti.excel_builder import salvar_excel, nome_arquivo
from src import ticker_service


def main() -> None:
    parser = argparse.ArgumentParser(description="Apuração LTI — TSR IBrX-50 TIM")
    parser.add_argument(
        "--outorga",
        nargs="+",
        type=int,
        default=list(OUTORGAS.keys()),
        help="Anos das outorgas a calcular (ex: --outorga 2023 2024). Default: todas.",
    )
    parser.add_argument(
        "--output",
        default="./output",
        help="Pasta de destino para os arquivos Excel. Default: ./output",
    )
    args = parser.parse_args()

    anos = [a for a in args.outorga if a in OUTORGAS]
    if not anos:
        print(f"Erro: outorgas válidas são {list(OUTORGAS.keys())}. Recebido: {args.outorga}")
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)

    print("Carregando base de empresas B3...")
    df_empresas = ticker_service.carregar_empresas()
    if df_empresas.empty:
        print("Erro: não foi possível carregar a base de empresas B3.")
        sys.exit(1)
    print(f"  {len(df_empresas)} empresas carregadas.\n")

    resultados = calcular_todas_outorgas(anos, df_empresas, logger=print)

    for ano, resultado in resultados.items():
        fname = nome_arquivo(resultado)
        fpath = os.path.join(args.output, fname)
        salvar_excel(resultado, fpath)
        print(f"\nOutorga {ano}: {resultado.n_incluidos} incluídos | {resultado.n_excluidos} excluídos")
        print(f"  Arquivo: {fpath}")
        print(f"\n  Ranking (top 10):")
        for t in resultado.ranking[:10]:
            tim_mark = " ◄ TIM" if t.ticker == "TIMS3" else ""
            print(f"    {t.rank:2d}. {t.ticker:<8s} TSR={t.tsr*100:+.2f}%  Grupo {t.grupo}{tim_mark}")

    print("\nApuração concluída.")


if __name__ == "__main__":
    main()
