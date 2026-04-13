"""
Investiga divergências entre dividendos B3 e Yahoo Finance por ticker.

Para cada ticker divergente:
  1. Busca dividendos da B3 com eventos corporativos aplicados (engine)
  2. Busca dividendos do YF (ajustados e brutos)
  3. Mostra linha a linha onde as diferenças ocorrem
  4. Classifica a causa: missing_b3 | split_scaling | data_mismatch | outro
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
import yfinance as yf
from datetime import date

from src.lti.config import OUTORGAS
from src.lti.engine import (
    _fetch_dividendos_b3,
    _fetch_bonificacoes_b3,
    calcular_tsr,
    _parse_float,
)
from src import ticker_service

# ---------------------------------------------------------------------------
# Tickers a investigar (da tabela de divergências outorga 2023)
# Formato: (ticker_efetivo, ticker_original)
# ---------------------------------------------------------------------------
DIVERGENTES = [
    ("ITUB4",  "ITUB4"),
    ("ABEV3",  "ABEV3"),
    ("B3SA3",  "B3SA3"),
    ("BBAS3",  "BBAS3"),
    ("GGBR4",  "GGBR4"),
    ("CMIG4",  "CMIG4"),
    ("VIVT3",  "VIVT3"),
    ("VBBR3",  "VBBR3"),
    ("LREN3",  "LREN3"),
    ("EMBJ3",  "EMBR3"),
    ("HYPE3",  "HYPE3"),
    ("MOTV3",  "CCRO3"),
    ("CPLE3",  "CPLE6"),
    ("EGIE3",  "EGIE3"),
    ("TIMS3",  "TIMS3"),
    ("AXIA6",  "ELET6"),
    ("ENGI11", "ENGI11"),
    ("MGLU3",  "MGLU3"),
    ("GOAU4",  "GOAU4"),
    ("USIM5",  "USIM5"),
]

CFG = OUTORGAS[2023]
DT_INI = CFG.dt_divs_ini   # 2023-03-01
DT_FIM = CFG.dt_divs_fim   # 2026-03-31
T0 = pd.Timestamp(DT_INI)
T1 = pd.Timestamp(DT_FIM)

YF_MAP = {"AXIA3": "ELET3.SA", "AXIA6": "ELET6.SA", "MOTV3": "CCRO3.SA",
          "EMBJ3": "EMBR3.SA", "CPLE3": "CPLE6.SA"}

def yf_ticker(t: str) -> str:
    return YF_MAP.get(t, f"{t}.SA")


def buscar_divs_yf(ticker_yf: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retorna (divs_ajustados, divs_brutos) do YF no período."""
    try:
        obj = yf.Ticker(ticker_yf)
        def _filtrar(divs):
            if divs.empty:
                return pd.DataFrame(columns=["Date", "value"])
            df = divs.reset_index()
            df.columns = ["Date", "value"]
            df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
            return df[(df["Date"] >= T0) & (df["Date"] <= T1)].reset_index(drop=True)
        adj  = _filtrar(obj.dividends)
        # Bruto: history com auto_adjust=False
        hist = obj.history(start=DT_INI, end=DT_FIM, auto_adjust=False)
        if not hist.empty and "Dividends" in hist.columns:
            raw = hist[["Dividends"]].reset_index()
            raw.columns = ["Date", "value"]
            raw["Date"] = pd.to_datetime(raw["Date"]).dt.tz_localize(None)
            raw = raw[raw["value"] > 0].reset_index(drop=True)
        else:
            raw = pd.DataFrame(columns=["Date", "value"])
        return adj, raw
    except Exception as e:
        print(f"  ERRO YF {ticker_yf}: {e}")
        return pd.DataFrame(columns=["Date","value"]), pd.DataFrame(columns=["Date","value"])


# ---------------------------------------------------------------------------
def main():
    print("Carregando base de empresas B3...")
    df_empresas = ticker_service.carregar_empresas()
    if df_empresas.empty:
        print("ERRO: não foi possível carregar empresas.")
        return

    resultados = []

    for ticker_ef, ticker_orig in DIVERGENTES:
        print(f"\n{'='*70}")
        print(f"  {ticker_ef} (orig={ticker_orig})")
        print(f"{'='*70}")

        # ── B3 dividendos + eventos ─────────────────────────────────────────
        df_divs_b3   = _fetch_dividendos_b3(ticker_ef, df_empresas, DT_INI, DT_FIM)
        df_bonif     = _fetch_bonificacoes_b3(ticker_ef, df_empresas, DT_INI, DT_FIM)

        if df_divs_b3.empty:
            print("  B3 dividendos: VAZIO")
        else:
            print(f"  B3 dividendos: {len(df_divs_b3)} linhas")

        # Eventos corporativos com parsing correto
        eventos_b3 = []
        if not df_bonif.empty and "lastDatePrior" in df_bonif.columns:
            for _, row in df_bonif.iterrows():
                dt  = pd.to_datetime(row.get("lastDatePrior", ""), format="%d/%m/%Y", errors="coerce")
                fac = _parse_float(row.get("factor", 0))
                lbl = str(row.get("label","")).upper()
                if pd.notna(dt) and pd.notna(fac) and fac != 0 and T0 < dt <= T1:
                    if lbl in ("RESG TOTAL RV", "RESGATE TOTAL RV"):
                        mult = None  # ignorado
                    elif lbl in ("GRUPAMENTO", "SPLIT_YF"):
                        mult = fac
                    else:
                        mult = 1.0 + fac / 100.0
                    eventos_b3.append({"dt": dt, "label": row.get("label",""), "factor": fac, "mult": mult})
        eventos_b3.sort(key=lambda x: x["dt"])

        if eventos_b3:
            print("  Eventos corporativos B3:")
            for ev in eventos_b3:
                mult_str = f"{ev['mult']:.6f}" if ev["mult"] is not None else "ignorado"
                print(f"    {ev['dt'].date()}  {ev['label']:<18}  fac={ev['factor']:>12.4f}  mult={mult_str}")

        def mult_ate(data: pd.Timestamp) -> float:
            m = 1.0
            for ev in eventos_b3:
                if ev["mult"] is not None and ev["dt"] <= data:
                    m *= ev["mult"]
            return m

        # Total B3 com multiplicador
        total_b3 = 0.0
        if not df_divs_b3.empty and "value" in df_divs_b3.columns:
            print("  Dividendos B3 detalhado:")
            for _, row in df_divs_b3.iterrows():
                dt_ex = pd.to_datetime(row.get("lastDatePriorEx",""), format="%d/%m/%Y", errors="coerce")
                val   = _parse_float(row.get("value", 0))
                if pd.isna(dt_ex) or pd.isna(val):
                    continue
                m   = mult_ate(dt_ex)
                adj = m * val
                total_b3 += adj
                print(f"    {dt_ex.date()}  val={val:>8.4f}  mult={m:.6f}  adj={adj:.6f}  [{row.get('label','')}]")

        print(f"  TOTAL B3 (ajustado por eventos):  {total_b3:.4f}")

        # ── YF dividendos ────────────────────────────────────────────────────
        yf_tk = yf_ticker(ticker_ef)
        df_adj, df_raw = buscar_divs_yf(yf_tk)

        total_yf_adj = float(df_adj["value"].sum()) if not df_adj.empty else 0.0
        total_yf_raw = float(df_raw["value"].sum()) if not df_raw.empty else 0.0

        if not df_adj.empty:
            print(f"  Dividendos YF ajustados: {len(df_adj)} linhas")
            for _, r in df_adj.iterrows():
                print(f"    {r['Date'].date()}  {r['value']:.6f}")

        if not df_raw.empty:
            print(f"  Dividendos YF brutos (auto_adjust=False): {len(df_raw)} linhas")
            for _, r in df_raw.iterrows():
                print(f"    {r['Date'].date()}  {r['value']:.6f}")

        print(f"  TOTAL YF ajustado:  {total_yf_adj:.4f}")
        print(f"  TOTAL YF bruto:     {total_yf_raw:.4f}")

        # ── Análise da divergência ────────────────────────────────────────────
        mult_liquido = mult_ate(T1)  # produto de todos os eventos no período

        ratio_adj = total_b3 / total_yf_adj if total_yf_adj else None
        ratio_raw = total_b3 / total_yf_raw if total_yf_raw else None

        print(f"  Mult. corporativo líquido: {mult_liquido:.6f}")
        if ratio_adj:
            print(f"  B3 / YF_adj = {ratio_adj:.4f}  (esperado ≈ {mult_liquido:.4f} se YF não ajusta)")
        if ratio_raw:
            print(f"  B3 / YF_raw = {ratio_raw:.4f}")

        # Classificação automática
        if total_b3 == 0 and total_yf_adj > 0:
            causa = "MISSING_B3 (lookup falhou)"
        elif total_b3 == 0 and total_yf_adj == 0:
            causa = "AMBOS_ZERO"
        elif ratio_adj and abs(ratio_adj - mult_liquido) / max(abs(mult_liquido), 1e-9) < 0.15:
            causa = f"SPLIT_SCALING (ratio≈mult_liq={mult_liquido:.4f})"
        elif ratio_raw and abs(ratio_raw - 1.0) < 0.12:
            causa = "OK_BRUTO (YF ajustado diverge, bruto bate)"
        else:
            causa = "INVESTIGAR"

        print(f"  >>> CAUSA PROVÁVEL: {causa}")

        resultados.append({
            "ticker": ticker_ef,
            "ticker_orig": ticker_orig,
            "total_b3": round(total_b3, 4),
            "total_yf_adj": round(total_yf_adj, 4),
            "total_yf_raw": round(total_yf_raw, 4),
            "mult_liq": round(mult_liquido, 6),
            "ratio_b3_yf_adj": round(ratio_adj, 4) if ratio_adj else None,
            "ratio_b3_yf_raw": round(ratio_raw, 4) if ratio_raw else None,
            "causa": causa,
        })

    # Tabela resumo
    print(f"\n\n{'='*90}")
    print("RESUMO")
    print(f"{'='*90}")
    df_res = pd.DataFrame(resultados)
    print(df_res.to_string(index=False))

    out = os.path.join(os.path.dirname(__file__), "investigacao_divergencias.csv")
    df_res.to_csv(out, index=False, sep=";", decimal=",")
    print(f"\nSalvo em: {out}")


if __name__ == "__main__":
    main()
