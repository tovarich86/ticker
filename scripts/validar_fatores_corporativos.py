"""
Valida os fatores dos eventos corporativos retornados pela B3 contra os ajustes do Yahoo Finance.

Para cada ticker com evento, compara:
  - O multiplicador implícito do B3 (como o engine calcula hoje)
  - O split ratio reportado pelo YF (yf.Ticker.splits)
  - O ajuste de preço implícito no histórico do YF (ratio close_antes / close_depois ajustado)

Isso permite confirmar:
  1. Se GRUPAMENTO deve usar fac direto (não 1+fac/100)
  2. Se DESDOBRAMENTO usa a fórmula certa
  3. Se RESG TOTAL RV deve ser ignorado
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from datetime import date, timedelta
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Eventos da tabela retornada pela apuração 2023
# label, lastDatePrior (dd/mm/yyyy), factor
# ---------------------------------------------------------------------------
EVENTOS = [
    ("ITUB4",  "BONIFICACAO",     "23/12/2025", 3.0),
    ("AXIA3",  "BONIFICACAO",     "19/12/2025", 26.28378881074),
    ("AXIA3",  "RESG TOTAL RV",   "19/12/2025", 100.0),
    ("BBAS3",  "DESDOBRAMENTO",   "15/04/2024", 100.0),
    ("ITSA4",  "BONIFICACAO",     "18/12/2025", 2.0),
    ("RENT3",  "BONIFICACAO",     "29/12/2025", 3.84609533429),
    ("RADL3",  "BONIFICACAO",     "22/12/2025", 2.0),
    ("GGBR4",  "BONIFICACAO",     "17/04/2024", 20.0),
    ("SBSP3",  "BONIFICACAO",     "19/03/2026", 0.16098032200),
    ("SBSP3",  "BONIFICACAO",     "23/12/2025", 2.96469750000),
    ("CMIG4",  "BONIFICACAO",     "29/04/2024", 30.0),
    ("VIVT3",  "DESDOBRAMENTO",   "14/04/2025", 7.9),
    ("VIVT3",  "GRUPAMENTO",      "14/04/2025", 0.025),
    ("VBBR3",  "BONIFICACAO",     "25/11/2025", 7.11023515914),
    ("LREN3",  "BONIFICACAO",     "11/12/2024", 10.0),
    ("KLBN11", "BONIFICACAO",     "17/12/2025", 1.0),
    ("HAPV3",  "GRUPAMENTO",      "05/06/2025", 0.06666666667),
    ("CPLE3",  "RESG TOTAL RV",   "19/12/2025", 100.0),
    ("EGIE3",  "BONIFICACAO",     "26/11/2025", 40.0),
    ("TIMS3",  "DESDOBRAMENTO",   "02/07/2025", 9.9),
    ("TIMS3",  "GRUPAMENTO",      "02/07/2025", 0.01),
    ("AXIA6",  "BONIFICACAO",     "19/12/2025", 26.28378881074),
    ("AXIA6",  "RESG TOTAL RV",   "19/12/2025", 100.0),
    ("ENGI11", "BONIFICACAO",     "27/11/2025", 10.0),
    ("MGLU3",  "BONIFICACAO",     "29/12/2025", 5.0),
    ("MGLU3",  "GRUPAMENTO",      "24/05/2024", 0.10),
    ("GOAU4",  "BONIFICACAO",     "18/12/2025", 33.33),
    ("CYRE3",  "BONIFICACAO",     "30/12/2025", 18.95833333333),
]

# Mapa de tickers B3 → Yahoo (alguns trocaram de código)
YF_MAP = {
    "AXIA3": "AXIA3.SA",   # ex-ELET3
    "AXIA6": "AXIA6.SA",   # ex-ELET6
    "CPLE3": "CPLE3.SA",
}

def yf_ticker(t: str) -> str:
    return YF_MAP.get(t, f"{t}.SA")


def mult_b3_atual(label: str, factor: float) -> float:
    """Fórmula atual do engine (potencialmente errada para grupamento)."""
    return factor if label == "SPLIT_YF" else (1.0 + factor / 100.0)


def mult_b3_proposto(label: str, factor: float) -> float:
    """Fórmula proposta (grupamento = direto, RESG TOTAL RV = ignorado)."""
    label_up = label.upper()
    if label_up in ("RESG TOTAL RV", "RESGATE TOTAL RV"):
        return 1.0   # ignorado
    if label_up == "GRUPAMENTO":
        return factor
    return 1.0 + factor / 100.0  # BONIFICACAO, DESDOBRAMENTO, SPLIT_YF


def buscar_splits_yf(ticker_yf: str, dt_ini: date, dt_fim: date) -> pd.DataFrame:
    """Retorna splits do YF no período."""
    try:
        obj = yf.Ticker(ticker_yf)
        splits = obj.splits
        if splits.empty:
            return pd.DataFrame(columns=["Date", "yf_ratio"])
        df = splits.reset_index()
        df.columns = ["Date", "yf_ratio"]
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        mask = (df["Date"].dt.date >= dt_ini) & (df["Date"].dt.date <= dt_fim)
        return df[mask].reset_index(drop=True)
    except Exception as e:
        print(f"  ERRO YF splits {ticker_yf}: {e}")
        return pd.DataFrame(columns=["Date", "yf_ratio"])


def buscar_preco_ao_redor(ticker_yf: str, dt_evento: date, janela: int = 5) -> tuple[float | None, float | None]:
    """
    Retorna (close_antes, close_depois) em preços ajustados e não-ajustados
    para estimar o fator implícito no preço bruto.
    """
    try:
        obj = yf.Ticker(ticker_yf)
        ini = dt_evento - timedelta(days=janela + 5)
        fim = dt_evento + timedelta(days=janela + 5)
        hist = obj.history(start=ini, end=fim, auto_adjust=False)
        if hist.empty:
            return None, None
        hist.index = pd.to_datetime(hist.index).tz_localize(None)
        dt_ts = pd.Timestamp(dt_evento)
        antes = hist[hist.index < dt_ts]["Close"]
        depois = hist[hist.index >= dt_ts]["Close"]
        if antes.empty or depois.empty:
            return None, None
        return float(antes.iloc[-1]), float(depois.iloc[0])
    except Exception as e:
        return None, None


# ---------------------------------------------------------------------------
# Agrupa eventos por (ticker, data) para calcular mult combinado
# ---------------------------------------------------------------------------
from collections import defaultdict
grupos_evento: dict[tuple, list] = defaultdict(list)
for ticker, label, dt_str, factor in EVENTOS:
    dt = pd.to_datetime(dt_str, format="%d/%m/%Y").date()
    grupos_evento[(ticker, dt)].append((label, factor))

# ---------------------------------------------------------------------------
# Validação
# ---------------------------------------------------------------------------
DT_INI = date(2023, 3, 1)
DT_FIM = date(2026, 3, 31)

rows = []
print(f"\n{'='*100}")
print(f"{'Ticker':<8} {'Label':<18} {'Data':<12} {'Factor':>12} {'mult_atual':>10} {'mult_prop':>10} "
      f"{'yf_ratio':>10} {'yf_impl':>10} {'status'}")
print(f"{'='*100}")

tickers_vistos = set()
for (ticker, dt_evento), evs in sorted(grupos_evento.items()):
    ticker_yf = yf_ticker(ticker)

    # Splits YF na janela ±3 dias do evento
    janela_ini = dt_evento - timedelta(days=3)
    janela_fim = dt_evento + timedelta(days=3)
    df_splits = buscar_splits_yf(ticker_yf, janela_ini, janela_fim)
    yf_ratio_total = df_splits["yf_ratio"].prod() if not df_splits.empty else None

    # Preço bruto antes/depois
    p_antes, p_depois = buscar_preco_ao_redor(ticker_yf, dt_evento, janela=3)
    yf_impl = (p_depois / p_antes) if (p_antes and p_depois and p_antes > 0) else None

    for label, factor in evs:
        m_atual  = mult_b3_atual(label, factor)
        m_prop   = mult_b3_proposto(label, factor)

        # Status comparação com YF splits
        status = ""
        if yf_ratio_total is not None and label.upper() not in ("RESG TOTAL RV",):
            # Compara mult proposto com YF ratio (tolerância 5%)
            if abs(m_prop - yf_ratio_total) / max(abs(yf_ratio_total), 1e-9) < 0.05:
                status = "OK_prop"
            elif abs(m_atual - yf_ratio_total) / max(abs(yf_ratio_total), 1e-9) < 0.05:
                status = "OK_atual"
            else:
                status = "DIVERGE"
        elif label.upper() in ("RESG TOTAL RV",):
            status = "RESG→ignorar"

        yf_r_str  = f"{yf_ratio_total:.4f}" if yf_ratio_total is not None else "n/a"
        yf_i_str  = f"{yf_impl:.4f}"        if yf_impl is not None else "n/a"
        print(f"{ticker:<8} {label:<18} {str(dt_evento):<12} {factor:>12.5f} "
              f"{m_atual:>10.4f} {m_prop:>10.4f} {yf_r_str:>10} {yf_i_str:>10}  {status}")

        rows.append({
            "Ticker": ticker,
            "Label": label,
            "Data": dt_evento,
            "Factor B3": round(factor, 6),
            "mult_atual": round(m_atual, 6),
            "mult_proposto": round(m_prop, 6),
            "yf_ratio": round(yf_ratio_total, 6) if yf_ratio_total else None,
            "yf_impl_preco": round(yf_impl, 4) if yf_impl else None,
            "status": status,
        })

print(f"\n{'='*100}")

# Salva CSV para inspeção
import os
out_path = os.path.join(os.path.dirname(__file__), "validacao_fatores_corporativos.csv")
pd.DataFrame(rows).to_csv(out_path, index=False, sep=";", decimal=",")
print(f"\nResultado salvo em: {out_path}")
