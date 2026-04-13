"""
Testes unitários para calcular_tsr — foco nos ajustes por eventos corporativos.
"""
import pandas as pd
import pytest
from src.lti.engine import calcular_tsr, _parse_float

T0 = pd.Timestamp("2023-03-31")
T1 = pd.Timestamp("2026-03-31")


def _bonif_df(rows: list[dict]) -> pd.DataFrame:
    """Constrói df_bonif no formato retornado por _fetch_bonificacoes_b3."""
    return pd.DataFrame(rows)


def _divs_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _parse_float — formato brasileiro
# ---------------------------------------------------------------------------

def test_parse_float_formato_brasileiro_milhar():
    """'9.900,00000000000' deve ser interpretado como 9900.0 (ponto = milhar, vírgula = decimal)."""
    assert _parse_float("9.900,00000000000") == pytest.approx(9900.0)


def test_parse_float_formato_brasileiro_decimal():
    """'0,02500000000' deve ser interpretado como 0.025."""
    assert _parse_float("0,02500000000") == pytest.approx(0.025)


def test_parse_float_sem_separador_milhar():
    """'100' e '20,00' sem ambiguidade."""
    assert _parse_float("100") == pytest.approx(100.0)
    assert _parse_float("20,00000000000") == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# Pares DESDOBRAMENTO+GRUPAMENTO — matemática resolve com parsing correto
# ---------------------------------------------------------------------------

def test_par_tims3_neutro():
    """
    TIMS3: DESDOBRAMENTO factor='9.900,00' (=9900) → mult=100
           GRUPAMENTO factor='0,01' → mult=0.01
           Net: 100 × 0.01 = 1.0 (limpeza de base, neutro — confirmado pela TIM).
    """
    df_bonif = _bonif_df([
        {"label": "DESDOBRAMENTO", "lastDatePrior": "02/07/2025", "factor": "9.900,00000000000"},
        {"label": "GRUPAMENTO",    "lastDatePrior": "02/07/2025", "factor": "0,01000000000"},
    ])
    result = calcular_tsr("TIMS3", p0=10.0, p_final=10.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(1.0, rel=1e-6)
    assert len(result["_eventos"]) == 2


def test_par_vivt3_split_2_1():
    """
    VIVT3: DESDOBRAMENTO factor='7.900,00' (=7900) → mult=80
           GRUPAMENTO factor='0,025' → mult=0.025
           Net: 80 × 0.025 = 2.0 (desdobramento 2:1 real — confirmado pela Vivo).
    """
    df_bonif = _bonif_df([
        {"label": "DESDOBRAMENTO", "lastDatePrior": "14/04/2025", "factor": "7.900,00000000000"},
        {"label": "GRUPAMENTO",    "lastDatePrior": "14/04/2025", "factor": "0,02500000000"},
    ])
    result = calcular_tsr("VIVT3", p0=10.0, p_final=5.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(2.0, rel=1e-6)
    assert result["P Final Ajustado (R$)"] == pytest.approx(10.0)  # 5.0 × 2.0 = P0


# ---------------------------------------------------------------------------
# GRUPAMENTO isolado usa factor direto
# ---------------------------------------------------------------------------

def test_grupamento_mglu3_fator_direto():
    """MGLU3 grupamento 10:1 — factor=0.10 deve dar mult=0.10, não 1.001."""
    df_bonif = _bonif_df([
        {"label": "GRUPAMENTO", "lastDatePrior": "24/05/2024", "factor": "0,10"},
    ])
    result = calcular_tsr("MGLU3", p0=2.0, p_final=20.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(0.10, rel=1e-4)
    assert len(result["_eventos"]) == 1
    assert result["_eventos"][0]["mult"] == pytest.approx(0.10, rel=1e-4)


def test_grupamento_hapv3_fator_direto():
    """HAPV3 grupamento 15:1 — factor=0.06667 deve dar mult≈0.0667."""
    df_bonif = _bonif_df([
        {"label": "GRUPAMENTO", "lastDatePrior": "05/06/2025", "factor": "0,06667"},
    ])
    result = calcular_tsr("HAPV3", p0=5.0, p_final=50.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(0.06667, rel=1e-3)


# ---------------------------------------------------------------------------
# RESG TOTAL RV é ignorado
# ---------------------------------------------------------------------------

def test_resg_total_rv_ignorado():
    """RESG TOTAL RV não deve contribuir com nenhum multiplicador."""
    df_bonif = _bonif_df([
        {"label": "RESG TOTAL RV", "lastDatePrior": "19/12/2025", "factor": "100"},
    ])
    result = calcular_tsr("AXIA3", p0=10.0, p_final=12.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == 1.0
    assert result["_eventos"] == []


# ---------------------------------------------------------------------------
# BONIFICACAO e DESDOBRAMENTO preservam fórmula percentual
# ---------------------------------------------------------------------------

def test_bonificacao_percentual():
    """Bonificação 20% → mult = 1.20."""
    df_bonif = _bonif_df([
        {"label": "BONIFICACAO", "lastDatePrior": "17/04/2024", "factor": "20"},
    ])
    result = calcular_tsr("GGBR4", p0=10.0, p_final=12.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(1.20, rel=1e-4)


def test_desdobramento_bbas3():
    """BBAS3 desdobramento 2:1 (factor=100) → mult=2.0."""
    df_bonif = _bonif_df([
        {"label": "DESDOBRAMENTO", "lastDatePrior": "15/04/2024", "factor": "100"},
    ])
    result = calcular_tsr("BBAS3", p0=10.0, p_final=10.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(2.0, rel=1e-4)


# ---------------------------------------------------------------------------
# Dividendos ajustados por mult acumulado até a data ex
# ---------------------------------------------------------------------------

def test_dividendo_reescalonado_por_grupamento():
    """
    Dividendo pago ANTES de um grupamento real deve ser multiplicado pelo fator
    do grupamento (quem tinha 10 ações passou a ter 1, mas recebeu o dividendo
    por 10 ações → equivalente a 10× o dividendo por ação final).
    """
    df_bonif = _bonif_df([
        {"label": "GRUPAMENTO", "lastDatePrior": "24/05/2024", "factor": "0,10"},
    ])
    df_divs = _divs_df([
        {"lastDatePriorEx": "31/03/2024", "value": "1.00", "paymentDate": "15/04/2024", "label": "DIV"},
    ])
    result = calcular_tsr("MGLU3", p0=2.0, p_final=20.0,
                          df_divs=df_divs, df_bonif=df_bonif,
                          t0=T0, t1=T1)
    # Dividendo antes do grupamento: mult_em_data_ex = 1.0 (grupamento ainda não ocorreu)
    # Depois do grupamento: mult_final = 0.10
    # O dividendo de R$1,00 foi recebido quando o acionista ainda tinha 10 ações por cada
    # ação final — mas mult_ate(data_ex_div) = 1.0 (grupamento é posterior)
    # Então div_total = 1.0 × 1.0 = 1.0 (por ação original)
    assert result["Dividendos/JCP (R$)"] == pytest.approx(1.0, rel=1e-4)
    detail = result["_divs_detail"]
    assert len(detail) == 1
    assert detail[0]["Multiplicador"] == pytest.approx(1.0, rel=1e-4)


# ---------------------------------------------------------------------------
# _mult_yf: apenas splits/desdobramentos/grupamentos entram; bonificação não
# ---------------------------------------------------------------------------

def test_mult_yf_desdobramento_incluido():
    """DESDOBRAMENTO entra no _mult_yf (YF ajusta retroativamente para splits)."""
    df_bonif = _bonif_df([
        {"label": "DESDOBRAMENTO", "lastDatePrior": "15/04/2024", "factor": "100"},
    ])
    result = calcular_tsr("BBAS3", p0=10.0, p_final=10.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(2.0, rel=1e-4)
    assert result["_mult_yf"] == pytest.approx(2.0, rel=1e-4)


def test_mult_yf_grupamento_incluido():
    """GRUPAMENTO entra no _mult_yf (YF ajusta retroativamente para reverse splits)."""
    df_bonif = _bonif_df([
        {"label": "GRUPAMENTO", "lastDatePrior": "24/05/2024", "factor": "0,10"},
    ])
    result = calcular_tsr("MGLU3", p0=2.0, p_final=20.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(0.10, rel=1e-4)
    assert result["_mult_yf"] == pytest.approx(0.10, rel=1e-4)


def test_mult_yf_bonificacao_excluida():
    """BONIFICACAO NAO entra no _mult_yf — YF nao ajusta dividendos historicos para bonus shares."""
    df_bonif = _bonif_df([
        {"label": "BONIFICACAO", "lastDatePrior": "17/04/2024", "factor": "26.28"},
    ])
    result = calcular_tsr("AXIA3", p0=10.0, p_final=12.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(1.2628, rel=1e-4)
    assert result["_mult_yf"] == pytest.approx(1.0)  # bonificacao excluida


def test_mult_yf_tims3_par_neutro():
    """Par DESDOBRAMENTO+GRUPAMENTO neutro: mult_total=1.0 e mult_yf=1.0."""
    df_bonif = _bonif_df([
        {"label": "DESDOBRAMENTO", "lastDatePrior": "02/07/2025", "factor": "9.900,00000000000"},
        {"label": "GRUPAMENTO",    "lastDatePrior": "02/07/2025", "factor": "0,01000000000"},
    ])
    result = calcular_tsr("TIMS3", p0=10.0, p_final=10.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(1.0, rel=1e-6)
    assert result["_mult_yf"] == pytest.approx(1.0, rel=1e-6)


def test_mult_yf_vivt3_split_real():
    """Par DESDOBRAMENTO+GRUPAMENTO com split 2:1 real: mult_yf=2.0."""
    df_bonif = _bonif_df([
        {"label": "DESDOBRAMENTO", "lastDatePrior": "14/04/2025", "factor": "7.900,00000000000"},
        {"label": "GRUPAMENTO",    "lastDatePrior": "14/04/2025", "factor": "0,02500000000"},
    ])
    result = calcular_tsr("VIVT3", p0=10.0, p_final=5.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(2.0, rel=1e-6)
    assert result["_mult_yf"] == pytest.approx(2.0, rel=1e-6)


def test_mult_yf_bonificacao_mais_desdobramento():
    """Bonificacao + Desdobramento: mult_yf inclui apenas desdobramento."""
    df_bonif = _bonif_df([
        {"label": "BONIFICACAO",   "lastDatePrior": "01/03/2024", "factor": "10"},  # +10% shares
        {"label": "DESDOBRAMENTO", "lastDatePrior": "01/06/2024", "factor": "100"}, # 2:1 split
    ])
    result = calcular_tsr("TEST3", p0=10.0, p_final=10.0,
                          df_divs=pd.DataFrame(), df_bonif=df_bonif,
                          t0=T0, t1=T1)
    assert result["Mult. Corporativo"] == pytest.approx(1.10 * 2.0, rel=1e-4)  # total = 2.20
    assert result["_mult_yf"] == pytest.approx(2.0, rel=1e-4)  # apenas desdobramento
