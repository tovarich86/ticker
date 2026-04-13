import math
import pandas as pd
import pytest
from src.lti.engine import _parse_float, _calcular_vwap, calcular_tsr


def test_parse_float_basico():
    assert _parse_float("3.14") == pytest.approx(3.14)
    assert _parse_float("3,14") == pytest.approx(3.14)
    assert math.isnan(_parse_float("abc"))


def test_calcular_vwap_ponderado():
    df = pd.DataFrame({
        "Quantity": [100, 200, 300],
        "Average": [10.0, 11.0, 12.0],
    })
    # VWAP = (100*10 + 200*11 + 300*12) / 600 = 6800/600 ≈ 11.3333
    assert _calcular_vwap(df) == pytest.approx(11.3333, rel=1e-3)


def test_calcular_vwap_zero_qty_usa_media():
    df = pd.DataFrame({
        "Quantity": [0, 0],
        "Average": [10.0, 12.0],
    })
    assert _calcular_vwap(df) == pytest.approx(11.0)


def test_calcular_tsr_sem_eventos():
    df_divs = pd.DataFrame([{"lastDatePriorEx": "15/06/2024", "value": 2.0}])
    df_bonif = pd.DataFrame()
    t0 = pd.Timestamp("2023-03-01")
    t1 = pd.Timestamp("2026-03-31")
    result = calcular_tsr("TEST3", 10.0, 15.0, df_divs, df_bonif, t0, t1)
    # TSR = (15 - 10 + 2) / 10 = 0.70 = 70%
    assert result["TSR Total (%)"] == pytest.approx(70.0, rel=1e-4)
    assert result["Dividendos/JCP (R$)"] == pytest.approx(2.0)
    assert result["Mult. Corporativo"] == pytest.approx(1.0)


def test_calcular_tsr_com_desdobramento():
    # Desdobramento 2:1 (factor=100 no formato B3 → mult=2.0)
    df_divs = pd.DataFrame()
    df_bonif = pd.DataFrame([{
        "lastDatePrior": "01/06/2024",
        "label": "DESDOBRAMENTO",
        "factor": 100.0,  # 100 novas ações por 100 existentes → mult = 2.0
    }])
    t0 = pd.Timestamp("2023-03-01")
    t1 = pd.Timestamp("2026-03-31")
    result = calcular_tsr("TEST3", 20.0, 10.0, df_divs, df_bonif, t0, t1)
    # mult=2, p_final_adj = 10*2 = 20, ret_preco = (20-20)/20 = 0, TSR=0%
    assert result["Mult. Corporativo"] == pytest.approx(2.0)
    assert result["TSR Total (%)"] == pytest.approx(0.0, abs=0.01)


def test_calcular_tsr_com_bonificacao_tim():
    # Bonificação 10% (factor=10 no formato B3 → mult=1.10)
    df_divs = pd.DataFrame()
    df_bonif = pd.DataFrame([{
        "lastDatePrior": "01/06/2024",
        "label": "BONIFICACAO EM ACOES",
        "factor": 10.0,  # 10 novas ações por 100 → mult=1.10
    }])
    t0 = pd.Timestamp("2023-03-01")
    t1 = pd.Timestamp("2026-03-31")
    result = calcular_tsr("TEST3", 10.0, 10.0, df_divs, df_bonif, t0, t1)
    # mult=1.10, p_final_adj = 10*1.10 = 11.0, TSR = (11-10)/10 = 10%
    assert result["Mult. Corporativo"] == pytest.approx(1.10)
    assert result["TSR Total (%)"] == pytest.approx(10.0, rel=1e-4)


from unittest.mock import patch, MagicMock
from datetime import date
from src.lti.engine import buscar_vwap_mes


def _make_cotahist_df(ticker: str, rows: list[dict]) -> pd.DataFrame:
    """Helper: builds a fake COTAHIST DataFrame for one ticker."""
    import datetime as dt
    records = []
    for i, r in enumerate(rows):
        records.append({
            "Ticker": ticker,
            "Date": dt.date(2026, 3, i + 2),
            "Open": r["avg"], "High": r["avg"], "Low": r["avg"],
            "Close": r["avg"], "Average": r["avg"],
            "Volume": r["qty"] * r["avg"],
            "Quantity": r["qty"],
        })
    return pd.DataFrame(records)


def test_buscar_vwap_mes_calcula_corretamente():
    # Mock b3_engine to return two days of data for VALE3
    df_day1 = _make_cotahist_df("VALE3", [{"avg": 50.0, "qty": 1000}])
    df_day2 = _make_cotahist_df("VALE3", [{"avg": 60.0, "qty": 2000}])

    with patch("src.lti.engine.b3_engine.listar_dias_uteis") as mock_dias, \
         patch("src.lti.engine.b3_engine.baixar_e_parsear_dia") as mock_baixar, \
         patch("src.lti.engine.requests.Session"):
        mock_dias.return_value = [date(2026, 3, 2), date(2026, 3, 3)]
        mock_baixar.side_effect = [df_day1, df_day2]

        result = buscar_vwap_mes(
            ["VALE3"],
            date(2026, 3, 2),
            date(2026, 3, 31),
        )

    assert "VALE3" in result
    vwap, df_cot = result["VALE3"]
    # VWAP = (1000*50 + 2000*60) / 3000 = 170000/3000 ≈ 56.6667
    assert vwap == pytest.approx(56.6667, rel=1e-3)
    assert len(df_cot) == 2


def test_buscar_vwap_mes_ticker_sem_dados():
    with patch("src.lti.engine.b3_engine.listar_dias_uteis") as mock_dias, \
         patch("src.lti.engine.b3_engine.baixar_e_parsear_dia") as mock_baixar, \
         patch("src.lti.engine.requests.Session"):
        mock_dias.return_value = [date(2026, 3, 2)]
        mock_baixar.return_value = None  # sem dados neste dia

        result = buscar_vwap_mes(["ZZZT3"], date(2026, 3, 2), date(2026, 3, 31))

    vwap, df_cot = result["ZZZT3"]
    assert vwap is None
    assert df_cot.empty


from src.lti.engine import _fetch_dividendos_b3, _fetch_bonificacoes_b3


def _make_empresas_df(ticker: str = "TIMS3") -> pd.DataFrame:
    base = "".join(c for c in ticker if not c.isdigit())
    num = "".join(c for c in ticker if c.isdigit())
    tipo = {"3": "ON", "4": "PN", "11": "UNT"}.get(num, "ON")
    return pd.DataFrame([{
        "Nome do Pregão": f"{base}EMPREGAO",
        "CODE": base,
    }])


def test_fetch_dividendos_b3_retorna_dataframe():
    empresas = _make_empresas_df("TIMS3")
    fake_response = {
        "page": {"totalPages": 1},
        "results": [
            {
                "lastDatePriorEx": "15/03/2024",
                "paymentDate": "30/03/2024",
                "value": "1.50",
                "typeStock": "ON",
                "label": "DIVIDENDO",
            }
        ],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("src.lti.engine.curl_requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        from datetime import date
        df = _fetch_dividendos_b3(
            "TIMS3", empresas,
            date(2024, 1, 1), date(2024, 12, 31),
        )

    assert not df.empty
    assert "lastDatePriorEx" in df.columns
    assert float(df.iloc[0]["value"]) == pytest.approx(1.50)


def test_fetch_dividendos_b3_retorna_vazio_fora_do_periodo():
    empresas = _make_empresas_df("TIMS3")
    fake_response = {
        "page": {"totalPages": 1},
        "results": [
            {
                "lastDatePriorEx": "15/03/2020",  # fora do período pedido
                "value": "1.50",
                "typeStock": "ON",
                "label": "DIVIDENDO",
            }
        ],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("src.lti.engine.curl_requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        from datetime import date
        df = _fetch_dividendos_b3(
            "TIMS3", empresas,
            date(2024, 1, 1), date(2024, 12, 31),
        )

    assert df.empty


from src.lti.engine import _calcular_grupos, _detectar_divergencia_yf


def _make_ranking(n: int) -> list:
    """Creates a fake ranking of n tickers ordered by TSR desc."""
    from src.lti.engine import TickerResult
    results = []
    for i in range(n):
        t = TickerResult(
            ticker=f"T{i:03d}3",
            ticker_original=f"T{i:03d}3",
            vwap_p0=10.0,
            vwap_pf=12.0,
            dividendos_total=0.5,
            tsr=(n - i) / n,
            status="INCLUIDO",
        )
        results.append(t)
    return results


def test_calcular_grupos_42_tickers():
    ranking = _make_ranking(42)
    grupos = _calcular_grupos(ranking)
    assert len(grupos) == 6
    for g in range(1, 7):
        assert len(grupos[g]) == 7  # 42/6 = 7


def test_calcular_grupos_46_tickers():
    ranking = _make_ranking(46)
    grupos = _calcular_grupos(ranking)
    # 46 = 6*7 + 4 → primeiros 4 grupos têm 8, últimos 2 têm 7
    assert len(grupos[1]) == 8
    assert len(grupos[4]) == 8
    assert len(grupos[5]) == 7
    assert len(grupos[6]) == 7
    total = sum(len(v) for v in grupos.values())
    assert total == 46


def test_calcular_grupos_rank_e_grupo_atribuidos():
    ranking = _make_ranking(42)
    grupos = _calcular_grupos(ranking)
    # Primeiro da lista → rank 1, grupo 1
    assert grupos[1][0].rank == 1
    assert grupos[1][0].grupo == 1
    # Último → rank 42, grupo 6
    assert grupos[6][-1].rank == 42
    assert grupos[6][-1].grupo == 6


def test_detectar_divergencia_yf():
    # Sem divergência, sem eventos corporativos (mult=1.0)
    assert _detectar_divergencia_yf("VALE3", 10.0, 10.2, 1.0, 0.05) is None
    # Com divergência (>5%), sem eventos
    msg = _detectar_divergencia_yf("VALE3", 10.0, 16.0, 1.0, 0.05)
    assert msg is not None
    assert "VALE3" in msg
    # Bonificação 10% (mult=1.1): B3 escala dividendos, YF não — não deve ser divergência
    # B3=11.0 = YF=10.0 × 1.1 → dentro do threshold
    assert _detectar_divergencia_yf("GGBR4", 11.0, 10.0, 1.1, 0.05) is None
    # Split 2:1 (mult=2.0): B3=20 = YF=10 × 2.0 → sem divergência
    assert _detectar_divergencia_yf("BBAS3", 20.0, 10.0, 2.0, 0.05) is None
    # Grupamento 10:1 (mult=0.1): B3=1.0 = YF=10.0 × 0.1 → sem divergência
    assert _detectar_divergencia_yf("MGLU3", 1.0, 10.0, 0.1, 0.05) is None


# ---------------------------------------------------------------------------
# Additional coverage: bonificações, SPLIT_YF, grupos N<6
# ---------------------------------------------------------------------------

def test_fetch_bonificacoes_b3_retorna_dataframe():
    empresas = _make_empresas_df("VALE3")
    fake_data = [
        {
            "stockDividends": [
                {
                    "lastDatePrior": "15/06/2024",
                    "label": "DESDOBRAMENTO",
                    "factor": "100",
                    "approvedIn": "10/06/2024",
                    "isinCode": "BRVALE3TEST",
                }
            ]
        }
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_data
    mock_resp.content = b"[...]"
    mock_resp.text = "[...]"
    mock_resp.raise_for_status = MagicMock()

    with patch("src.lti.engine.curl_requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        from datetime import date
        df = _fetch_bonificacoes_b3(
            "VALE3", empresas,
            date(2024, 1, 1), date(2024, 12, 31),
        )

    assert not df.empty
    assert "lastDatePrior" in df.columns
    assert "label" in df.columns


def test_calcular_tsr_com_split_yf():
    """SPLIT_YF label: factor is used directly as multiplier (not 1 + factor/100)."""
    df_divs = pd.DataFrame()
    df_bonif = pd.DataFrame([{
        "lastDatePrior": "01/06/2024",
        "label": "SPLIT_YF",
        "factor": 3.0,  # 3:1 split → mult = 3.0 directly
    }])
    t0 = pd.Timestamp("2023-03-01")
    t1 = pd.Timestamp("2026-03-31")
    result = calcular_tsr("TEST3", 30.0, 10.0, df_divs, df_bonif, t0, t1)
    # mult=3.0, p_final_adj = 10*3 = 30, TSR = (30-30)/30 = 0%
    assert result["Mult. Corporativo"] == pytest.approx(3.0)
    assert result["TSR Total (%)"] == pytest.approx(0.0, abs=0.01)


def test_calcular_grupos_menos_de_6():
    """N=3: first 3 groups have 1 member each, remaining 3 are empty."""
    ranking = _make_ranking(3)
    grupos = _calcular_grupos(ranking)
    assert len(grupos) == 6
    assert len(grupos[1]) == 1
    assert len(grupos[2]) == 1
    assert len(grupos[3]) == 1
    assert len(grupos[4]) == 0
    assert len(grupos[5]) == 0
    assert len(grupos[6]) == 0
    total = sum(len(v) for v in grupos.values())
    assert total == 3


# ---------------------------------------------------------------------------
# ticker_service.get_ticker_info — parsing rstrip para tickers com digito no CODE
# ---------------------------------------------------------------------------

from src import ticker_service


def _make_empresas_b3sa():
    """DataFrame simulando B3SA no cadastro da B3 (CODE='B3SA')."""
    return pd.DataFrame([{"Nome do Pregão": "B3SAEMPREGAO", "CODE": "B3SA"}])


def test_get_ticker_info_b3sa3_base_correta():
    """B3SA3: rstrip deve extrair base='B3SA' e num='3', retornando typeStock='ON'."""
    df = _make_empresas_b3sa()
    info = ticker_service.get_ticker_info("B3SA3", df)
    assert info is not None, "B3SA3 nao encontrado — parsing de ticker com digito no CODE falhou"
    assert info["type_stock"] == "ON"
    assert info["code"] == "B3SA"


def test_get_ticker_info_tickers_normais_nao_afetados():
    """Tickers sem digito no CODE continuam funcionando corretamente."""
    from src import ticker_service as ts
    df = pd.DataFrame([
        {"Nome do Pregão": "VALEEMPREGAO", "CODE": "VALE"},
        {"Nome do Pregão": "USIMEMPREGAO", "CODE": "USIM"},
    ])
    vale = ts.get_ticker_info("VALE3", df)
    assert vale is not None and vale["type_stock"] == "ON"
    usim = ts.get_ticker_info("USIM5", df)
    assert usim is not None and usim["type_stock"] == "PN"


# ---------------------------------------------------------------------------
# _fetch_dividendos_b3 — filtro PN variante (PNB, PNC) para sufixos 4/5/6
# ---------------------------------------------------------------------------

def test_fetch_dividendos_b3_aceita_pnb():
    """USIM5 e AXIA6 podem retornar typeStock='PNB' ou 'PNC' — deve ser aceito."""
    empresas = _make_empresas_df("USIM5")
    # Sobrescreve o CODE para "USIM" (base correta após rstrip)
    empresas = pd.DataFrame([{"Nome do Pregão": "USIMEMPREGAO", "CODE": "USIM"}])

    fake_response = {
        "page": {"totalPages": 1},
        "results": [
            {
                "lastDatePriorEx": "15/06/2024",
                "paymentDate": "30/06/2024",
                "value": "0.50",
                "typeStock": "PNB",   # variante retornada pela API B3
                "label": "DIVIDENDO",
            }
        ],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("src.lti.engine.curl_requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        from datetime import date
        from src.lti.engine import _fetch_dividendos_b3
        df = _fetch_dividendos_b3(
            "USIM5", empresas,
            date(2024, 1, 1), date(2024, 12, 31),
        )

    assert not df.empty, "PNB deveria ser aceito pelo filtro PN variante"
    assert float(df.iloc[0]["value"]) == pytest.approx(0.50)
