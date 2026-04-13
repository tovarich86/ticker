import polars as pl
import pytest
from datetime import date

from src.b3_engine import detectar_substituicoes_cotahist


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal parsear_acoes_dia-style DataFrame."""
    return pl.DataFrame({
        'ticker': [r['ticker'] for r in rows],
        'isin':   [r['isin']   for r in rows],
        'nome':   [r['nome']   for r in rows],
    })


def _make_mock(df_origem: pl.DataFrame, df_alvo: pl.DataFrame):
    """Return a mock function that serves df_origem on first call, df_alvo on second."""
    calls = [df_origem, df_alvo]
    idx = [0]

    def _mock(data_pregao, session):
        result = calls[idx[0]]
        idx[0] += 1
        return result

    return _mock


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_detectar_substituicoes_por_isin(monkeypatch):
    """ISIN strategy: when both CPLE6 and CPLE3 share the same ISIN."""
    SHARED_ISIN = 'BRCPLEACNOR8'
    df_origem = _make_df([
        {'ticker': 'CPLE6', 'isin': SHARED_ISIN,    'nome': 'COPEL'},
        {'ticker': 'VALE3', 'isin': 'BRVALEACNOR0', 'nome': 'VALE'},
    ])
    df_alvo = _make_df([
        {'ticker': 'CPLE3', 'isin': SHARED_ISIN,    'nome': 'COPEL'},
        {'ticker': 'VALE3', 'isin': 'BRVALEACNOR0', 'nome': 'VALE'},
    ])

    monkeypatch.setattr('src.b3_engine.parsear_acoes_dia', _make_mock(df_origem, df_alvo))

    result = detectar_substituicoes_cotahist(
        tickers=['CPLE6', 'VALE3'],
        dt_origem=date(2023, 3, 2),
        dt_alvo=date(2026, 3, 3),
    )

    assert 'CPLE6' in result
    assert result['CPLE6']['substituto'] == 'CPLE3'
    assert result['CPLE6']['metodo'] == 'isin'
    assert 'VALE3' not in result  # present in alvo → not included


def test_detectar_substituicoes_por_nome_exato(monkeypatch):
    """Exact NOME strategy: CPLE6 NOME='COPEL' → CPLE3 NOME='COPEL' (ISINs differ)."""
    df_origem = _make_df([
        {'ticker': 'CPLE6', 'isin': 'BRCPLEACNPB9', 'nome': 'COPEL'},
        {'ticker': 'VALE3', 'isin': 'BRVALEACNOR0', 'nome': 'VALE'},
    ])
    df_alvo = _make_df([
        {'ticker': 'CPLE3', 'isin': 'BRCPLEACNOR8', 'nome': 'COPEL'},
        {'ticker': 'VALE3', 'isin': 'BRVALEACNOR0', 'nome': 'VALE'},
    ])

    monkeypatch.setattr('src.b3_engine.parsear_acoes_dia', _make_mock(df_origem, df_alvo))

    result = detectar_substituicoes_cotahist(
        tickers=['CPLE6', 'VALE3'],
        dt_origem=date(2023, 3, 2),
        dt_alvo=date(2026, 3, 3),
    )

    assert 'CPLE6' in result
    assert result['CPLE6']['substituto'] == 'CPLE3'
    assert result['CPLE6']['metodo'] == 'nome_exato'
    assert result['CPLE6']['nome_orig'] == 'COPEL'
    assert result['CPLE6']['nome_subst'] == 'COPEL'
    assert 'VALE3' not in result  # present in alvo → not included


def test_detectar_substituicoes_por_nome_prefixo(monkeypatch):
    """
    Prefix NOME strategy: EMBR3 NOME='EMBRAER SA' in origem has no exact match in alvo,
    but the prefix 'EMBRAER' (>= 4 chars) matches exactly one ticker 'EMBJ3' with
    nome 'EMBRAER MIL'.
    """
    df_origem = _make_df([
        {'ticker': 'EMBR3', 'isin': 'BREMBRACNOR9', 'nome': 'EMBRAER SA'},
        {'ticker': 'VALE3', 'isin': 'BRVALEACNOR0', 'nome': 'VALE'},
    ])
    df_alvo = _make_df([
        {'ticker': 'EMBJ3', 'isin': 'BREMBJACNOR1', 'nome': 'EMBRAER MIL'},
        {'ticker': 'VALE3', 'isin': 'BRVALEACNOR0', 'nome': 'VALE'},
    ])

    monkeypatch.setattr('src.b3_engine.parsear_acoes_dia', _make_mock(df_origem, df_alvo))

    result = detectar_substituicoes_cotahist(
        tickers=['EMBR3', 'VALE3'],
        dt_origem=date(2023, 3, 2),
        dt_alvo=date(2026, 3, 3),
    )

    assert 'EMBR3' in result
    assert result['EMBR3']['substituto'] == 'EMBJ3'
    assert result['EMBR3']['metodo'] == 'nome_prefixo'
    assert 'VALE3' not in result


def test_detectar_substituicoes_ticker_presente_nao_incluido(monkeypatch):
    """Tickers already present in alvo must NOT appear in the result dict."""
    df_origem = _make_df([
        {'ticker': 'PETR4', 'isin': 'BRPETRACNPR6', 'nome': 'PETROBRAS'},
        {'ticker': 'ITUB4', 'isin': 'BRITUBACNPR8', 'nome': 'ITAUUNIBANCO'},
    ])
    df_alvo = _make_df([
        {'ticker': 'PETR4', 'isin': 'BRPETRACNPR6', 'nome': 'PETROBRAS'},
        {'ticker': 'ITUB4', 'isin': 'BRITUBACNPR8', 'nome': 'ITAUUNIBANCO'},
    ])

    monkeypatch.setattr('src.b3_engine.parsear_acoes_dia', _make_mock(df_origem, df_alvo))

    result = detectar_substituicoes_cotahist(
        tickers=['PETR4', 'ITUB4'],
        dt_origem=date(2023, 3, 2),
        dt_alvo=date(2026, 3, 3),
    )

    assert result == {}


def test_detectar_substituicoes_sem_match_retorna_none(monkeypatch):
    """When no strategy matches, substituto must be None and metodo='sem_match'."""
    df_origem = _make_df([
        {'ticker': 'ELET3', 'isin': 'BRELETACNOR6', 'nome': 'ELETROBRAS'},
    ])
    # Alvo has completely unrelated tickers/ISINs/names
    df_alvo = _make_df([
        {'ticker': 'VALE3', 'isin': 'BRVALEACNOR0', 'nome': 'VALE'},
    ])

    monkeypatch.setattr('src.b3_engine.parsear_acoes_dia', _make_mock(df_origem, df_alvo))

    result = detectar_substituicoes_cotahist(
        tickers=['ELET3'],
        dt_origem=date(2023, 3, 2),
        dt_alvo=date(2026, 3, 3),
    )

    assert 'ELET3' in result
    assert result['ELET3']['substituto'] is None
    assert result['ELET3']['metodo'] == 'sem_match'
    assert result['ELET3']['nome_orig'] == 'ELETROBRAS'
    assert result['ELET3']['nome_subst'] is None
