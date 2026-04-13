from datetime import date
import pytest
from src.lti.config import OUTORGAS, OutorgaConfig


def test_all_outorgas_defined():
    assert set(OUTORGAS.keys()) == {2023, 2024, 2025}


def test_tims3_in_all_outorgas():
    for ano, cfg in OUTORGAS.items():
        assert "TIMS3" in cfg.tickers, f"TIMS3 ausente na outorga {ano}"


def test_outorga_2023_periodos():
    cfg = OUTORGAS[2023]
    assert cfg.dt_p0_ini == date(2023, 3, 1)
    assert cfg.dt_p0_fim == date(2023, 3, 29)
    assert cfg.dt_pf_ini == date(2026, 3, 2)
    assert cfg.dt_pf_fim == date(2026, 3, 31)
    assert cfg.dt_divs_ini == date(2023, 3, 1)
    assert cfg.dt_divs_fim == date(2026, 3, 31)


def test_outorga_2024_periodos():
    cfg = OUTORGAS[2024]
    assert cfg.dt_p0_ini == date(2024, 3, 1)
    assert cfg.dt_p0_fim == date(2024, 3, 28)
    assert cfg.dt_pf_ini == date(2026, 3, 2)
    assert cfg.dt_pf_fim == date(2026, 3, 31)


def test_outorga_2025_periodos():
    cfg = OUTORGAS[2025]
    assert cfg.dt_p0_ini == date(2025, 3, 5)
    assert cfg.dt_p0_fim == date(2025, 3, 31)


def test_embr3_substituido_em_todas():
    for ano, cfg in OUTORGAS.items():
        if "EMBR3" in cfg.tickers:
            assert "EMBR3" in cfg.substituicoes, f"EMBR3 sem substituição na outorga {ano}"
            assert cfg.substituicoes["EMBR3"] == "EMBJ3"


def test_azul4_excluido_em_2023_2024():
    assert "AZUL4" in OUTORGAS[2023].exclusoes_forcadas
    assert "AZUL4" in OUTORGAS[2024].exclusoes_forcadas


def test_divergencia_threshold_padrao():
    cfg = OUTORGAS[2023]
    assert cfg.divergencia_threshold == pytest.approx(0.05)
