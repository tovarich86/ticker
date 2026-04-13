from dataclasses import dataclass, field
from datetime import date


@dataclass
class OutorgaConfig:
    ano: int
    tickers: list[str]
    dt_p0_ini: date
    dt_p0_fim: date
    dt_pf_ini: date
    dt_pf_fim: date
    dt_divs_ini: date
    dt_divs_fim: date
    exclusoes_forcadas: list[str] = field(default_factory=list)
    substituicoes: dict[str, str] = field(default_factory=dict)
    divergencia_threshold: float = 0.05


# IBrX-50 congelado em março/2023 (idêntico ao de março/2024)
_IBRX50_2023_2024: list[str] = [
    "VALE3", "ITUB4", "PETR4", "PETR3", "BBDC4", "ELET3", "ABEV3", "B3SA3",
    "WEGE3", "BBAS3", "ITSA4", "RENT3", "SUZB3", "EQTL3", "RADL3", "GGBR4",
    "PRIO3", "RDOR3", "BPAC11", "RAIL3", "BBSE3", "JBSS3", "ASAI3", "BBDC3",
    "CSAN3", "SBSP3", "ENEV3", "CMIG4", "VIVT3", "VBBR3", "LREN3", "EMBR3",
    "UGPA3", "HYPE3", "TOTS3", "CCRO3", "KLBN11", "HAPV3", "NTCO3", "CPLE6",
    "EGIE3", "TIMS3", "ELET6", "ENGI11", "SANB11", "MGLU3", "CSNA3", "ISAE4",
    "GOAU4", "TAEE11", "AZUL4", "BRFS3", "CYRE3", "MRVE3", "MULT3", "USIM5",
    "BRAV3",
]

# IBrX-50 congelado em março/2025
_IBRX50_2025: list[str] = [
    "ABEV3", "ASAI3", "AZUL4", "AZZA3", "B3SA3", "BBAS3", "BBDC4", "BBSE3",
    "BPAC11", "BRAV3", "BRFS3", "CMIG4", "CPLE6", "CSAN3", "CSNA3", "CYRE3",
    "ELET3", "EMBR3", "ENEV3", "ENGI11", "EQTL3", "GGBR4", "HAPV3", "HYPE3",
    "ITSA4", "ITUB4", "JBSS3", "KLBN11", "LREN3", "MGLU3", "MRVE3", "MULT3",
    "NTCO3", "PETR3", "PETR4", "PRIO3", "RADL3", "RAIL3", "RDOR3", "RENT3",
    "SBSP3", "SUZB3", "TIMS3", "TOTS3", "UGPA3", "USIM5", "VALE3", "VBBR3",
    "VIVT3", "WEGE3",
]

OUTORGAS: dict[int, OutorgaConfig] = {
    2023: OutorgaConfig(
        ano=2023,
        tickers=_IBRX50_2023_2024,
        dt_p0_ini=date(2023, 3, 1),
        dt_p0_fim=date(2023, 3, 29),
        dt_pf_ini=date(2026, 3, 2),
        dt_pf_fim=date(2026, 3, 31),
        dt_divs_ini=date(2023, 3, 1),
        dt_divs_fim=date(2026, 3, 31),
        # AZUL4: recuperação judicial → ação PN cancelada (AZUL53 é instrumento distinto)
        # JBSS3: saiu da B3 em jun/2025, virou BDR JBSS32 (BDI=35, não é ação)
        exclusoes_forcadas=["AZUL4", "JBSS3"],
        # EMBR3→EMBJ3, CPLE6→CPLE3: auto-detectados via COTAHIST (nome_exato)
        # ELET3→AXIA3, ELET6→AXIA6: rebranding completo — config manual necessária
        # CCRO3→MOTV3: rebranding completo — config manual necessária
        # NTCO3→NATU3: incorporação (nome mudou) — config manual necessária
        substituicoes={
            "EMBR3": "EMBJ3",
            "ELET3": "AXIA3",
            "ELET6": "AXIA6",
            "CCRO3": "MOTV3",
            "NTCO3": "NATU3",
        },
        divergencia_threshold=0.05,
    ),
    2024: OutorgaConfig(
        ano=2024,
        tickers=_IBRX50_2023_2024,
        dt_p0_ini=date(2024, 3, 1),
        dt_p0_fim=date(2024, 3, 28),
        dt_pf_ini=date(2026, 3, 2),
        dt_pf_fim=date(2026, 3, 31),
        dt_divs_ini=date(2024, 3, 1),
        dt_divs_fim=date(2026, 3, 31),
        exclusoes_forcadas=["AZUL4", "JBSS3"],
        substituicoes={
            "EMBR3": "EMBJ3",
            "ELET3": "AXIA3",
            "ELET6": "AXIA6",
            "CCRO3": "MOTV3",
            "NTCO3": "NATU3",
        },
        divergencia_threshold=0.05,
    ),
    2025: OutorgaConfig(
        ano=2025,
        tickers=_IBRX50_2025,
        dt_p0_ini=date(2025, 3, 5),
        dt_p0_fim=date(2025, 3, 31),
        dt_pf_ini=date(2026, 3, 2),
        dt_pf_fim=date(2026, 3, 31),
        dt_divs_ini=date(2025, 3, 1),
        dt_divs_fim=date(2026, 3, 31),
        # AZUL4: recuperação judicial / PN cancelada
        # JBSS3: virou BDR JBSS32 — não é ação
        exclusoes_forcadas=["AZUL4", "JBSS3"],
        # CPLE6→CPLE3 auto-detectado via COTAHIST; demais são rebrandings manuais
        # (CCRO3 e ELET6 não estão na lista 2025)
        substituicoes={
            "EMBR3": "EMBJ3",
            "ELET3": "AXIA3",
            "NTCO3": "NATU3",
        },
        divergencia_threshold=0.05,
    ),
}
