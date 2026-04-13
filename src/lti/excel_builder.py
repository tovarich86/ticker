# src/lti/excel_builder.py
from __future__ import annotations

from datetime import datetime
from io import BytesIO

import pandas as pd
import xlsxwriter

from src.lti.engine import ApuracaoResult, TickerResult

# ---------------------------------------------------------------------------
# Notas de incerteza para casos onde a classificação (M&A vs substituição
# vs continuidade) não é consensual. Exibidas na aba Composição e Eventos_Corp.
# ---------------------------------------------------------------------------
_NOTAS_INCERTEZAS: dict[str, str] = {
    "NTCO3": (
        "INCERTEZA: NTCO3 incorporada por NATU3 (Natura &Co → Natura Cosméticos, 2024). "
        "Incorporação é juridicamente M&A. Interpretação adotada: substituição por NATU3. "
        "Alternativa: excluir como empresa que passou por M&A."
    ),
    "NATU3": "Ver nota NTCO3 — empresa resultante da incorporação.",
    "BRAV3": (
        "INCERTEZA: Brava Energia (BRAV3) formada pela fusão RRRP3 (3R Petroleum) + ENAT3 (Enauta) "
        "em nov/2024. Possível anacronismo na lista de março/2023 (empresa não existia nessa data). "
        "Verificar se o IBrX-50 de março/2023 continha RRRP3."
    ),
    "ELET3": (
        "INCERTEZA: Eletrobras privatizada em 2022 (subscrição com diluição dos acionistas). "
        "AXIA3 (Eneva) é a empresa adotada como substituta. Debatível se é evento de M&A "
        "ou reestruturação com continuidade econômica. Interpretação adotada: substituição."
    ),
    "ELET6": "INCERTEZA: Ação PN da Eletrobras. Ver nota ELET3.",
    "AXIA3": "Ver nota ELET3 — empresa efetiva adotada em substituição.",
    "AXIA6": "Ver nota ELET3 — ação PN da empresa efetiva.",
}

# Labels de evento que o Yahoo Finance ajusta retroativamente nos dividendos históricos
_LABELS_SPLIT = {"DESDOBRAMENTO", "GRUPAMENTO", "SPLIT_YF"}

# ---------------------------------------------------------------------------
# Helpers de formatação
# ---------------------------------------------------------------------------

_HDR  = {"bold": True, "bg_color": "#D0D8E8", "border": 1}
_SEC  = {"bold": True, "bg_color": "#C6EFCE", "border": 1}   # cabeçalho de seção (verde)
_WARN = {"bg_color": "#FFC7CE"}                               # alerta/incerteza (vermelho claro)
_TIM_BG = {"bg_color": "#FFFACD"}                             # linha TIMS3 (amarelo)
_FORMULA_NUM = {"num_format": "0.0000", "border": 1}
_FORMULA_PCT = {"num_format": "0.00", "border": 1}


def _fmt(wb, base: dict, extra: dict | None = None):
    d = {**base, **(extra or {})}
    return wb.add_format(d)


def _write_df(ws, df: pd.DataFrame, workbook, row_offset: int = 1) -> None:
    """Escreve um DataFrame na aba a partir de row_offset (1-based)."""
    fmt_hdr = workbook.add_format(_HDR)
    for col_idx, col_name in enumerate(df.columns):
        ws.write(row_offset - 1, col_idx, col_name, fmt_hdr)
    for r_idx, row in enumerate(df.itertuples(index=False), start=row_offset):
        for c_idx, val in enumerate(row):
            if isinstance(val, float) and pd.isna(val):
                ws.write(r_idx, c_idx, "")
            else:
                ws.write(r_idx, c_idx, val)


# ---------------------------------------------------------------------------
# 1. Resultado — ranking com fórmulas Excel explícitas para TSR
# ---------------------------------------------------------------------------
#
# Layout de colunas (índice 0-based / letra Excel):
#   A=0  Rank
#   B=1  Ticker
#   C=2  Ticker Original
#   D=3  Grupo
#   E=4  VWAP P0 (R$)
#   F=5  VWAP Pf (R$)
#   G=6  Dividendos/JCP (R$)
#   H=7  Mult. Corporativo
#   I=8  P Final Ajustado (R$)  →  =F*H
#   J=9  Ret. Preço (%)         →  =(I-E)/E*100
#   K=10 Ret. Divs (%)          →  =G/E*100
#   L=11 TSR (%)                →  =(I-E+G)/E*100   ← fórmula visível no Excel
# ---------------------------------------------------------------------------

def _sheet_resultado(wb, ws, resultado: ApuracaoResult) -> None:
    fmt_hdr = wb.add_format(_HDR)
    fmt_num = wb.add_format({"num_format": "0.0000"})
    fmt_pct = wb.add_format({"num_format": "0.00"})

    headers = [
        "Rank", "Ticker", "Ticker Original", "Grupo",
        "VWAP P0 (R$)", "VWAP Pf (R$)", "Dividendos/JCP (R$)", "Mult. Corporativo",
        "P Final Ajustado (R$)", "Ret. Preço (%)", "Ret. Divs (%)", "TSR (%)",
    ]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_hdr)
    ws.set_column(0, len(headers) - 1, 20)

    for r_idx, t in enumerate(resultado.ranking, start=1):
        is_tim = t.ticker == "TIMS3"
        bg = {"bg_color": "#FFFACD"} if is_tim else {}
        f_num = wb.add_format({**bg, "num_format": "0.0000"})
        f_pct = wb.add_format({**bg, "num_format": "0.00"})
        f_txt = wb.add_format(bg)

        vwap_p0 = t.vwap_p0 or 0.0
        vwap_pf = t.vwap_pf or 0.0
        mult    = t.mult_corporativo if t.mult_corporativo is not None else 1.0
        divs    = t.dividendos_total
        ex = r_idx + 1   # número da linha Excel (header = linha 1)

        ws.write(r_idx, 0, t.rank, f_txt)
        ws.write(r_idx, 1, t.ticker, f_txt)
        ws.write(r_idx, 2, t.ticker_original, f_txt)
        ws.write(r_idx, 3, t.grupo, f_txt)
        ws.write(r_idx, 4, round(vwap_p0, 4), f_num)
        ws.write(r_idx, 5, round(vwap_pf, 4), f_num)
        ws.write(r_idx, 6, round(divs, 4), f_num)
        ws.write(r_idx, 7, round(mult, 6), f_num)
        # Fórmulas Excel — mantém rastreabilidade do cálculo
        ws.write_formula(r_idx, 8,  f"=F{ex}*H{ex}",              f_num, round(vwap_pf * mult, 4))
        ws.write_formula(r_idx, 9,  f"=(I{ex}-E{ex})/E{ex}*100",  f_pct, round((vwap_pf * mult - vwap_p0) / vwap_p0 * 100, 2) if vwap_p0 else 0)
        ws.write_formula(r_idx, 10, f"=G{ex}/E{ex}*100",          f_pct, round(divs / vwap_p0 * 100, 2) if vwap_p0 else 0)
        ws.write_formula(r_idx, 11, f"=(I{ex}-E{ex}+G{ex})/E{ex}*100", f_pct,
                         round((vwap_pf * mult - vwap_p0 + divs) / vwap_p0 * 100, 2) if vwap_p0 else 0)


# ---------------------------------------------------------------------------
# 2. Grupos
# ---------------------------------------------------------------------------

def _sheet_grupos(wb, ws, resultado: ApuracaoResult) -> None:
    fmt_hdr = wb.add_format(_HDR)
    ws.write(0, 0, "Grupo", fmt_hdr)
    ws.write(0, 1, "Rank Inicial", fmt_hdr)
    ws.write(0, 2, "Rank Final", fmt_hdr)
    ws.write(0, 3, "TSR Médio (%)", fmt_hdr)
    ws.write(0, 4, "TSR Máx (%)", fmt_hdr)
    ws.write(0, 5, "TSR Mín (%)", fmt_hdr)
    ws.write(0, 6, "TIM no grupo?", fmt_hdr)
    ws.set_column(0, 6, 18)

    row_num = 1
    for g, members in sorted(resultado.grupos.items()):
        if not members:
            continue
        tsrs   = [m.tsr * 100 for m in members if m.tsr is not None]
        tim_   = any(m.ticker == "TIMS3" for m in members)
        ranks  = [m.rank for m in members if m.rank is not None]
        fmt    = wb.add_format({**({"bg_color": "#FFFACD"} if tim_ else {})})
        ws.write(row_num, 0, g, fmt)
        ws.write(row_num, 1, min(ranks) if ranks else "", fmt)
        ws.write(row_num, 2, max(ranks) if ranks else "", fmt)
        ws.write(row_num, 3, round(sum(tsrs) / len(tsrs), 4) if tsrs else "", fmt)
        ws.write(row_num, 4, round(max(tsrs), 4) if tsrs else "", fmt)
        ws.write(row_num, 5, round(min(tsrs), 4) if tsrs else "", fmt)
        ws.write(row_num, 6, "SIM" if tim_ else "não", fmt)
        row_num += 1


# ---------------------------------------------------------------------------
# 3. Composição — lista inicial vs final IBrX-50, com observações de incerteza
# ---------------------------------------------------------------------------

def _sheet_composicao(wb, ws, resultado: ApuracaoResult) -> None:
    cfg = resultado.outorga
    fmt_hdr  = wb.add_format(_HDR)
    fmt_warn = wb.add_format({**_WARN})
    fmt_norm = wb.add_format({})
    fmt_pct  = wb.add_format({"num_format": "0.00"})

    headers = [
        "Nº", "Ticker IBrX-50", "Ticker Efetivo", "Substituição",
        "Status", "Motivo Exclusão", "Rank", "TSR (%)",
        "Mult. Corporativo", "Eventos Corporativos", "Observação",
    ]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_hdr)
    ws.set_column(0,  0, 5)
    ws.set_column(1,  3, 18)
    ws.set_column(4,  5, 22)
    ws.set_column(6,  8, 16)
    ws.set_column(9,  9, 35)
    ws.set_column(10, 10, 70)

    # Índice por ticker_original para lookup rápido
    by_orig: dict[str, TickerResult] = {t.ticker_original: t for t in resultado.tickers}

    for i, ticker_orig in enumerate(cfg.tickers, start=1):
        ticker_ef    = cfg.substituicoes.get(ticker_orig, ticker_orig)
        t            = by_orig.get(ticker_orig)
        status       = t.status if t else "N/A"
        motivo       = t.motivo_exclusao if t else ""
        rank_val     = t.rank if t and t.rank is not None else ""
        tsr_pct      = round(t.tsr * 100, 2) if t and t.tsr is not None else ""
        mult_corp    = round(t.mult_corporativo, 6) if t and t.mult_corporativo is not None else ""
        subs_txt     = f"{ticker_orig} → {ticker_ef}" if ticker_ef != ticker_orig else ""

        # Resumo dos eventos corporativos
        evs = (t.eventos_corporativos or []) if t else []
        ev_resumo = "; ".join(
            f"{ev.get('label', '')} {ev['date'].strftime('%d/%m/%Y')} (×{ev['mult']:.4g})"
            for ev in evs
        ) if evs else ""

        obs = _NOTAS_INCERTEZAS.get(ticker_orig, _NOTAS_INCERTEZAS.get(ticker_ef, ""))
        tem_incerteza = bool(obs)
        fmt_row = fmt_warn if tem_incerteza else fmt_norm

        ws.write(i, 0,  i, fmt_row)
        ws.write(i, 1,  ticker_orig, fmt_row)
        ws.write(i, 2,  ticker_ef, fmt_row)
        ws.write(i, 3,  subs_txt, fmt_row)
        ws.write(i, 4,  status, fmt_row)
        ws.write(i, 5,  motivo, fmt_row)
        ws.write(i, 6,  rank_val, fmt_row)
        if isinstance(tsr_pct, float):
            ws.write(i, 7, tsr_pct, wb.add_format({**({**_WARN} if tem_incerteza else {}), "num_format": "0.00"}))
        else:
            ws.write(i, 7, tsr_pct, fmt_row)
        ws.write(i, 8,  mult_corp, fmt_row)
        ws.write(i, 9,  ev_resumo, fmt_row)
        ws.write(i, 10, obs, fmt_row)

    # Legenda de cores
    leg_row = len(cfg.tickers) + 2
    ws.write(leg_row, 0, "Legenda:", wb.add_format({"bold": True}))
    ws.write(leg_row + 1, 0, "Fundo vermelho claro", wb.add_format(_WARN))
    ws.write(leg_row + 1, 1, "Caso com incerteza de classificação (M&A vs substituição)")


# ---------------------------------------------------------------------------
# 4. Metodologia — regras de cálculo para replicação por auditor externo
# ---------------------------------------------------------------------------

def _sheet_metodologia(wb, ws, resultado: ApuracaoResult) -> None:
    cfg = resultado.outorga
    fmt_sec  = wb.add_format({**_SEC, "font_size": 11})
    fmt_lbl  = wb.add_format({"bold": True, "valign": "top"})
    fmt_val  = wb.add_format({"text_wrap": True, "valign": "top"})
    fmt_mono = wb.add_format({"font_name": "Courier New", "text_wrap": True, "valign": "top", "bg_color": "#F5F5F5"})

    ws.set_column(0, 0, 30)
    ws.set_column(1, 1, 90)
    ws.set_row(0, 20)

    def sec(row, title):
        ws.merge_range(row, 0, row, 1, title, fmt_sec)
        return row + 1

    def row_(ws_obj, r, label, value, mono=False):
        ws_obj.write(r, 0, label, fmt_lbl)
        ws_obj.write(r, 1, value, fmt_mono if mono else fmt_val)
        ws_obj.set_row(r, max(15, min(90, len(str(value)) // 2)))
        return r + 1

    r = 0
    r = sec(r, "METODOLOGIA DE CÁLCULO — APURAÇÃO LTI")
    r += 1

    # ── TSR ─────────────────────────────────────────────────────────────────
    r = sec(r, "1. TSR (Total Shareholder Return)")
    r = row_(ws, r, "Fórmula geral",
             "TSR = (P_final × Mult_Corp + Div_total − P0) / P0", mono=True)
    r = row_(ws, r, "P0",
             f"VWAP do período inicial P0: {cfg.dt_p0_ini.strftime('%d/%m/%Y')} a {cfg.dt_p0_fim.strftime('%d/%m/%Y')}")
    r = row_(ws, r, "P_final",
             f"VWAP do período final Pf: {cfg.dt_pf_ini.strftime('%d/%m/%Y')} a {cfg.dt_pf_fim.strftime('%d/%m/%Y')}")
    r = row_(ws, r, "Mult_Corp",
             "Produto de todos os multiplicadores de eventos corporativos no período de dividendos. "
             "Ver coluna 'Mult. Corporativo' na aba Resultado e detalhes na aba Eventos_Corp.")
    r = row_(ws, r, "Div_total",
             "Soma dos proventos (dividendos + JCP) ajustados pelo multiplicador corporativo "
             "acumulado até a data ex de cada provento. Ver aba DivAjustados.")
    r += 1

    # ── VWAP ────────────────────────────────────────────────────────────────
    r = sec(r, "2. VWAP (Volume-Weighted Average Price)")
    r = row_(ws, r, "Fórmula",
             "VWAP = Σ(Preço_médio_dia × Volume_dia) / Σ(Volume_dia)", mono=True)
    r = row_(ws, r, "Fonte dos dados",
             "COTAHIST B3 (arquivo diário de cotações). Cada dia útil do período é baixado individualmente.")
    r = row_(ws, r, "Fallback",
             "Se Volume total = 0 ou dado ausente, usa-se média simples dos preços diários.")
    r += 1

    # ── Eventos corporativos ─────────────────────────────────────────────────
    r = sec(r, "3. Eventos Corporativos — Regras de Multiplicador")
    r = row_(ws, r, "BONIFICAÇÃO",
             "Emissão de ações bonus (ex: 20% bonus = +20% de ações).\n"
             "Multiplicador: mult = 1 + factor/100\n"
             "Exemplo: factor=20 → mult=1.20\n"
             "Nota: o Yahoo Finance NÃO ajusta dividendos históricos para bonificações.", mono=True)
    r = row_(ws, r, "DESDOBRAMENTO (split)",
             "Divisão de ações (ex: 2:1 = dobro de ações a metade do preço).\n"
             "Multiplicador: mult = 1 + factor/100\n"
             "Exemplo: BBAS3 factor=100 → mult=2.0 (desdobramento 2:1)\n"
             "Exemplo: VIVT3 factor=7.900,00 (=7900) → mult=80 (parcela de par DESDOBRAMENTO+GRUPAMENTO)\n"
             "Nota: a B3 retorna fatores grandes no formato brasileiro (ponto=milhar, vírgula=decimal).", mono=True)
    r = row_(ws, r, "GRUPAMENTO (reverse split)",
             "Consolidação de ações (ex: 10:1 = 1/10 das ações a 10× o preço).\n"
             "Multiplicador: mult = factor (ratio direto, não percentual)\n"
             "Exemplo: MGLU3 factor=0.10 → mult=0.10 (grupamento 10:1)\n"
             "Exemplo: VIVT3 factor=0.025 → mult=0.025 (parcela de par DESDOBRAMENTO+GRUPAMENTO)", mono=True)
    r = row_(ws, r, "Par DESDOBRAMENTO+GRUPAMENTO\nna mesma data",
             "Utilizado pela B3 para representar splits com fatores grandes.\n"
             "Mult líquido = mult_desdobramento × mult_grupamento\n"
             "Exemplo neutro — TIMS3: 100 × 0.01 = 1.0 (limpeza de base, sem efeito real)\n"
             "Exemplo com split 2:1 — VIVT3: 80 × 0.025 = 2.0", mono=True)
    r = row_(ws, r, "RESG TOTAL RV",
             "Resgate de instrumento de renda variável. Sem efeito na quantidade de ações. Ignorado.", mono=True)
    r += 1

    # ── Ajuste de dividendos ─────────────────────────────────────────────────
    r = sec(r, "4. Ajuste de Dividendos por Eventos Corporativos")
    r = row_(ws, r, "Fórmula por provento",
             "div_ajustado_j = valor_bruto_j × mult_acumulado_até(data_ex_j)", mono=True)
    r = row_(ws, r, "Lógica",
             "Dividendos pagos ANTES de um split devem ser reescalonados para refletir que "
             "o acionista tinha mais ações (em termos das ações pós-split) no momento do pagamento. "
             "Dividendos pagos APÓS o split já estão na escala correta. "
             "Desta forma, a soma total é expressa em unidades de 1 ação original (pré-todos-eventos).")
    r = row_(ws, r, "Mult_YF vs Mult_Corp",
             "Mult_Corp = produto de TODOS os eventos (bonificação + split + grupamento).\n"
             "Mult_YF   = produto apenas de SPLITS/DESDOBRAMENTOS/GRUPAMENTOS.\n"
             "Distinção: o Yahoo Finance ajusta dividendos históricos retroativamente para splits "
             "mas NÃO para bonificações em ações. Mult_YF é usado apenas na verificação de "
             "consistência vs YF — não entra no cálculo do TSR.", mono=True)
    r += 1

    # ── Verificação YF ───────────────────────────────────────────────────────
    r = sec(r, "5. Verificação de Consistência com Yahoo Finance")
    r = row_(ws, r, "Metodologia",
             f"Para cada ticker, soma-se o total de proventos B3 (ajustado) e compara com o total "
             f"YF. Divergência > {cfg.divergencia_threshold*100:.0f}% é sinalizada na coluna "
             f"'Divergência YF' e na aba Divergencias_YF.")
    r = row_(ws, r, "Escalonamento YF",
             "total_b3 comparado com total_yf × Mult_YF (não Mult_Corp), pois o YF "
             "já ajustou seus dividendos para splits, mas não para bonificações.\n"
             "Fórmula: divergência% = |total_b3 − total_yf × mult_yf| / max(total_b3, total_yf × mult_yf)", mono=True)
    r = row_(ws, r, "Ticker YF para renomeados",
             "Para tickers efetivos diferentes do original (ex: CCRO3→MOTV3), o YF é consultado "
             "com o ticker ORIGINAL, pois o Yahoo Finance mantém histórico sob o nome antigo.")
    r += 1

    # ── Critérios de exclusão ────────────────────────────────────────────────
    r = sec(r, "6. Critérios de Exclusão")
    r = row_(ws, r, "Exclusões forçadas",
             f"Tickers em exclusoes_forcadas: {', '.join(cfg.exclusoes_forcadas) or 'nenhuma'}.\n"
             f"Razão: empresas deslistadas ou que passaram por M&A durante o período de apuração.")
    r = row_(ws, r, "Sem dados COTAHIST",
             "Ticker excluído se não há cotações disponíveis em P0 ou em Pf. "
             "Sem VWAP não é possível calcular o retorno de preço.")
    r = row_(ws, r, "Substituições",
             "Empresa renomeada/reestruturada que mantém continuidade econômica: "
             "usa-se o ticker efetivo para cotações Pf e dividendos, mas o ticker original para P0. "
             f"Substituições desta apuração: {cfg.substituicoes or 'nenhuma'}")
    r += 1

    # ── Lista IBrX-50 ─────────────────────────────────────────────────────────
    r = sec(r, "7. Composição IBrX-50 Utilizada")
    r = row_(ws, r, "Data de referência",
             "Lista congelada na data de início do período de apuração (março do ano da outorga). "
             "Empresas que saíram do índice após essa data permanecem no cálculo.")
    r = row_(ws, r, "Total de tickers", str(len(cfg.tickers)))
    r = row_(ws, r, "Lista completa", ", ".join(cfg.tickers))
    r += 1

    # ── Fontes de dados ───────────────────────────────────────────────────────
    r = sec(r, "8. Fontes de Dados")

    fmt_url  = wb.add_format({"font_color": "#0563C1", "underline": True, "valign": "top"})
    fmt_lbl2 = wb.add_format({"bold": True, "valign": "top"})
    fmt_val2 = wb.add_format({"text_wrap": True, "valign": "top"})

    def fonte(r_, label, descricao, url_display, url):
        ws.write(r_, 0, label, fmt_lbl2)
        ws.write(r_, 1, descricao, fmt_val2)
        ws.write(r_ + 1, 0, "URL", fmt_lbl2)
        ws.write_url(r_ + 1, 1, url, fmt_url, url_display)
        return r_ + 3

    r = fonte(
        r,
        "Cotações históricas (VWAP)",
        "B3 — Série Histórica de Cotações (COTAHIST). Arquivo diário em formato ZIP com "
        "todos os negócios do pregão. Baixado individualmente para cada dia útil do período P0 e Pf.\n"
        "Endpoint: https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{DDMMAAAA}.ZIP",
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/"
        "historico/mercado-a-vista/series-historicas/",
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/"
        "historico/mercado-a-vista/series-historicas/",
    )

    r = fonte(
        r,
        "Dividendos e JCP",
        "B3 — API de Proventos (GetListedCashDividends). Retorna todos os proventos "
        "declarados pela empresa (dividendos, JCP, rendimentos) com data ex e valor por ação.\n"
        "Endpoint: https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/"
        "CompanyCall/GetListedCashDividends/{params_base64}",
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/",
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/",
    )

    r = fonte(
        r,
        "Eventos corporativos\n(splits, grupamentos, bonificações)",
        "B3 — API de Complemento de Empresa (GetListedSupplementCompany). Retorna "
        "eventos societários como desdobramentos, grupamentos, bonificações, resgates.\n"
        "Endpoint: https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/"
        "CompanyCall/GetListedSupplementCompany/{params_base64}",
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/",
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/",
    )

    r = fonte(
        r,
        "Lista de empresas B3\n(trading name, tipo de ação)",
        "B3 — API de Empresas Listadas (GetInitialCompanies). Usada para mapear "
        "ticker → trading name e tipo de ação (ON/PN/UNT), necessário para filtrar "
        "os proventos corretos na API de dividendos.\n"
        "Endpoint: https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/"
        "CompanyCall/GetInitialCompanies/{params_base64}",
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/",
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/",
    )

    r = fonte(
        r,
        "Verificação cruzada de proventos\n(Yahoo Finance)",
        "Yahoo Finance — proventos históricos via biblioteca Python yfinance. "
        "Usado exclusivamente como verificação de consistência: divergência > threshold "
        "é sinalizada mas não altera o cálculo (dados B3 são primários).\n"
        "Biblioteca: pip install yfinance",
        "https://pypi.org/project/yfinance/",
        "https://pypi.org/project/yfinance/",
    )


# ---------------------------------------------------------------------------
# 5. Cotações P0 e Pf
# ---------------------------------------------------------------------------

def _sheet_cotacoes(wb, ws, resultado: ApuracaoResult, periodo: str) -> None:
    frames = []
    for t in resultado.tickers:
        df = t.df_cotacoes_p0 if periodo == "p0" else t.df_cotacoes_pf
        if df is not None and not df.empty:
            df2 = df.copy()
            df2["Ticker_Original"] = t.ticker_original
            frames.append(df2)
    if frames:
        df_all = pd.concat(frames, ignore_index=True)
        if "Date" in df_all.columns:
            df_all["Date"] = pd.to_datetime(df_all["Date"]).dt.strftime("%d/%m/%Y")
        _write_df(ws, df_all, wb)
    ws.set_column(0, 10, 16)


# ---------------------------------------------------------------------------
# 6. Dividendos brutos B3
# ---------------------------------------------------------------------------

def _sheet_dividendos(wb, ws, resultado: ApuracaoResult) -> None:
    frames = []
    for t in resultado.tickers:
        if t.df_dividendos is not None and not t.df_dividendos.empty:
            frames.append(t.df_dividendos.copy())
    if frames:
        _write_df(ws, pd.concat(frames, ignore_index=True), wb)
    ws.set_column(0, 10, 18)


# ---------------------------------------------------------------------------
# 7. Eventos corporativos — com colunas interpretadas para auditoria
# ---------------------------------------------------------------------------
#
# Colunas:
#   Ticker | Ticker Original | Data | Tipo | Factor (decimal) | Cálculo Mult.
#   Multiplicador Evento | Mult. Acumulado | Inclui em Mult_YF | Observação
# ---------------------------------------------------------------------------

def _sheet_eventos_corp(wb, ws, resultado: ApuracaoResult) -> None:
    fmt_hdr = wb.add_format(_HDR)
    fmt_warn = wb.add_format(_WARN)

    headers = [
        "Ticker", "Ticker Original", "Data", "Tipo",
        "Factor (decimal)", "Cálculo Multiplicador",
        "Mult. Evento", "Mult. Acumulado",
        "Inclui em Mult_YF?", "Observação",
    ]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_hdr)
    ws.set_column(0,  1, 16)
    ws.set_column(2,  3, 16)
    ws.set_column(4,  4, 16)
    ws.set_column(5,  5, 40)
    ws.set_column(6,  7, 16)
    ws.set_column(8,  8, 16)
    ws.set_column(9,  9, 70)

    r = 1
    for t in resultado.tickers:
        if t.status != "INCLUIDO":
            continue
        obs = _NOTAS_INCERTEZAS.get(t.ticker_original, _NOTAS_INCERTEZAS.get(t.ticker, ""))
        tem_incerteza = bool(obs)

        if not t.eventos_corporativos:
            fmt = fmt_warn if tem_incerteza else None
            ws.write(r, 0, t.ticker, fmt)
            ws.write(r, 1, t.ticker_original, fmt)
            ws.write(r, 2, "", fmt)
            ws.write(r, 3, "(sem eventos)", fmt)
            ws.write(r, 4, "", fmt)
            ws.write(r, 5, "", fmt)
            ws.write(r, 6, 1.0, fmt)
            ws.write(r, 7, 1.0, fmt)
            ws.write(r, 8, "", fmt)
            ws.write(r, 9, obs, fmt)
            r += 1
            continue

        acc = 1.0
        for ev in t.eventos_corporativos:
            label  = str(ev.get("label", "")).upper()
            factor = ev.get("factor", "")
            mult   = ev.get("mult", 1.0)
            acc   *= mult
            incl_yf = "SIM" if label in _LABELS_SPLIT else "NÃO"
            # Texto explicando como o multiplicador foi calculado
            if label == "GRUPAMENTO":
                calculo = f"direto: {factor} → mult={mult:.6g}"
            elif label == "SPLIT_YF":
                calculo = f"ratio YF direto: {factor} → mult={mult:.6g}"
            else:
                # BONIFICACAO ou DESDOBRAMENTO: 1 + factor/100
                calculo = f"1 + {factor}/100 = {mult:.6g}"

            fmt = fmt_warn if tem_incerteza else None
            dt_str = ev["date"].strftime("%d/%m/%Y") if hasattr(ev.get("date"), "strftime") else str(ev.get("date", ""))
            ws.write(r, 0, t.ticker, fmt)
            ws.write(r, 1, t.ticker_original, fmt)
            ws.write(r, 2, dt_str, fmt)
            ws.write(r, 3, ev.get("label", ""), fmt)
            ws.write(r, 4, factor, fmt)
            ws.write(r, 5, calculo, fmt)
            ws.write(r, 6, round(mult, 6), fmt)
            ws.write(r, 7, round(acc, 6), fmt)
            ws.write(r, 8, incl_yf, fmt)
            ws.write(r, 9, obs, fmt)
            r += 1

    # Legenda
    leg = r + 1
    ws.write(leg, 0, "Legenda:", wb.add_format({"bold": True}))
    ws.write(leg + 1, 0, "Mult_YF", wb.add_format({"bold": True}))
    ws.write(leg + 1, 1,
             "Subconjunto do Mult. Corporativo que inclui apenas DESDOBRAMENTO/GRUPAMENTO/SPLIT_YF. "
             "Yahoo Finance ajusta dividendos históricos para esses eventos, mas NÃO para BONIFICAÇÃO.")
    ws.write(leg + 2, 0, "Fundo vermelho", wb.add_format(_WARN))
    ws.write(leg + 2, 1, "Ticker com incerteza de classificação — ver coluna Observação.")


# ---------------------------------------------------------------------------
# 8. DivAjustados — com fórmula Excel explícita no Total Recebido
# ---------------------------------------------------------------------------
#
# Colunas (índice 0-based / letra Excel):
#   A=0  Ticker
#   B=1  Data Ex
#   C=2  Pagamento
#   D=3  Tipo
#   E=4  Valor/Ação (R$)
#   F=5  Multiplicador
#   G=6  Total Recebido (R$)  →  =E*F   ← fórmula visível
# ---------------------------------------------------------------------------

def _sheet_divs_ajustados(wb, ws, resultado: ApuracaoResult) -> None:
    fmt_hdr  = wb.add_format(_HDR)
    fmt_num  = wb.add_format({"num_format": "0.000000"})
    fmt_note = wb.add_format({"italic": True, "font_color": "#555555"})

    headers = ["Ticker", "Data Ex", "Pagamento", "Tipo",
               "Valor/Ação (R$)", "Multiplicador", "Total Recebido (R$)"]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_hdr)
    ws.set_column(0, 6, 22)

    # Nota explicativa sobre a coluna G
    ws.write_comment(0, 6,
        "Fórmula: =Valor/Ação × Multiplicador\n"
        "Multiplicador = produto dos eventos corporativos ocorridos ATÉ a data ex deste dividendo.\n"
        "Dividendo pago antes de um split tem mult > 1.0 (pois o acionista tinha mais ações naquele momento).")

    rows = []
    for t in resultado.tickers:
        for d in t.divs_ajustados:
            rows.append({"Ticker": t.ticker, **d})

    for r_idx, row_data in enumerate(rows, start=1):
        ex = r_idx + 1  # linha Excel
        ws.write(r_idx, 0, row_data.get("Ticker", ""))
        ws.write(r_idx, 1, row_data.get("Data Ex", ""))
        ws.write(r_idx, 2, row_data.get("Pagamento", ""))
        ws.write(r_idx, 3, row_data.get("Tipo", ""))
        ws.write(r_idx, 4, row_data.get("Valor/Ação (R$)", 0), fmt_num)
        ws.write(r_idx, 5, row_data.get("Multiplicador", 1.0), fmt_num)
        # Fórmula explícita: Total Recebido = Valor/Ação × Multiplicador
        val_orig = row_data.get("Valor/Ação (R$)", 0)
        mult_v   = row_data.get("Multiplicador", 1.0)
        ws.write_formula(r_idx, 6, f"=E{ex}*F{ex}", fmt_num, round(val_orig * mult_v, 6))

    # Linha de total
    if rows:
        total_row = len(rows) + 1
        ws.write(total_row, 5, "TOTAL:", wb.add_format({"bold": True}))
        ws.write_formula(total_row, 6,
                         f"=SUM(G2:G{total_row})",
                         wb.add_format({"bold": True, "num_format": "0.0000"}),
                         sum(r.get("Total Recebido (R$)", 0) for r in rows))


# ---------------------------------------------------------------------------
# 9. Exclusões
# ---------------------------------------------------------------------------

def _sheet_exclusoes(wb, ws, resultado: ApuracaoResult) -> None:
    fmt_hdr = wb.add_format(_HDR)
    headers = ["Ticker", "Ticker Original", "Status", "Motivo", "Divergência YF"]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_hdr)
    ws.set_column(0, 4, 30)
    r = 1
    for t in resultado.tickers:
        if t.status != "INCLUIDO" or t.divergencia_yf:
            ws.write(r, 0, t.ticker)
            ws.write(r, 1, t.ticker_original)
            ws.write(r, 2, t.status)
            ws.write(r, 3, t.motivo_exclusao)
            ws.write(r, 4, t.divergencia_yf or "")
            r += 1


# ---------------------------------------------------------------------------
# 10. Divergências YF
# ---------------------------------------------------------------------------

def _sheet_divergencias_yf(wb, ws, resultado: ApuracaoResult) -> None:
    fmt_hdr = wb.add_format(_HDR)
    headers = ["Ticker", "Ticker Original", "Divergência (descrição)"]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_hdr)
    ws.set_column(0, 2, 40)
    r = 1
    for t in resultado.tickers:
        if t.divergencia_yf:
            ws.write(r, 0, t.ticker)
            ws.write(r, 1, t.ticker_original)
            ws.write(r, 2, t.divergencia_yf)
            r += 1


# ---------------------------------------------------------------------------
# 11. Config
# ---------------------------------------------------------------------------

def _sheet_config(wb, ws, resultado: ApuracaoResult) -> None:
    fmt_hdr  = wb.add_format(_HDR)
    fmt_sec  = wb.add_format({**_SEC, "bold": True})
    fmt_warn = wb.add_format({**_WARN, "text_wrap": True})
    fmt_norm = wb.add_format({"text_wrap": True})

    cfg = resultado.outorga
    ws.set_column(0, 0, 32)
    ws.set_column(1, 1, 80)

    r = 0
    ws.write(r, 0, "Parâmetro", fmt_hdr)
    ws.write(r, 1, "Valor", fmt_hdr)
    r += 1

    params = [
        ("Ano Outorga",           cfg.ano),
        ("Data Rodada",           resultado.timestamp.strftime("%d/%m/%Y %H:%M")),
        ("Período P0 Início",     cfg.dt_p0_ini.strftime("%d/%m/%Y")),
        ("Período P0 Fim",        cfg.dt_p0_fim.strftime("%d/%m/%Y")),
        ("Período Pf Início",     cfg.dt_pf_ini.strftime("%d/%m/%Y")),
        ("Período Pf Fim",        cfg.dt_pf_fim.strftime("%d/%m/%Y")),
        ("Período Divs Início",   cfg.dt_divs_ini.strftime("%d/%m/%Y")),
        ("Período Divs Fim",      cfg.dt_divs_fim.strftime("%d/%m/%Y")),
        ("Total Tickers IBrX-50", len(cfg.tickers)),
        ("Incluídos no Ranking",  resultado.n_incluidos),
        ("Excluídos",             resultado.n_excluidos),
        ("Threshold Divergência YF", f"{cfg.divergencia_threshold*100:.0f}%"),
        ("Exclusões Forçadas",    ", ".join(cfg.exclusoes_forcadas) or "nenhuma"),
        ("Substituições",         str(cfg.substituicoes) if cfg.substituicoes else "nenhuma"),
    ]
    for k, v in params:
        ws.write(r, 0, k)
        ws.write(r, 1, str(v) if not isinstance(v, (int, float)) else v)
        r += 1

    r += 1
    ws.merge_range(r, 0, r, 1, "Lista IBrX-50 (inicial — congelada na data da outorga)", fmt_sec)
    r += 1
    ws.write(r, 0, "Tickers IBrX-50", fmt_hdr)
    ws.write(r, 1, ", ".join(cfg.tickers), fmt_norm)
    r += 1

    # Substituições com explicação
    if cfg.substituicoes:
        r += 1
        ws.merge_range(r, 0, r, 1, "Substituições (ticker original → ticker efetivo)", fmt_sec)
        r += 1
        ws.write(r, 0, "Ticker Original", fmt_hdr)
        ws.write(r, 1, "Ticker Efetivo / Motivo / Observação", fmt_hdr)
        r += 1
        _subs_obs = {
            "EMBR3":  "EMBJ3 — Embraer: reorganização societária (EMBR3 → EMBJ3)",
            "CCRO3":  "MOTV3 — CCR renomeada para Motiva (rebranding, não M&A)",
            "NTCO3":  "NATU3 — INCERTEZA: incorporação de NTCO3 pela NATU3 (ver aba Composição)",
            "ELET3":  "AXIA3 — INCERTEZA: Eletrobras privatizada; AXIA3 (Eneva) adotada como substituta",
            "ELET6":  "AXIA6 — INCERTEZA: ação PN da mesma situação (ver ELET3)",
        }
        for orig, ef in cfg.substituicoes.items():
            obs_subs = _subs_obs.get(orig, ef)
            tem_inc  = "INCERTEZA" in _subs_obs.get(orig, "")
            ws.write(r, 0, orig, fmt_warn if tem_inc else fmt_norm)
            ws.write(r, 1, obs_subs, fmt_warn if tem_inc else fmt_norm)
            r += 1

    # Casos com incerteza
    r += 1
    ws.merge_range(r, 0, r, 1, "Casos com Incerteza de Classificação", fmt_sec)
    r += 1
    for ticker, nota in _NOTAS_INCERTEZAS.items():
        ws.write(r, 0, ticker, fmt_warn)
        ws.write(r, 1, nota, fmt_warn)
        ws.set_row(r, 40)
        r += 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _escrever_excel(resultado: ApuracaoResult, buffer) -> None:
    with xlsxwriter.Workbook(buffer, {"in_memory": True}) as wb:
        # Ordem pensada para um auditor: resultado → composição → detalhe → dados brutos → config
        _sheet_resultado(wb,      wb.add_worksheet("Resultado"),      resultado)
        _sheet_grupos(wb,         wb.add_worksheet("Grupos"),          resultado)
        _sheet_composicao(wb,     wb.add_worksheet("Composição"),      resultado)
        _sheet_metodologia(wb,    wb.add_worksheet("Metodologia"),     resultado)
        _sheet_divs_ajustados(wb, wb.add_worksheet("DivAjustados"),    resultado)
        _sheet_eventos_corp(wb,   wb.add_worksheet("Eventos_Corp"),    resultado)
        _sheet_dividendos(wb,     wb.add_worksheet("Dividendos"),      resultado)
        _sheet_cotacoes(wb,       wb.add_worksheet("Cotacao_P0"),      resultado, "p0")
        _sheet_cotacoes(wb,       wb.add_worksheet("Cotacao_Pf"),      resultado, "pf")
        _sheet_exclusoes(wb,      wb.add_worksheet("Exclusoes"),       resultado)
        _sheet_divergencias_yf(wb, wb.add_worksheet("Divergencias_YF"), resultado)
        _sheet_config(wb,         wb.add_worksheet("Config"),           resultado)


def gerar_excel_bytes(resultado: ApuracaoResult) -> bytes:
    """Gera o Excel auditável em memória e retorna como bytes (para Streamlit)."""
    buf = BytesIO()
    _escrever_excel(resultado, buf)
    return buf.getvalue()


def salvar_excel(resultado: ApuracaoResult, path: str) -> None:
    """Grava o Excel auditável em disco (para CLI)."""
    with open(path, "wb") as f:
        f.write(gerar_excel_bytes(resultado))


def nome_arquivo(resultado: ApuracaoResult) -> str:
    """Retorna o nome padrão do arquivo: Apuracao_LTI_<ano>_<YYYYMMDD>.xlsx"""
    return f"Apuracao_LTI_{resultado.outorga.ano}_{resultado.timestamp.strftime('%Y%m%d')}.xlsx"
