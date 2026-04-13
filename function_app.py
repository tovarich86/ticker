"""
Azure Function — Apuração LTI (TSR IBrX-50 TIM)
HTTP Trigger: GET/POST /api/apuracao

Parâmetros (query string):
  outorga  Anos separados por vírgula. Ex: ?outorga=2024 ou ?outorga=2023,2024,2025
           Omitir calcula todas as outorgas configuradas.

Retorno:
  - 1 outorga  → arquivo .xlsx para download
  - N outorgas → arquivo .zip com um .xlsx por outorga
"""

import io
import logging
import zipfile
from datetime import datetime

import azure.functions as func

from src import ticker_service
from src.lti.config import OUTORGAS
from src.lti.engine import calcular_todas_outorgas
from src.lti.excel_builder import gerar_excel_bytes, nome_arquivo

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="apuracao", methods=["GET", "POST"])
def apuracao_lti(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Apuração LTI — requisição recebida.")

    # ── Parâmetros ──────────────────────────────────────────────────────────
    outorga_param = req.params.get("outorga", "").strip()
    if outorga_param:
        try:
            anos = [int(a.strip()) for a in outorga_param.split(",") if a.strip()]
        except ValueError:
            return func.HttpResponse(
                "Parâmetro 'outorga' inválido. Use ex: ?outorga=2024 ou ?outorga=2023,2024,2025",
                status_code=400,
            )
    else:
        anos = list(OUTORGAS.keys())

    anos_validos = [a for a in anos if a in OUTORGAS]
    if not anos_validos:
        return func.HttpResponse(
            f"Outorgas inválidas: {anos}. Válidas: {list(OUTORGAS.keys())}",
            status_code=400,
        )

    logging.info(f"Calculando outorgas: {anos_validos}")

    # ── Base de empresas B3 ──────────────────────────────────────────────────
    df_empresas = ticker_service.carregar_empresas()
    if df_empresas.empty:
        return func.HttpResponse(
            "Erro: não foi possível carregar a base de empresas B3.",
            status_code=500,
        )
    logging.info(f"{len(df_empresas)} empresas B3 carregadas.")

    # ── Cálculo ──────────────────────────────────────────────────────────────
    try:
        resultados = calcular_todas_outorgas(anos_validos, df_empresas, logger=logging.info)
    except Exception as exc:
        logging.exception("Erro durante o cálculo.")
        return func.HttpResponse(f"Erro no cálculo: {exc}", status_code=500)

    if not resultados:
        return func.HttpResponse("Nenhum resultado gerado.", status_code=500)

    # ── Resposta ─────────────────────────────────────────────────────────────
    if len(anos_validos) == 1:
        # Única outorga → .xlsx direto
        ano = anos_validos[0]
        resultado = resultados[ano]
        xlsx_bytes = gerar_excel_bytes(resultado)
        filename = nome_arquivo(resultado)
        logging.info(f"Retornando {filename} ({len(xlsx_bytes):,} bytes).")
        return func.HttpResponse(
            body=xlsx_bytes,
            status_code=200,
            headers={
                "Content-Type": (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    else:
        # Múltiplas outorgas → .zip com um .xlsx por outorga
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for ano, resultado in resultados.items():
                xlsx_bytes = gerar_excel_bytes(resultado)
                zf.writestr(nome_arquivo(resultado), xlsx_bytes)

        ts = datetime.now().strftime("%Y%m%d")
        zip_filename = f"Apuracao_LTI_{ts}.zip"
        logging.info(f"Retornando {zip_filename} ({len(zip_buf.getvalue()):,} bytes).")
        return func.HttpResponse(
            body=zip_buf.getvalue(),
            status_code=200,
            headers={
                "Content-Type": "application/zip",
                "Content-Disposition": f'attachment; filename="{zip_filename}"',
            },
        )
