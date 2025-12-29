# b3_engine.py
import io, zipfile, datetime, requests, urllib3
import polars as pl
from concurrent.futures import ThreadPoolExecutor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FIELD_SIZES = {
    'TIPO_DE_REGISTRO': 2, 'DATA_DO_PREGAO': 8, 'CODIGO_BDI': 2,
    'CODIGO_DE_NEGOCIACAO': 12, 'TIPO_DE_MERCADO': 3, 'NOME_DA_EMPRESA': 12,
    'ESPECIFICACAO_DO_PAPEL': 10, 'PRAZO_EM_DIAS_DO_MERCADO_A_TERMO': 3,
    'MOEDA_DE_REFERENCIA': 4, 'PRECO_DE_ABERTURA': 13, 'PRECO_MAXIMO': 13,
    'PRECO_MINIMO': 13, 'PRECO_MEDIO': 13, 'PRECO_ULTIMO_NEGOCIO': 13,
    'PRECO_MELHOR_OFERTA_DE_COMPRA': 13, 'PRECO_MELHOR_OFERTA_DE_VENDAS': 13,
    'NUMERO_DE_NEGOCIOS': 5, 'QUANTIDADE_NEGOCIADA': 18, 'VOLUME_TOTAL_NEGOCIADO': 18,
    'PRECO_DE_EXERCICIO': 13, 'INDICADOR_DE_CORRECAO_DE_PRECOS': 1,
    'DATA_DE_VENCIMENTO': 8, 'FATOR_DE_COTACAO': 7, 'PRECO_DE_EXERCICIO_EM_PONTOS': 13,
    'CODIGO_ISIN': 12, 'NUMERO_DE_DISTRIBUICAO': 3,
}

def _calc_pascoa(ano: int):
    a, b, c = ano % 19, ano // 100, ano % 100
    d, e = b // 4, b % 4
    f, g = (b + 8) // 25, (b - (b + 8) // 25 + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = ((h + l - 7 * m + 114) % 31) + 1
    return datetime.date(ano, mes, dia)

def obter_feriados_b3(ano: int):
    pascoa = _calc_pascoa(ano)
    feriados = [
        datetime.date(ano, 1, 1), pascoa - datetime.timedelta(days=48), 
        pascoa - datetime.timedelta(days=47), pascoa - datetime.timedelta(days=2), 
        datetime.date(ano, 4, 21), datetime.date(ano, 5, 1),
        pascoa + datetime.timedelta(days=60), datetime.date(ano, 9, 7), 
        datetime.date(ano, 10, 12), datetime.date(ano, 11, 2), 
        datetime.date(ano, 11, 15), datetime.date(ano, 12, 25)
    ]
    if ano >= 2024: feriados.append(datetime.date(ano, 11, 20))
    return feriados

def baixar_e_parsear_dia(data_pregao, tickers_b3, session):
    url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{data_pregao.strftime("%d%m%Y")}.ZIP'
    try:
        r = session.get(url, verify=False, timeout=10)
        if r.status_code == 404: return None
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            dados = z.read(z.namelist()[0])
        df = pl.read_csv(io.BytesIO(dados), has_header=False, new_columns=['raw'], encoding='latin1', separator='|')
        df_filtered = df.slice(1, -1).filter(pl.col('raw').str.slice(12, 12).str.strip_chars().is_in(tickers_b3))
        if df_filtered.is_empty(): return None
        slices = []
        start = 0
        for col, width in FIELD_SIZES.items():
            slices.append(pl.col('raw').str.slice(start, width).str.strip_chars().alias(col))
            start += width
        return df_filtered.with_columns(slices).drop('raw').with_columns([
            pl.col('DATA_DO_PREGAO').str.to_date('%Y%m%d').alias('Date'),
            pl.col('CODIGO_DE_NEGOCIACAO').alias('Ticker'),
            pl.col('PRECO_ULTIMO_NEGOCIO').cast(pl.Float64).truediv(100).alias('Close')
            pl.col('VOLUME_TOTAL_NEGOCIADO').cast(pl.Float64).truediv(100).alias('Close')
            # ... adicione Open, High, Low, Volume se necessário
        ]).to_pandas()
    except: return None
