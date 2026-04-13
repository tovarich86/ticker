import io, zipfile, datetime, requests, urllib3
import polars as pl
from concurrent.futures import ThreadPoolExecutor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Mapeamento oficial de posições do arquivo COTAHIST da B3
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
    f = (b + 8) // 25
    g = (b - f + 1) // 3
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

def listar_dias_uteis(inicio, fim):
    dias = []
    curr = inicio
    feriados_cache = {}
    while curr <= fim:
        if curr.year not in feriados_cache:
            feriados_cache[curr.year] = obter_feriados_b3(curr.year)
        if curr.weekday() < 5 and curr not in feriados_cache[curr.year]:
            dias.append(curr)
        curr += datetime.timedelta(days=1)
    return dias

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
        df_parsed = df_filtered.with_columns(slices).drop('raw').with_columns([
            pl.col('FATOR_DE_COTACAO').cast(pl.Float64).alias('_FATCOT'),
        ])
        # Preços são (13)V99 → ÷100; depois ÷FATCOT para corrigir cotações históricas (ex: FATCOT=1000 pré-2010)
        return df_parsed.with_columns([
            pl.col('DATA_DO_PREGAO').str.to_date('%Y%m%d').alias('Date'),
            pl.col('CODIGO_DE_NEGOCIACAO').alias('Ticker'),
            (pl.col('PRECO_DE_ABERTURA').cast(pl.Float64) / 100 / pl.col('_FATCOT')).alias('Open'),
            (pl.col('PRECO_MAXIMO').cast(pl.Float64) / 100 / pl.col('_FATCOT')).alias('High'),
            (pl.col('PRECO_MINIMO').cast(pl.Float64) / 100 / pl.col('_FATCOT')).alias('Low'),
            (pl.col('PRECO_ULTIMO_NEGOCIO').cast(pl.Float64) / 100 / pl.col('_FATCOT')).alias('Close'),
            (pl.col('PRECO_MEDIO').cast(pl.Float64) / 100 / pl.col('_FATCOT')).alias('Average'),
            # VOLTOT é (16)V99 → ÷100
            pl.col('VOLUME_TOTAL_NEGOCIADO').cast(pl.Float64).truediv(100).alias('Volume'),
            pl.col('QUANTIDADE_NEGOCIADA').cast(pl.Int64).alias('Quantity'),
        ]).select(['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Average', 'Volume', 'Quantity']).to_pandas()
    except: return None


def parsear_acoes_dia(data_pregao: datetime.date, session) -> pl.DataFrame | None:
    """
    Baixa e parseia um dia do COTAHIST retornando apenas ações à vista
    (BDI 02 ou 12, TIPO_DE_MERCADO 010) com ticker, ISIN e nome da empresa.
    Usado internamente por detectar_substituicoes_cotahist().

    Returns DataFrame com colunas [ticker, isin, nome] ou None em caso de erro.
    """
    url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{data_pregao.strftime("%d%m%Y")}.ZIP'
    try:
        r = session.get(url, verify=False, timeout=10)
        if r.status_code == 404:
            return None
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            dados = z.read(z.namelist()[0])
        df = pl.read_csv(
            io.BytesIO(dados),
            has_header=False,
            new_columns=['raw'],
            encoding='latin1',
            separator='|',
        )
        # Slice off header/trailer rows (first and last)
        df = df.slice(1, -1)
        slices = []
        start = 0
        for col, width in FIELD_SIZES.items():
            slices.append(pl.col('raw').str.slice(start, width).str.strip_chars().alias(col))
            start += width
        df_parsed = df.with_columns(slices).drop('raw')
        df_acoes = df_parsed.filter(
            (pl.col('TIPO_DE_REGISTRO') == '01') &
            (pl.col('CODIGO_BDI').is_in(['2', '02', '12'])) &
            (pl.col('TIPO_DE_MERCADO') == '010')
        ).select([
            pl.col('CODIGO_DE_NEGOCIACAO').alias('ticker'),
            pl.col('CODIGO_ISIN').alias('isin'),
            pl.col('NOME_DA_EMPRESA').alias('nome'),
        ])
        return df_acoes
    except:
        return None


def detectar_substituicoes_cotahist(
    tickers: list,
    dt_origem: datetime.date,
    dt_alvo: datetime.date,
    session=None,
) -> dict:
    """
    Para tickers ausentes no período alvo, detecta substitutos via COTAHIST.

    Estratégia (em ordem):
    1. ISIN matching — mesmo ISIN, novo ticker (funciona para conversões simples)
    2. NOME exato — mesmo NOME_DA_EMPRESA (funciona para mudanças de classe: CPLE6→CPLE3)
    3. NOME prefixo — primeiro token do nome, mínimo 4 chars, exatamente 1 match
       (funciona para simplificações: "GRUPO NATURA" → "NATURA")

    Tickers já presentes em dt_alvo não são incluídos no resultado.
    Se nenhuma estratégia funcionar, retorna substituto=None.

    Args:
        tickers: lista de tickers a verificar (ex: IBrX-50 list)
        dt_origem: data de amostra no período de origem (onde os tickers existiam)
        dt_alvo: data de amostra no período alvo (onde queremos o equivalente)
        session: requests.Session opcional (criado internamente se None)

    Returns:
        Dict somente com tickers AUSENTES em dt_alvo:
        {
            'ELET3': {'substituto': None,   'metodo': 'sem_match',   'nome_orig': 'ELETROBRAS', 'nome_subst': None},
            'CPLE6': {'substituto': 'CPLE3','metodo': 'nome_exato',  'nome_orig': 'COPEL',      'nome_subst': 'COPEL'},
            'EMBR3': {'substituto': 'EMBJ3','metodo': 'nome_exato',  'nome_orig': 'EMBRAER',    'nome_subst': 'EMBRAER'},
        }
    """
    _own_session = session is None
    if _own_session:
        session = requests.Session()
    try:
        df_origem = parsear_acoes_dia(dt_origem, session)
        df_alvo = parsear_acoes_dia(dt_alvo, session)
        if df_origem is None or df_alvo is None:
            return {}

        tickers_alvo = set(df_alvo['ticker'].to_list())

        # Build lookups from alvo DataFrame
        isin_to_ticker_alvo: dict = {}
        nome_to_ticker_alvo: dict = {}
        for row in df_alvo.iter_rows(named=True):
            isin_to_ticker_alvo[row['isin']] = row['ticker']
            nome_to_ticker_alvo[row['nome']] = row['ticker']

        # Build lookup from origem DataFrame
        ticker_to_isin_orig: dict = {}
        ticker_to_nome_orig: dict = {}
        for row in df_origem.iter_rows(named=True):
            ticker_to_isin_orig[row['ticker']] = row['isin']
            ticker_to_nome_orig[row['ticker']] = row['nome']

        result = {}
        for ticker in tickers:
            # Skip tickers already present in alvo
            if ticker in tickers_alvo:
                continue

            isin_orig = ticker_to_isin_orig.get(ticker)
            nome_orig = ticker_to_nome_orig.get(ticker)

            substituto = None
            metodo = 'sem_match'
            nome_subst = None

            # Strategy 1: ISIN matching
            if isin_orig and isin_orig in isin_to_ticker_alvo:
                cand = isin_to_ticker_alvo[isin_orig]
                if cand != ticker:
                    substituto = cand
                    metodo = 'isin'
                    nome_subst = ticker_to_nome_orig.get(cand) or nome_to_ticker_alvo.get(nome_orig)
                    # Get nome_subst from alvo rows
                    for row in df_alvo.iter_rows(named=True):
                        if row['ticker'] == cand:
                            nome_subst = row['nome']
                            break

            # Strategy 2: Exact NOME matching
            if substituto is None and nome_orig and nome_orig in nome_to_ticker_alvo:
                cand = nome_to_ticker_alvo[nome_orig]
                if cand != ticker:
                    substituto = cand
                    metodo = 'nome_exato'
                    nome_subst = nome_orig

            # Strategy 3: Prefix NOME matching (first token >= 4 chars, exactly 1 match)
            if substituto is None and nome_orig:
                prefix = nome_orig.split()[0] if nome_orig.split() else ''
                if len(prefix) >= 4:
                    matches = [
                        row['ticker']
                        for row in df_alvo.iter_rows(named=True)
                        if row['nome'].startswith(prefix) and row['ticker'] != ticker
                    ]
                    if len(matches) == 1:
                        substituto = matches[0]
                        metodo = 'nome_prefixo'
                        for row in df_alvo.iter_rows(named=True):
                            if row['ticker'] == substituto:
                                nome_subst = row['nome']
                                break

            result[ticker] = {
                'substituto': substituto,
                'metodo': metodo,
                'nome_orig': nome_orig,
                'nome_subst': nome_subst,
            }

        return result
    finally:
        if _own_session:
            session.close()
