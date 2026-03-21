import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import di_service

st.set_page_config(page_title="Taxas DI1", layout="wide")
st.title("📉 Curva de Juros Futuros (DI1)")

st.markdown("Consulta de dados históricos da Curva DI via ADVFN. Escolha as datas e os vencimentos que pretende analisar.")

col_mode = st.radio("Modo de Consulta:", ["Data Única", "Múltiplas Datas (Arquivo)"], horizontal=True)

datas_para_buscar = []
tickers_selecionados = []

if col_mode == "Data Única":
    col1, col2 = st.columns([1, 2])
    
    with col1:
        d = st.date_input("Selecione a Data de Referência", datetime.now())
        datas_para_buscar = [d]
        
    with col2:
        # Gera as opções possíveis dinamicamente com base na data escolhida
        opcoes_disponiveis = di_service.gerar_opcoes_tickers(d)
        
        tickers_selecionados = st.multiselect(
            "Selecione os Vencimentos (Tickers)",
            options=opcoes_disponiveis,
            default=opcoes_disponiveis[:6] # Deixa os primeiros 6 pré-selecionados
        )
        
else:
    st.markdown("Envie um arquivo Excel ou CSV contendo uma coluna chamada **Data**.")
    f = st.file_uploader("Upload de arquivo", type=['xlsx', 'csv'])
    
    if f:
        df_in = pd.read_excel(f) if f.name.endswith('.xlsx') else pd.read_csv(f)
        col_data = next((c for c in df_in.columns if 'data' in c.lower()), None)
        
        if col_data:
            datas_para_buscar = pd.to_datetime(df_in[col_data], errors='coerce').dropna().dt.date.unique()
            st.success(f"{len(datas_para_buscar)} datas válidas identificadas no arquivo.")
            
            # Utiliza a data mais recente do arquivo como âncora para sugerir os tickers
            ref_date = max(datas_para_buscar) if len(datas_para_buscar) > 0 else datetime.now().date()
            opcoes_disponiveis = di_service.gerar_opcoes_tickers(ref_date, meses_curtos=12, anos_longos=10)
            
            tickers_selecionados = st.multiselect(
                "Selecione os Vencimentos que deseja monitorar para estas datas:",
                options=opcoes_disponiveis,
                default=opcoes_disponiveis[:4]
            )
        else:
            st.error("Não foi possível encontrar uma coluna com o nome 'Data' no arquivo enviado.")

# --- BOTÃO DE BUSCA ---
if st.button("Buscar Taxas", type="primary"):
    if not len(datas_para_buscar):
        st.warning("Nenhuma data selecionada ou válida.")
        st.stop()
        
    if not tickers_selecionados:
        st.warning("Por favor, selecione pelo menos um vencimento na caixa acima.")
        st.stop()

    results = []
    bar = st.progress(0)
    
    with st.spinner("Consultando dados no ADVFN..."):
        for i, data_ref in enumerate(datas_para_buscar):
            df, err = di_service.consultar_taxas_di_por_tickers(data_ref, tickers_selecionados)
            
            if df is not None and not df.empty:
                # Adiciona a data de referência à tabela
                df.insert(0, "DATA_REF", data_ref.strftime("%d/%m/%Y"))
                results.append(df)
            else:
                if col_mode == "Data Única": 
                    st.error(f"{data_ref.strftime('%d/%m/%Y')}: {err}")
                
            bar.progress((i + 1) / len(datas_para_buscar))
            
    if results:
        df_final = pd.concat(results, ignore_index=True)
        
        # --- LÓGICA DE AUDITORIA: Gera o link direto para a fonte ---
        df_final['FONTE_AUDITORIA'] = "https://br.advfn.com/bolsa-de-valores/bmf/" + df_final['VENCIMENTO'] + "/historico"
        
        st.success("Busca Finalizada com sucesso!")
        
        # Exibe a tabela formatando a coluna de auditoria como um link clicável
        st.dataframe(
            df_final, 
            use_container_width=True,
            column_config={
                "FONTE_AUDITORIA": st.column_config.LinkColumn(
                    "Link para Validação",
                    help="Clique para conferir o histórico original deste vencimento no ADVFN.",
                    display_text="Ver Fonte" # Substitui a URL feia por um texto limpo na tela
                )
            }
        )
        
        # Geração do arquivo para download (no Excel o link vai completo)
        out = BytesIO()
        with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, index=False)
        
        st.download_button(
            label="📥 Baixar Consolidado (Excel)", 
            data=out.getvalue(), 
            file_name="taxas_di.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("Nenhum dado foi retornado. Verifique se as datas escolhidas coincidem com feriados ou fins de semana.")
