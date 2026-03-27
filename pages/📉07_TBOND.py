import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from io import BytesIO
from datetime import datetime, timedelta

st.set_page_config(page_title="Curva US Treasuries", layout="wide")
st.title("🇺🇸 Curva de Juros (US Treasuries)")

st.markdown("Consulta da curva completa de rendimentos do **US Department of the Treasury** para uma data específica.")

# Mapeamento de todas as tags oficiais do XML para exibição e ordenação cronológica
TREASURY_MATURITIES = {
    "BC_1MONTH": {"nome": "1 Mês", "ordem": 1},
    "BC_2MONTH": {"nome": "2 Meses", "ordem": 2},
    "BC_3MONTH": {"nome": "3 Meses", "ordem": 3},
    "BC_4MONTH": {"nome": "4 Meses", "ordem": 4},
    "BC_6MONTH": {"nome": "6 Meses", "ordem": 6},
    "BC_1YEAR": {"nome": "1 Ano", "ordem": 12},
    "BC_2YEAR": {"nome": "2 Anos", "ordem": 24},
    "BC_3YEAR": {"nome": "3 Anos", "ordem": 36},
    "BC_5YEAR": {"nome": "5 Anos", "ordem": 60},
    "BC_7YEAR": {"nome": "7 Anos", "ordem": 84},
    "BC_10YEAR": {"nome": "10 Anos", "ordem": 120},
    "BC_20YEAR": {"nome": "20 Anos", "ordem": 240},
    "BC_30YEAR": {"nome": "30 Anos", "ordem": 360}
}

col1, col2 = st.columns([1, 2])

with col1:
    # Por padrão, sugere o dia anterior (pois o dia atual pode ainda não ter fechado)
    data_ref = st.date_input("Selecione a Data de Referência", datetime.now() - timedelta(days=1))

if st.button("Buscar Curva de Juros", type="primary"):
    
    ano = data_ref.year
    url_xml = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value={ano}"
    
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
    }
    
    resultados = []
    encontrou_data = False
    
    with st.spinner(f"Consultando dados do Tesouro Americano para {data_ref.strftime('%d/%m/%Y')}..."):
        try:
            response = requests.get(url_xml, verify=False, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            data_str_busca = data_ref.strftime("%Y-%m-%d")
            
            # Varre o XML procurando a data exata escolhida
            for entry in root.findall('atom:entry', ns):
                props = entry.find('atom:content/m:properties', ns)
                
                if props is not None:
                    data_xml = props.find('d:NEW_DATE', ns).text[:10]
                    
                    if data_xml == data_str_busca:
                        encontrou_data = True
                        
                        # Coleta todos os prazos (maturities) para a data encontrada
                        for tag, info in TREASURY_MATURITIES.items():
                            taxa_node = props.find(f'd:{tag}', ns)
                            
                            # Nem todas as datas históricas possuem todos os prazos (ex: 2 meses foi introduzido depois)
                            if taxa_node is not None and taxa_node.text is not None:
                                resultados.append({
                                    "VENCIMENTO": info["nome"],
                                    "TAXA_YIELD (%)": float(taxa_node.text),
                                    "ORDEM": info["ordem"], # Usado apenas para ordenar internamente
                                    "FONTE_AUDITORIA": url_xml
                                })
                        break # Como achou a data, não precisa continuar varrendo o XML
                        
        except Exception as e:
            st.error(f"Erro ao conectar com a base do Tesouro: {e}")

    if encontrou_data and resultados:
        df_final = pd.DataFrame(resultados)
        
        # Ordena do prazo mais curto (1 Mês) para o mais longo (30 Anos)
        df_final = df_final.sort_values(by="ORDEM").reset_index(drop=True)
        
        # Remove a coluna de ordenação antes de exibir
        df_final = df_final.drop(columns=["ORDEM"])
        
        # Adiciona a data no formato BR como primeira coluna
        df_final.insert(0, "DATA", data_ref.strftime("%d/%m/%Y"))
        
        st.success("Curva de Juros extraída com sucesso!")
        
        st.dataframe(
            df_final, 
            use_container_width=True,
            column_config={
                "FONTE_AUDITORIA": st.column_config.LinkColumn(
                    "Link para Validação",
                    help="Dados extraídos diretamente do XML da curva de juros do Tesouro Americano.",
                    display_text="Ver XML Fonte"
                )
            }
        )
        
        out = BytesIO()
        with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, index=False)
        
        st.download_button(
            label="📥 Baixar Curva (Excel)", 
            data=out.getvalue(), 
            file_name=f"curva_treasury_{data_ref.strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning(f"Nenhum dado encontrado para a data {data_ref.strftime('%d/%m/%Y')}. Verifique se a data escolhida cai em um fim de semana ou feriado americano.")
