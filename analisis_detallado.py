import streamlit as st
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Análisis Detallado", page_icon="🔬", layout="wide")

def clasificar_registro_analisis(row):
    orden_compra = str(row['orden_compra']).strip().lower()
    if orden_compra in ('', 'nan', 'none', 'xx') or (orden_compra.startswith('2332-') and '-xx-' in orden_compra):
        return 'EN PROCESO'
    return 'PROCESADO'

def analizar_clasificacion(df_experto, df_cancelados):
    ids_cancelados = set(df_cancelados.iloc[:, 0].astype(str))
    df_filtrado = df_experto[~df_experto['numero_req'].astype(str).isin(ids_cancelados)].copy()
    
    st.write(f"📊 **Registros después de filtrar cancelados**: {len(df_filtrado)}")
    
    df_filtrado['clasificacion'] = df_filtrado.apply(clasificar_registro_analisis, axis=1)
    return df_filtrado

def main():
    st.title("🔬 Análisis Detallado de Clasificación")
    st.sidebar.header("Cargar Archivos para Análisis")
    st.sidebar.info("Esta herramienta asume que el archivo principal ya tiene las columnas renombradas a los nombres estándar del sistema (ej: 'numero_req', 'orden_compra').")
    
    archivo_experto = st.sidebar.file_uploader("Informe Principal (con columnas estándar)", type=["csv"], key="analisis_experto")
    archivo_cancelados = st.sidebar.file_uploader("Informe Cancelados", type=["csv"], key="analisis_cancelados")

    if st.sidebar.button("🔬 Ejecutar Análisis Detallado", type="primary"):
        if archivo_experto and archivo_cancelados:
            df_experto = pd.read_csv(archivo_experto)
            df_cancelados = pd.read_csv(archivo_cancelados)
            df_resultados = analizar_clasificacion(df_experto, df_cancelados)
            st.session_state.df_analisis_resultados = df_resultados
        else:
            st.sidebar.error("Sube ambos archivos.")

    if 'df_analisis_resultados' in st.session_state:
        df_resultados = st.session_state.df_analisis_resultados
        st.header("Resultados del Análisis")
        st.dataframe(df_resultados, use_container_width=True)

if __name__ == "__main__":
    main()