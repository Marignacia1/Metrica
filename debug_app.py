import streamlit as st
import pandas as pd

st.set_page_config(page_title="Debug - Sistema KPI Compras", page_icon="🔍", layout="wide")

def clasificar_registro_debug(row):
    orden_compra = str(row['orden_compra']).strip().lower()
    if orden_compra in ('', 'nan', 'none', 'xx') or (orden_compra.startswith('2332-') and '-xx-' in orden_compra):
        return 'EN PROCESO'
    return 'PROCESADO'

def procesar_datos_debug(df_experto, df_cancelados):
    st.info(f"**PASO 1**: Total registros brutos: {len(df_experto)}")
    
    # Asume que el df_experto ya viene con columnas estandarizadas
    ids_cancelados = set(df_cancelados.iloc[:, 0].astype(str))
    df_filtrado = df_experto[~df_experto['numero_req'].astype(str).isin(ids_cancelados)].copy()
    
    st.info(f"**PASO 2**: Registros tras excluir cancelados: {len(df_filtrado)}.")
    
    df_filtrado['clasificacion'] = df_filtrado.apply(clasificar_registro_debug, axis=1)

    procesados_df = df_filtrado[df_filtrado['clasificacion'] == 'PROCESADO']
    en_proceso_df = df_filtrado[df_filtrado['clasificacion'] == 'EN PROCESO']
    
    st.success("**PASO 3**: Clasificación completada.")
    return {"total_procesados": len(procesados_df), "total_en_proceso": len(en_proceso_df)}

def main():
    st.title("🔍 Debug y Verificación de Lógica")
    st.sidebar.header("Cargar Archivos")
    st.sidebar.info("Esta herramienta asume que el archivo principal ya tiene las columnas renombradas a los nombres estándar del sistema (ej: 'numero_req', 'orden_compra').")
    
    archivo_experto = st.sidebar.file_uploader("Informe Principal (con columnas estándar)", type=["csv"], key="debug_experto")
    archivo_cancelados = st.sidebar.file_uploader("Informe Cancelados", type=["csv"], key="debug_cancelados")

    if st.sidebar.button("🔬 Analizar Lógica", type="primary"):
        if archivo_experto and archivo_cancelados:
            df_experto = pd.read_csv(archivo_experto)
            df_cancelados = pd.read_csv(archivo_cancelados)
            resultado = procesar_datos_debug(df_experto, df_cancelados)
            
            st.header("Resultados del Análisis")
            st.metric("✅ Procesados", resultado['total_procesados'])
            st.metric("⏳ En Proceso", resultado['total_en_proceso'])
        else:
            st.sidebar.error("Sube ambos archivos.")

if __name__ == "__main__":
    main()