import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from datetime import datetime
import os
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="Sistema KPI Compras",
    page_icon="📊",
    layout="wide"
)

# --- ESTILO CSS PROFESIONAL PARA DASHBOARD ---
st.markdown("""
<style>
    /* Importar fuente Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Variables CSS para el tema celeste/azul/blanco profesional */
    :root {
        --primary-blue: #1E40AF;        /* Azul corporativo principal */
        --secondary-blue: #3B82F6;      /* Azul vibrante secundario */
        --light-blue: #60A5FA;          /* Azul claro para acentos */
        --pale-blue: #EFF6FF;           /* Celeste muy pálido para fondos */
        --sky-blue: #0EA5E9;            /* Celeste intenso para highlights */
        --bg-white: #FFFFFF;            /* Blanco puro */
        --bg-light: #F9FAFB;            /* Gris muy claro para el fondo */
        --bg-card: #FFFFFF;             /* Fondo blanco para tarjetas */
        --bg-card-hover: #F3F4F6;       /* Hover sutil para tarjetas */
        --text-primary: #1F2937;        /* Texto principal oscuro */
        --text-secondary: #6B7280;      /* Texto secundario */
        --text-muted: #9CA3AF;          /* Texto atenuado */
        --border-light: #E5E7EB;        /* Bordes ligeros */
        --border-medium: #D1D5DB;       /* Bordes medios */
        --success-color: #10B981;
        --warning-color: #F59E0B;
        --error-color: #EF4444;
        --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
        --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        --shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
    }

    /* Ocultar elementos de Streamlit pero mantener el botón de sidebar */
    footer {visibility: hidden;}
    #MainMenu {visibility: hidden;}
    .stDeployButton {display: none;}

    /* Mantener visible el toolbar con el botón del sidebar */
    [data-testid="stHeader"] {
        background: transparent !important;
    }

    /* Asegurar que el botón del sidebar sea visible */
    [data-testid="collapsedControl"] {
        display: block !important;
        visibility: visible !important;
        background: var(--primary-blue) !important;
        color: white !important;
        border-radius: 8px !important;
        padding: 0.5rem !important;
        transition: all 0.3s ease !important;
        margin: 0.5rem !important;
    }

    [data-testid="collapsedControl"]:hover {
        background: var(--secondary-blue) !important;
        transform: scale(1.05) !important;
    }

    /* Estilo para el botón cuando el sidebar está abierto */
    button[kind="header"] {
        color: var(--primary-blue) !important;
    }

    /* Fondo global limpio y profesional */
    .stApp {
        background: var(--bg-light);
        font-family: 'Inter', sans-serif;
        min-height: 100vh;
    }

    /* Contenedor principal con mejor espaciado */
    .block-container {
        padding: 2rem 1.5rem;
        max-width: 1400px;
        margin: 0 auto;
    }

    /* Grid container para dashboard */
    .dashboard-grid {
        display: grid;
        gap: 1.5rem;
        margin-bottom: 2rem;
    }

    /* Títulos profesionales */
    h1 {
        color: var(--text-primary) !important;
        font-weight: 700 !important;
        font-size: 2.25rem !important;
        margin-bottom: 0.5rem !important;
        letter-spacing: -0.025em !important;
    }

    h2 {
        color: var(--text-primary) !important;
        font-weight: 600 !important;
        font-size: 1.5rem !important;
        margin-bottom: 1rem !important;
        padding-bottom: 0.5rem !important;
        border-bottom: 2px solid var(--border-light) !important;
    }

    h3 {
        color: var(--text-secondary) !important;
        font-weight: 600 !important;
        font-size: 1.125rem !important;
        margin-bottom: 0.75rem !important;
    }

    h4 {
        color: var(--text-secondary) !important;
        font-weight: 500 !important;
        font-size: 1rem !important;
        margin-bottom: 0.5rem !important;
    }

    /* Tarjetas profesionales con diseño limpio */
    .stContainer > div {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
        border-radius: 12px !important;
        padding: 1.5rem !important;
        box-shadow: var(--shadow-sm) !important;
        transition: all 0.2s ease !important;
    }

    .stContainer > div:hover {
        background: var(--bg-card) !important;
        box-shadow: var(--shadow-md) !important;
        border-color: var(--border-medium) !important;
    }

    /* KPI Cards Profesionales */
    .kpi-card-professional {
        background: var(--bg-card);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: var(--shadow-sm);
        transition: all 0.2s ease;
        position: relative;
        overflow: hidden;
    }

    .kpi-card-professional:hover {
        box-shadow: var(--shadow-md);
        transform: translateY(-2px);
    }

    .kpi-card-professional::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, var(--primary-blue), var(--secondary-blue));
    }

    .kpi-icon {
        width: 48px;
        height: 48px;
        background: var(--secondary-blue);
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-bottom: 1rem;
        font-size: 1.25rem;
        color: white;
    }

    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--primary-blue);
        margin-bottom: 0.25rem;
        line-height: 1;
    }

    .kpi-label {
        font-size: 0.875rem;
        font-weight: 500;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.25rem;
    }

    .kpi-trend {
        font-size: 0.75rem;
        color: var(--text-muted);
        font-weight: 400;
    }

    /* Métricas modernas */
    .metric-container {
        background: linear-gradient(135deg, var(--bg-card), var(--bg-card-hover)) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 10px !important;
        padding: 1rem !important;
        text-align: center !important;
        box-shadow: var(--shadow-md) !important;
        transition: all 0.3s ease !important;
        position: relative !important;
        overflow: hidden !important;
        margin-bottom: 0.5rem !important;
    }

    .metric-container::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--primary-blue), var(--secondary-blue), var(--primary-blue));
    }

    .metric-container:hover {
        transform: translateY(-5px) !important;
        box-shadow: var(--shadow-xl) !important;
        border-color: var(--primary-blue) !important;
    }

    /* Texto de métricas */
    .metric-container .metric-value {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
        color: var(--text-primary) !important;
        margin-bottom: 0.3rem !important;
    }

    .metric-container .metric-label {
        font-size: 0.8rem !important;
        color: var(--text-secondary) !important;
        font-weight: 500 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
    }

    /* Sidebar moderna */
    .css-1d391kg {
        background: linear-gradient(180deg, var(--dark-blue), var(--primary-blue)) !important;
        border-right: 2px solid var(--secondary-blue) !important;
    }

    .css-1d391kg .stRadio > label {
        color: var(--text-primary) !important;
        font-weight: 500 !important;
        padding: 0.8rem 1rem !important;
        border-radius: 8px !important;
        margin: 0.3rem 0 !important;
        transition: all 0.3s ease !important;
    }

    .css-1d391kg .stRadio > label:hover {
        background: rgba(255, 255, 255, 0.1) !important;
        transform: translateX(5px) !important;
    }

    /* Botones elegantes */
    .stButton > button {
        background: linear-gradient(135deg, var(--primary-blue), var(--secondary-blue)) !important;
        color: var(--text-primary) !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.8rem 2rem !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        box-shadow: var(--shadow-md) !important;
        transition: all 0.3s ease !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, var(--secondary-blue), var(--light-blue)) !important;
        transform: translateY(-3px) !important;
        box-shadow: var(--shadow-xl) !important;
    }

    /* Dataframes modernos */
    .stDataFrame {
        background: var(--bg-card) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
        box-shadow: var(--shadow-md) !important;
        border: 1px solid var(--border-color) !important;
    }

    /* File uploader */
    .stFileUploader > div {
        background: var(--bg-card) !important;
        border: 2px dashed var(--primary-blue) !important;
        border-radius: 12px !important;
        padding: 2rem !important;
        text-align: center !important;
        transition: all 0.3s ease !important;
    }

    .stFileUploader > div:hover {
        border-color: var(--secondary-blue) !important;
        background: var(--bg-card-hover) !important;
    }

    /* Divisores elegantes */
    .stDivider {
        border-top: 2px solid var(--primary-blue) !important;
        margin: 2rem 0 !important;
        opacity: 0.3 !important;
    }

    /* Selectbox y inputs */
    .stSelectbox > div > div {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
        color: var(--text-primary) !important;
    }

    /* Expander moderno */
    .streamlit-expanderHeader {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
        color: var(--text-primary) !important;
        font-weight: 500 !important;
    }

    /* Alertas y notificaciones */
    .stAlert {
        border-radius: 12px !important;
        border: none !important;
        box-shadow: var(--shadow-md) !important;
    }

    .stSuccess {
        background: rgba(16, 185, 129, 0.1) !important;
        color: var(--success-color) !important;
        border-left: 4px solid var(--success-color) !important;
    }

    .stWarning {
        background: rgba(245, 158, 11, 0.1) !important;
        color: var(--warning-color) !important;
        border-left: 4px solid var(--warning-color) !important;
    }

    .stError {
        background: rgba(239, 68, 68, 0.1) !important;
        color: var(--error-color) !important;
        border-left: 4px solid var(--error-color) !important;
    }

    .stInfo {
        background: rgba(30, 64, 175, 0.1) !important;
        color: var(--primary-blue) !important;
        border-left: 4px solid var(--primary-blue) !important;
    }

    /* Loading spinner */
    .stSpinner {
        border-color: var(--primary-blue) !important;
    }

    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, var(--primary-blue), var(--secondary-blue)) !important;
    }

    /* Dashboard moderno y limpio */
    .dashboard-container {
        max-width: 1200px;
        margin: 0 auto;
        padding: 2rem 1rem;
    }

    /* Título principal */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--primary-blue);
        margin-bottom: 2rem;
        text-align: left;
    }

    /* Tarjetas de KPI modernas */
    .kpi-container {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 1.5rem;
        margin-bottom: 2rem;
    }

    .kpi-card {
        background: var(--bg-card);
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: var(--shadow-card);
        border: 1px solid var(--border-color);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }

    .kpi-card:hover {
        transform: translateY(-5px);
        box-shadow: var(--shadow-hover);
    }

    .kpi-icon {
        width: 48px;
        height: 48px;
        margin: 0 auto 1rem auto;
        background: var(--light-blue);
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.5rem;
        color: white;
    }

    .kpi-title {
        font-size: 0.9rem;
        font-weight: 500;
        color: var(--text-secondary);
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--primary-blue);
    }

    /* Layout de dos columnas */
    .two-column-layout {
        display: grid;
        grid-template-columns: 1fr 2fr;
        gap: 2rem;
        margin-top: 2rem;
    }

    /* Columna izquierda - Texto */
    .text-column {
        display: flex;
        flex-direction: column;
        gap: 1.5rem;
    }

    .text-block {
        background: var(--bg-card);
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: var(--shadow-card);
        border: 1px solid var(--border-color);
    }

    .text-block-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: var(--primary-blue);
        margin-bottom: 1rem;
    }

    .text-block-content {
        font-size: 0.95rem;
        line-height: 1.6;
        color: var(--text-secondary);
    }

    /* Columna derecha - Gráficos */
    .charts-column {
        display: grid;
        grid-template-rows: 1fr 1fr;
        gap: 1.5rem;
    }

    .chart-card {
        background: var(--bg-card);
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: var(--shadow-card);
        border: 1px solid var(--border-color);
    }

    .chart-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--primary-blue);
        margin-bottom: 1rem;
        text-align: center;
    }

    .chart-bottom-row {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1.5rem;
    }

    /* Efectos decorativos */
    .decoration-sparkle {
        position: absolute;
        width: 6px;
        height: 6px;
        background: var(--pale-blue);
        border-radius: 50%;
        opacity: 0.6;
    }
</style>
""", unsafe_allow_html=True)


# --- CLASE COMPRASPROCESSOR (LÓGICA ORIGINAL INTACTA) ---
class ComprasProcessor:
    def __init__(self):
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect('compras.db')

        # Crear tabla principal
        conn.execute('''
            CREATE TABLE IF NOT EXISTS sesiones (
                id INTEGER PRIMARY KEY, fecha DATETIME, total_brutos INTEGER,
                req_procesados INTEGER, req_en_proceso INTEGER, eficiencia REAL
            )''')

        # Verificar si existe la columna req_cancelados, si no, agregarla
        cursor = conn.execute("PRAGMA table_info(sesiones)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'req_cancelados' not in columns:
            conn.execute('ALTER TABLE sesiones ADD COLUMN req_cancelados INTEGER DEFAULT 0')
            st.info("✅ Base de datos actualizada: columna req_cancelados agregada")

        conn.execute('''
            CREATE TABLE IF NOT EXISTS procesados (
                id INTEGER PRIMARY KEY, sesion_id INTEGER, numero_req TEXT, titulo TEXT,
                tipo_compra TEXT, unidad TEXT, comprador TEXT, orden_compra TEXT,
                FOREIGN KEY (sesion_id) REFERENCES sesiones (id)
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS en_proceso (
                id INTEGER PRIMARY KEY, sesion_id INTEGER, numero_req TEXT, titulo TEXT,
                tipo_compra TEXT, unidad TEXT, comprador TEXT, estado TEXT,
                FOREIGN KEY (sesion_id) REFERENCES sesiones (id)
            )''')

        # Tablas para datos de licitaciones
        conn.execute('''
            CREATE TABLE IF NOT EXISTS licitaciones_mercado_publico (
                id INTEGER PRIMARY KEY,
                sesion_id INTEGER,
                nro_adquisicion TEXT UNIQUE,
                estado_licitacion TEXT,
                monto_estimado REAL,
                fecha_carga DATETIME,
                FOREIGN KEY (sesion_id) REFERENCES sesiones (id)
            )''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS seguimiento_licitaciones (
                id INTEGER PRIMARY KEY,
                sesion_id INTEGER,
                id_mercado_publico TEXT,
                lineas_leidas INTEGER,
                cantidad_oferentes INTEGER,
                fecha_carga DATETIME,
                FOREIGN KEY (sesion_id) REFERENCES sesiones (id)
            )''')

        conn.commit()
        conn.close()
    
    def _es_procesado(self, tipo_compra, estado_oc):
        estado_clean = str(estado_oc).strip()
        estado_upper = estado_clean.upper()
        estado_lower = estado_clean.lower()
        if estado_lower == 'nan' or estado_clean == '': return False
        if tipo_compra == "Compra Ágil": return "AG" in estado_upper
        elif tipo_compra == "Convenio Marco": return "CM" in estado_upper or "AG" in estado_upper
        elif tipo_compra == "Trato Directo": return "TD" in estado_upper or "LEY" in estado_upper
        elif tipo_compra in ["Licitación", "Convenio de Suministros Vigentes"]:
            return (estado_clean != "" and estado_lower != "xx" and not estado_lower.startswith("2332-xx-"))
        return False

    def _es_en_proceso(self, tipo_compra, estado_oc, numero_req, dict_precompra, es_procesado):
        if es_procesado: return False
        estado_clean = str(estado_oc).strip()
        is_blank_or_nan = (estado_clean == "" or estado_clean.lower() == "nan")
        if tipo_compra == "Compra Ágil":
            if is_blank_or_nan: return True
            if "COT" in estado_clean.upper() and numero_req not in dict_precompra: return True
        elif tipo_compra in ["Convenio Marco", "Trato Directo"]: return is_blank_or_nan or estado_clean.lower().startswith("2332-xx-")
        elif tipo_compra in ["Licitación", "Convenio de Suministros Vigentes"]: return is_blank_or_nan or estado_clean.lower() == "xx" or estado_clean.lower().startswith("2332-xx-")
        return False

    def _detectar_columnas(self, df):
        """Detecta automáticamente las columnas necesarias basándose en palabras clave."""
        columnas = df.columns.str.lower().str.strip()

        # Patrones para detectar cada tipo de columna
        patrones_numero = ['número', 'numero', 'req', 'requerimiento', 'solicitud', 'id']
        patrones_tipo_compra = ['tipo', 'compra', 'modalidad', 'procedimiento']
        patrones_orden_compra = ['orden', 'oc', 'compra', 'estado']

        col_numero = None
        col_tipo_compra = None
        col_orden_compra = None

        # Buscar columna de número
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(patron in col_lower for patron in patrones_numero):
                col_numero = col
                break

        # Buscar columna de tipo de compra
        for col in df.columns:
            col_lower = col.lower().strip()
            if 'tipo' in col_lower and 'compra' in col_lower:
                col_tipo_compra = col
                break
        if not col_tipo_compra:
            for col in df.columns:
                col_lower = col.lower().strip()
                if any(patron in col_lower for patron in patrones_tipo_compra):
                    col_tipo_compra = col
                    break

        # Buscar columna de orden de compra
        for col in df.columns:
            col_lower = col.lower().strip()
            if 'orden' in col_lower and 'compra' in col_lower:
                col_orden_compra = col
                break
        if not col_orden_compra:
            for col in df.columns:
                col_lower = col.lower().strip()
                if any(patron in col_lower for patron in patrones_orden_compra):
                    col_orden_compra = col
                    break

        return col_numero, col_tipo_compra, col_orden_compra

    def _normalizar_id(self, valor):
        """Normaliza un ID removiendo decimales innecesarios y espacios."""
        try:
            # Convertir a string y limpiar espacios
            valor_str = str(valor).strip()

            # Si es nan o vacío, retornar vacío
            if valor_str.lower() == 'nan' or valor_str == '':
                return ''

            # Intentar convertir a float primero para manejar números con decimales
            try:
                valor_float = float(valor_str)
                # Si es un número entero (ej: 4957.0), convertir a entero
                if valor_float.is_integer():
                    return str(int(valor_float))
                else:
                    return str(valor_float)
            except (ValueError, OverflowError):
                # Si no se puede convertir a número, retornar como string limpio
                return valor_str
        except:
            return str(valor)

    def procesar_datos(self, df_experto, df_cancelados, df_precompra=None):
        # Detectar columnas automáticamente
        COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC = self._detectar_columnas(df_experto)

        # Validar que se encontraron las columnas necesarias
        if not COL_NUM_REQ:
            st.error("❌ No se pudo detectar la columna de número de requerimiento. Columnas disponibles: " + ", ".join(df_experto.columns))
            return False
        if not COL_TIPO_COMPRA:
            st.error("❌ No se pudo detectar la columna de tipo de compra. Columnas disponibles: " + ", ".join(df_experto.columns))
            return False
        if not COL_ESTADO_OC:
            st.error("❌ No se pudo detectar la columna de orden de compra. Columnas disponibles: " + ", ".join(df_experto.columns))
            return False

        st.info(f"✅ Columnas detectadas: Número='{COL_NUM_REQ}', Tipo='{COL_TIPO_COMPRA}', Estado OC='{COL_ESTADO_OC}'")

        # Normalizar IDs de cancelados (remover .0 si es entero)
        ids_cancelados = set(df_cancelados.iloc[:, 0].apply(self._normalizar_id))
        # Remover valores vacíos del set
        ids_cancelados.discard('')

        # Normalizar IDs de experto para la comparación
        df_experto[COL_NUM_REQ + '_normalizado'] = df_experto[COL_NUM_REQ].apply(self._normalizar_id)

        # Filtrar usando los IDs normalizados
        df_filtrado = df_experto[~df_experto[COL_NUM_REQ + '_normalizado'].isin(ids_cancelados)].copy()

        # Normalizar precompra si existe
        dict_precompra = set(df_precompra.iloc[:, 0].apply(self._normalizar_id)) if df_precompra is not None and not df_precompra.empty else set()
        dict_precompra.discard('')

        # Debug: Mostrar conteos para verificar
        cancelados_filtrados = len(df_experto) - len(df_filtrado)
        st.info(f"📊 Debug: Total brutos: {len(df_experto)}, Cancelados en lista: {len(ids_cancelados)}, Cancelados encontrados y filtrados: {cancelados_filtrados}, Neto después filtro: {len(df_filtrado)}")

        # Debug adicional: mostrar ejemplos de IDs
        if len(ids_cancelados) > 0:
            ejemplos_cancelados = list(ids_cancelados)[:5]
            ejemplos_experto = df_experto[COL_NUM_REQ + '_normalizado'].head(5).tolist()
            st.info(f"🔍 Primeros 5 IDs cancelados normalizados: {ejemplos_cancelados}")
            st.info(f"🔍 Primeros 5 IDs experto normalizados: {ejemplos_experto}")

        # Contar tipos de compra en el filtrado
        if COL_TIPO_COMPRA in df_filtrado.columns:
            tipos_count = df_filtrado[COL_TIPO_COMPRA].value_counts()
            st.info(f"🎯 Debug - Distribución después de filtrar cancelados: {dict(tipos_count)}")

        procesados, en_proceso = [], []
        for _, row in df_filtrado.iterrows():
            numero_req, tipo_compra, estado_oc_raw = str(row[COL_NUM_REQ]), str(row[COL_TIPO_COMPRA]), row[COL_ESTADO_OC]
            estado_oc = str(estado_oc_raw) if pd.notna(estado_oc_raw) else ""
            if tipo_compra == "Convenio de Insumos": tipo_compra = "Convenio de Suministros Vigentes"
            elif tipo_compra == "Trato Directo con Cotizaciones": tipo_compra = "Trato Directo"
            es_proc = self._es_procesado(tipo_compra, estado_oc)
            es_en_proc = self._es_en_proceso(tipo_compra, estado_oc, numero_req, dict_precompra, es_proc)
            if es_proc: procesados.append(row)
            elif es_en_proc: en_proceso.append(row)

        total_brutos, total_procesados, total_en_proceso = len(df_experto), len(procesados), len(en_proceso)
        total_cancelados = cancelados_filtrados  # Solo los cancelados que coinciden en ambas tablas
        total_neto = len(df_filtrado)

        # Debug: Verificar que los totales sean correctos
        st.info(f"🔍 Debug Final: Procesados: {total_procesados}, En Proceso: {total_en_proceso}, Total Neto: {total_neto}")
        eficiencia = (total_procesados / total_neto * 100) if total_neto > 0 else 0
        df_procesados = pd.DataFrame(procesados) if procesados else pd.DataFrame()
        df_en_proceso = pd.DataFrame(en_proceso) if en_proceso else pd.DataFrame()
        sesion_id = self._guardar_resultados(total_brutos, total_procesados, total_en_proceso, eficiencia, total_cancelados, df_procesados, df_en_proceso, COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC)
        return sesion_id is not None

    def _detectar_columnas_adicionales(self, df, col_numero, col_tipo_compra, col_estado_oc):
        """Detecta las columnas adicionales necesarias para guardar los datos."""
        patrones_titulo = ['titulo', 'título', 'solicitud', 'descripcion', 'descripción', 'nombre']
        patrones_unidad = ['unidad', 'solicitante', 'area', 'área', 'departamento']
        patrones_comprador = ['comprador', 'asignado', 'responsable', 'buyer']

        col_titulo = None
        col_unidad = None
        col_comprador = None

        # Buscar columna de título
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(patron in col_lower for patron in patrones_titulo):
                col_titulo = col
                break

        # Buscar columna de unidad
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(patron in col_lower for patron in patrones_unidad):
                col_unidad = col
                break

        # Buscar columna de comprador
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(patron in col_lower for patron in patrones_comprador):
                col_comprador = col
                break

        return col_titulo, col_unidad, col_comprador

    def _guardar_resultados(self, total_brutos, total_procesados, total_en_proceso, eficiencia, total_cancelados, df_procesados, df_en_proceso, col_numero, col_tipo_compra, col_estado_oc):
        conn = sqlite3.connect('compras.db')
        cursor = conn.execute('INSERT INTO sesiones (fecha, total_brutos, req_procesados, req_en_proceso, eficiencia, req_cancelados) VALUES (?, ?, ?, ?, ?, ?)',
                                (datetime.now(), total_brutos, total_procesados, total_en_proceso, eficiencia, total_cancelados))
        sesion_id = cursor.lastrowid

        # Detectar columnas adicionales
        if not df_procesados.empty:
            col_titulo, col_unidad, col_comprador = self._detectar_columnas_adicionales(df_procesados, col_numero, col_tipo_compra, col_estado_oc)

            # Crear mapeo dinámico de columnas
            column_map_proc = {}
            if col_numero: column_map_proc[col_numero] = 'numero_req'
            if col_titulo: column_map_proc[col_titulo] = 'titulo'
            if col_tipo_compra: column_map_proc[col_tipo_compra] = 'tipo_compra'
            if col_unidad: column_map_proc[col_unidad] = 'unidad'
            if col_comprador: column_map_proc[col_comprador] = 'comprador'
            if col_estado_oc: column_map_proc[col_estado_oc] = 'orden_compra'

            # Filtrar solo las columnas que existen
            columnas_existentes = [col for col in column_map_proc.keys() if col in df_procesados.columns]
            if columnas_existentes:
                df_proc_save = df_procesados[columnas_existentes].rename(columns={col: column_map_proc[col] for col in columnas_existentes})
                df_proc_save['sesion_id'] = sesion_id
                df_proc_save.to_sql('procesados', conn, if_exists='append', index=False)

        if not df_en_proceso.empty:
            col_titulo, col_unidad, col_comprador = self._detectar_columnas_adicionales(df_en_proceso, col_numero, col_tipo_compra, col_estado_oc)

            # Crear mapeo dinámico de columnas para en proceso
            column_map_enproc = {}
            if col_numero: column_map_enproc[col_numero] = 'numero_req'
            if col_titulo: column_map_enproc[col_titulo] = 'titulo'
            if col_tipo_compra: column_map_enproc[col_tipo_compra] = 'tipo_compra'
            if col_unidad: column_map_enproc[col_unidad] = 'unidad'
            if col_comprador: column_map_enproc[col_comprador] = 'comprador'
            if col_estado_oc: column_map_enproc[col_estado_oc] = 'estado'

            # Filtrar solo las columnas que existen
            columnas_existentes = [col for col in column_map_enproc.keys() if col in df_en_proceso.columns]
            if columnas_existentes:
                df_enproc_save = df_en_proceso[columnas_existentes].rename(columns={col: column_map_enproc[col] for col in columnas_existentes})
                df_enproc_save['sesion_id'] = sesion_id
                df_enproc_save.to_sql('en_proceso', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()
        return sesion_id
    
    def obtener_estadisticas(self):
        try:
            with sqlite3.connect('compras.db') as conn:
                if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sesiones'").fetchone(): return None
                ultima_sesion = conn.execute('SELECT * FROM sesiones ORDER BY fecha DESC LIMIT 1').fetchone()
                if not ultima_sesion: return None
                tendencia = pd.read_sql_query('SELECT fecha, eficiencia, req_procesados, req_en_proceso FROM sesiones ORDER BY fecha DESC LIMIT 7', conn)
                column_names = ['id', 'fecha', 'total_brutos', 'procesados', 'en_proceso', 'eficiencia', 'cancelados']
                return {'ultima_sesion': dict(zip(column_names, ultima_sesion)), 'tendencia': tendencia}
        except sqlite3.Error as e:
            st.error(f"Error en la base de datos: {e}")
            return None

# --- FUNCIONES DE ANÁLISIS ---

def mostrar_analisis_tipos_compra():
    """Muestra tabla de distribución y gráfico circular de procesados por tipo de compra"""
    try:
        with sqlite3.connect('compras.db') as conn:
            # Obtener la última sesión
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
            if last_sesion_id_df.empty:
                st.warning("No hay datos de sesiones disponibles.")
                return

            last_sesion_id = int(last_sesion_id_df.iloc[0, 0])

            # Obtener datos procesados y en proceso de la última sesión
            query_procesados = "SELECT * FROM procesados WHERE sesion_id = ?"
            query_en_proceso = "SELECT * FROM en_proceso WHERE sesion_id = ?"

            df_procesados = pd.read_sql_query(query_procesados, conn, params=(last_sesion_id,))
            df_en_proceso = pd.read_sql_query(query_en_proceso, conn, params=(last_sesion_id,))

            # Aplicar filtro por rango de requerimientos si está activo
            if st.session_state.get('filtro_activo', False):
                req_desde = st.session_state.get('filtro_req_desde')
                req_hasta = st.session_state.get('filtro_req_hasta')

                if not df_procesados.empty and 'numero_req' in df_procesados.columns:
                    df_procesados['numero_req_num'] = pd.to_numeric(df_procesados['numero_req'], errors='coerce')
                    df_procesados = df_procesados[
                        (df_procesados['numero_req_num'] >= req_desde) &
                        (df_procesados['numero_req_num'] <= req_hasta)
                    ].copy()

                if not df_en_proceso.empty and 'numero_req' in df_en_proceso.columns:
                    df_en_proceso['numero_req_num'] = pd.to_numeric(df_en_proceso['numero_req'], errors='coerce')
                    df_en_proceso = df_en_proceso[
                        (df_en_proceso['numero_req_num'] >= req_desde) &
                        (df_en_proceso['numero_req_num'] <= req_hasta)
                    ].copy()

            if df_procesados.empty and df_en_proceso.empty:
                st.info("No hay datos de requerimientos procesados para mostrar.")
                return

            # Crear tabla de distribución
            col_tabla, col_grafico = st.columns(2, gap="large")

            with col_tabla:
                st.markdown("#### 📋 Distribución por Tipo de Compra")
                st.markdown("*Nota: Números netos (cancelados ya excluidos)*")

                # Contar procesados por tipo
                procesados_por_tipo = df_procesados['tipo_compra'].value_counts() if not df_procesados.empty else pd.Series(dtype=int)

                # Para En Proceso, necesitamos separar Compra Ágil regular de COT
                tabla_distribucion = []

                if not df_en_proceso.empty:
                    # Obtener todos los tipos únicos
                    tipos_compra = set(procesados_por_tipo.index.tolist() + df_en_proceso['tipo_compra'].unique().tolist())

                    for tipo in tipos_compra:
                        procesados_count = procesados_por_tipo.get(tipo, 0)

                        if tipo == "Compra Ágil":
                            # Para Compra Ágil, separar en proceso regular y COT en proceso
                            df_ca_en_proceso = df_en_proceso[df_en_proceso['tipo_compra'] == 'Compra Ágil']

                            # Separar por estado (esto replica la lógica de tu VBA)
                            en_proceso_regular = 0  # Campo vacío
                            cot_en_proceso = 0     # COT25

                            for _, row in df_ca_en_proceso.iterrows():
                                estado = str(row.get('estado', '')).strip() if 'estado' in row else ''
                                if estado == '' or estado.lower() == 'nan':
                                    en_proceso_regular += 1
                                elif 'COT25' in estado.upper():
                                    cot_en_proceso += 1
                                else:
                                    en_proceso_regular += 1  # Por defecto

                            total = procesados_count + en_proceso_regular + cot_en_proceso

                            tabla_distribucion.append({
                                'Tipo de Compra': f"{tipo} - Con OC",
                                'Cantidad': procesados_count,
                                'Subtipo': 'Procesados'
                            })

                            if en_proceso_regular > 0:
                                tabla_distribucion.append({
                                    'Tipo de Compra': f"{tipo} - En Proceso",
                                    'Cantidad': en_proceso_regular,
                                    'Subtipo': 'En Proceso'
                                })

                            if cot_en_proceso > 0:
                                tabla_distribucion.append({
                                    'Tipo de Compra': f"{tipo} - COT Proceso",
                                    'Cantidad': cot_en_proceso,
                                    'Subtipo': 'COT En Proceso'
                                })

                            # Fila total
                            tabla_distribucion.append({
                                'Tipo de Compra': f"**{tipo} - TOTAL**",
                                'Cantidad': total,
                                'Subtipo': f'Eficiencia: {(procesados_count/total*100):.1f}%'
                            })

                        else:
                            # Para otros tipos, lógica normal
                            en_proceso_count = len(df_en_proceso[df_en_proceso['tipo_compra'] == tipo])
                            total = procesados_count + en_proceso_count

                            if total > 0:
                                tabla_distribucion.append({
                                    'Tipo de Compra': tipo,
                                    'Cantidad': total,
                                    'Subtipo': f'Procesados: {procesados_count} | En Proceso: {en_proceso_count} | Eficiencia: {(procesados_count/total*100):.1f}%'
                                })
                else:
                    # Si no hay en proceso, mostrar solo procesados
                    for tipo, count in procesados_por_tipo.items():
                        tabla_distribucion.append({
                            'Tipo de Compra': tipo,
                            'Cantidad': count,
                            'Subtipo': 'Solo Procesados'
                        })

                df_tabla = pd.DataFrame(tabla_distribucion)
                st.dataframe(df_tabla, use_container_width=True, hide_index=True)

            with col_grafico:
                st.markdown("#### 🥧 Distribución de Procesados")

                if not df_procesados.empty:
                    # Crear gráfico circular solo de procesados
                    procesados_counts = df_procesados['tipo_compra'].value_counts()

                    # Paleta de colores profesional
                    professional_blue_palette = ['#1E40AF', '#3B82F6', '#60A5FA', '#93C5FD', '#DBEAFE', '#EFF6FF']

                    fig_pie_procesados = px.pie(
                        values=procesados_counts.values,
                        names=procesados_counts.index,
                        hole=0.4,
                        color_discrete_sequence=professional_blue_palette
                    )

                    fig_pie_procesados.update_traces(
                        textposition='inside',
                        textinfo='percent+label',
                        textfont_size=10,
                        textfont_color='white'
                    )

                    fig_pie_procesados.update_layout(
                        template="plotly_white",
                        height=400,
                        showlegend=True,
                        legend=dict(
                            orientation="v",
                            x=1.1,
                            y=0.5,
                            font=dict(color="#1F2937", size=10)
                        ),
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)'
                    )

                    st.plotly_chart(fig_pie_procesados, use_container_width=True)
                else:
                    st.info("No hay requerimientos procesados para mostrar en el gráfico.")

    except Exception as e:
        st.error(f"Error al generar el análisis: {e}")

# --- VISTAS DE PÁGINA ---

def pagina_ordenes_compra(stats):
    st.markdown('<h1 style="text-align: center; margin-bottom: 2rem;">📈 Dashboard de Órdenes de Compra</h1>', unsafe_allow_html=True)
    ultima = stats['ultima_sesion']

    # KPIs principales con diseño profesional limpio
    st.markdown("""
    <div style="text-align: center; margin: 2rem 0;">
        <h2 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
            Dashboard de Control Ejecutivo
        </h2>
        <p style="color: var(--text-secondary); font-size: 1rem; margin: 0;">
            Indicadores clave de rendimiento del proceso de compras
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Calcular las eficiencias solicitadas
    total_ingresados = ultima['total_brutos']
    total_cancelados = ultima.get('cancelados', 0)
    total_procesados = ultima['procesados']
    total_neto = total_ingresados - total_cancelados

    eficiencia_admision = (total_neto / total_ingresados * 100) if total_ingresados > 0 else 0
    eficiencia_procesamiento = (total_procesados / total_neto * 100) if total_neto > 0 else 0

    # KPIs en cuadrícula limpia
    col1, col2, col3, col4, col5 = st.columns(5, gap="medium")

    with col1:
        st.markdown(f"""
        <div class="kpi-card-professional">
            <div class="kpi-icon" style="background: var(--sky-blue);">📥</div>
            <div class="kpi-value">{total_ingresados:,}</div>
            <div class="kpi-label">Total Req Ingresados</div>
            <div class="kpi-trend">Inicial</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="kpi-card-professional">
            <div class="kpi-icon" style="background: var(--error-color);">❌</div>
            <div class="kpi-value">{total_cancelados:,}</div>
            <div class="kpi-label">Total Req Cancelados</div>
            <div class="kpi-trend">Descartados</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="kpi-card-professional">
            <div class="kpi-icon" style="background: var(--success-color);">✅</div>
            <div class="kpi-value">{total_procesados:,}</div>
            <div class="kpi-label">Total Req Procesados</div>
            <div class="kpi-trend">Completados</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="kpi-card-professional">
            <div class="kpi-icon" style="background: var(--secondary-blue);">📊</div>
            <div class="kpi-value">{eficiencia_admision:.1f}%</div>
            <div class="kpi-label">Eficiencia de Admisión</div>
            <div class="kpi-trend">Ingresados vs Cancelados</div>
        </div>
        """, unsafe_allow_html=True)

    with col5:
        st.markdown(f"""
        <div class="kpi-card-professional">
            <div class="kpi-icon">🎯</div>
            <div class="kpi-value">{eficiencia_procesamiento:.1f}%</div>
            <div class="kpi-label">Eficiencia</div>
            <div class="kpi-trend">Procesados vs Neto</div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ===== FILTROS DE ANÁLISIS =====
    st.markdown("""
    <div style="margin: 2rem 0 1rem 0;">
        <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
            🔍 Filtros de Análisis
        </h3>
        <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
            Filtra los datos por rango de números de requerimiento
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Obtener datos de la última sesión para determinar rangos
    try:
        with sqlite3.connect('compras.db') as conn:
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
            if not last_sesion_id_df.empty:
                last_sesion_id = int(last_sesion_id_df.iloc[0, 0])

                # Obtener todos los requerimientos (procesados + en proceso)
                df_todos_procesados = pd.read_sql_query("SELECT * FROM procesados WHERE sesion_id = ?", conn, params=(last_sesion_id,))
                df_todos_en_proceso = pd.read_sql_query("SELECT * FROM en_proceso WHERE sesion_id = ?", conn, params=(last_sesion_id,))

                # Combinar ambos dataframes
                df_todos = pd.concat([df_todos_procesados, df_todos_en_proceso], ignore_index=True)

                if not df_todos.empty and 'numero_req' in df_todos.columns:
                    # Convertir numero_req a numérico para encontrar min y max
                    df_todos['numero_req_num'] = pd.to_numeric(df_todos['numero_req'], errors='coerce')
                    df_todos = df_todos.dropna(subset=['numero_req_num'])

                    if not df_todos.empty:
                        min_req = int(df_todos['numero_req_num'].min())
                        max_req = int(df_todos['numero_req_num'].max())

                        # Crear columnas para los filtros
                        col_filtro1, col_filtro2, col_filtro3 = st.columns([1, 1, 1])

                        with col_filtro1:
                            usar_filtro = st.checkbox("🎯 Activar filtro por rango de requerimientos", value=False)

                        if usar_filtro:
                            with col_filtro2:
                                req_desde = st.number_input(
                                    "Desde Req N°",
                                    min_value=min_req,
                                    max_value=max_req,
                                    value=min_req,
                                    step=1
                                )

                            with col_filtro3:
                                req_hasta = st.number_input(
                                    "Hasta Req N°",
                                    min_value=min_req,
                                    max_value=max_req,
                                    value=max_req,
                                    step=1
                                )

                            # Aplicar filtro
                            if req_desde <= req_hasta:
                                df_filtrado_rango = df_todos[
                                    (df_todos['numero_req_num'] >= req_desde) &
                                    (df_todos['numero_req_num'] <= req_hasta)
                                ]

                                # Actualizar stats con datos filtrados
                                total_filtrado = len(df_filtrado_rango)
                                procesados_filtrado = len(df_filtrado_rango[df_filtrado_rango['sesion_id'].isin(df_todos_procesados['sesion_id'])])

                                st.info(f"📊 Mostrando requerimientos del **{req_desde}** al **{req_hasta}**: **{total_filtrado}** requerimientos encontrados")

                                # Guardar el filtro en session_state para usarlo en otros análisis
                                st.session_state['filtro_req_desde'] = req_desde
                                st.session_state['filtro_req_hasta'] = req_hasta
                                st.session_state['filtro_activo'] = True
                            else:
                                st.error("⚠️ El número 'Desde' debe ser menor o igual que 'Hasta'")
                                st.session_state['filtro_activo'] = False
                        else:
                            st.session_state['filtro_activo'] = False
                            if 'filtro_req_desde' in st.session_state:
                                del st.session_state['filtro_req_desde']
                            if 'filtro_req_hasta' in st.session_state:
                                del st.session_state['filtro_req_hasta']
    except Exception as e:
        st.warning(f"No se pudieron cargar los filtros: {e}")
        st.session_state['filtro_activo'] = False

    st.divider()

    # ===== ANÁLISIS DE DATOS PROCESADOS =====
    # Mostrar análisis después de cargar datos
    if stats:
        st.markdown("""
        <div style="margin: 2rem 0 1.5rem 0;">
            <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                📊 Distribución de Requerimientos por Tipo de Compra
            </h3>
            <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
                Análisis detallado de los datos procesados en la última sesión
            </p>
        </div>
        """, unsafe_allow_html=True)

        mostrar_analisis_tipos_compra()

    st.divider()

    st.markdown("""
    <div style="margin: 3rem 0 1.5rem 0;">
        <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
            📊 Análisis Financiero por Orden de Compra
        </h3>
        <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
            Dashboard financiero completo basado en órdenes de compra procesadas
        </p>
    </div>
    """, unsafe_allow_html=True)

    col_upload1, col_upload2 = st.columns(2, gap="medium")

    with col_upload1:
        file_resultado_oc = st.file_uploader(
            "📊 ListadoResultadoOC (Obligatorio)",
            type=['xlsx', 'csv'],
            key="uploader_general_resultado"
        )

    with col_upload2:
        file_experto_historico_general = st.file_uploader(
            "📁 Experto Histórico (Obligatorio)",
            type=['xlsx', 'csv'],
            help="Puede ser de cualquier mes/año. Se usa para obtener unidades y datos adicionales",
            key="uploader_general_experto_historico"
        )

    if file_resultado_oc and file_experto_historico_general:
        with st.spinner("Generando dashboard financiero..."):
            try:
                df_resultado = leer_archivo(file_resultado_oc)
                df_experto_hist = leer_archivo(file_experto_historico_general)

                if df_resultado is None or df_experto_hist is None:
                    st.warning("Uno o ambos archivos son inválidos o no se pudieron leer."); st.stop()

                # Detectar columna de orden de compra en resultado
                col_oc_resultado = None
                patrones_oc = ['orden', 'oc', 'n°', 'numero', 'número', 'compra']
                for col in df_resultado.columns:
                    col_lower = str(col).lower().strip()
                    if any(patron in col_lower for patron in patrones_oc):
                        col_oc_resultado = col
                        break

                if not col_oc_resultado:
                    st.error(f"❌ No se pudo detectar la columna de orden de compra en ListadoResultadoOC. Columnas disponibles: {', '.join(df_resultado.columns)}")
                    st.stop()

                st.info(f"✅ Columna de OC detectada: '{col_oc_resultado}'")

                # Detectar columnas en experto histórico
                COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC = st.session_state.processor._detectar_columnas(df_experto_hist)

                if not COL_NUM_REQ or not COL_ESTADO_OC:
                    st.error("❌ No se pudieron detectar las columnas necesarias en el archivo Experto")
                    st.stop()

                # Detectar columnas adicionales (unidad, título, comprador)
                col_titulo, col_unidad, col_comprador = st.session_state.processor._detectar_columnas_adicionales(
                    df_experto_hist, COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC
                )

                # Normalizar columnas de OC a texto
                df_resultado[col_oc_resultado] = df_resultado[col_oc_resultado].astype(str).str.strip().str.upper()
                df_experto_hist['oc_normalizada'] = df_experto_hist[COL_ESTADO_OC].astype(str).str.strip().str.upper()

                # Merge: prioridad a resultado (left join desde resultado)
                df_analisis = pd.merge(
                    df_resultado,
                    df_experto_hist,
                    left_on=col_oc_resultado,
                    right_on='oc_normalizada',
                    how='left',
                    suffixes=('', '_experto')
                ).copy()

                # Renombrar columnas para consistencia
                if col_unidad and col_unidad != 'unidad':
                    df_analisis['unidad'] = df_analisis[col_unidad]
                if COL_TIPO_COMPRA and COL_TIPO_COMPRA != 'tipo_compra':
                    df_analisis['tipo_compra'] = df_analisis[COL_TIPO_COMPRA]

                matches = df_analisis[col_unidad].notna().sum() if col_unidad else 0
                st.success(f"✅ Merge completado. {matches} de {len(df_analisis)} OCs con información combinada")

                # Debug: Mostrar OCs sin match
                sin_match = df_analisis[df_analisis[col_unidad].isna()] if col_unidad else df_analisis
                if len(sin_match) > 0:
                    st.warning(f"⚠️ {len(sin_match)} OCs sin match")
                    with st.expander("🔍 Ver OCs sin match"):
                        # Detectar columna de nombre en resultado
                        col_nombre = None
                        for col in df_analisis.columns:
                            if 'nombre' in str(col).lower():
                                col_nombre = col
                                break

                        if col_nombre:
                            debug_cols = [col_oc_resultado, col_nombre, 'Tipo de compra' if 'Tipo de compra' in df_analisis.columns else None]
                            debug_cols = [c for c in debug_cols if c and c in df_analisis.columns]
                            st.dataframe(sin_match[debug_cols].head(15), use_container_width=True)
                        else:
                            st.dataframe(sin_match[[col_oc_resultado]].head(15), use_container_width=True)

                st.session_state.df_oc_analisis = df_analisis
                st.success("¡Análisis financiero completado!")

            except Exception as e:
                st.error(f"Ocurrió un error al procesar los datos: {e}")
                st.exception(e)

    if 'df_oc_analisis' in st.session_state:
        st.divider()
        df_display = st.session_state.df_oc_analisis

        # Normalizar estados: Si tiene "solicitud cancelar" + "aceptado" = Cancelado
        if 'Estado OC' in df_display.columns:
            def normalizar_estado(estado):
                estado_lower = str(estado).lower()
                # Si tiene ambos términos, es cancelado
                if 'solicitud' in estado_lower and 'cancelar' in estado_lower:
                    return 'Cancelada'
                if 'no aceptada' in estado_lower or 'cancelada' in estado_lower:
                    return 'Cancelada'
                return estado

            df_display['Estado OC'] = df_display['Estado OC'].apply(normalizar_estado)

        # ===== TABLA DE ESTADOS =====
        st.markdown("""
        <div style="margin: 2rem 0 1.5rem 0;">
            <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                📊 Distribución de OCs por Estado
            </h3>
        </div>
        """, unsafe_allow_html=True)

        if 'Estado OC' in df_display.columns and 'Total OC' in df_display.columns:
            # Agrupar por estado
            estados_df = df_display.groupby('Estado OC').agg({
                'Estado OC': 'count',
                'Total OC': 'sum'
            }).rename(columns={'Estado OC': 'Cantidad'})
            estados_df['Monto Total'] = estados_df['Total OC'].apply(lambda x: f"${x:,.0f}")
            estados_df = estados_df[['Cantidad', 'Monto Total']].sort_values('Cantidad', ascending=False)

            col_tabla1, col_tabla2 = st.columns([1, 2])

            with col_tabla1:
                st.dataframe(estados_df, use_container_width=True)

            with col_tabla2:
                # Gráfico de estados
                import plotly.express as px
                fig_estados = px.bar(estados_df.reset_index(), x='Cantidad', y='Estado OC',
                                    orientation='h', text='Cantidad',
                                    color='Cantidad', color_continuous_scale='Blues')
                fig_estados.update_traces(textposition='outside')
                fig_estados.update_layout(
                    showlegend=False,
                    height=400,
                    xaxis_title="Cantidad de OCs",
                    yaxis_title=None
                )
                st.plotly_chart(fig_estados, use_container_width=True)

        st.divider()

        # ===== SECCIÓN 1: INDICADORES FINANCIEROS =====
        st.markdown("""
        <div style="margin: 2rem 0 1.5rem 0;">
            <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                Análisis Financiero de Órdenes de Compra
            </h3>
            <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
                Montos transados y efectividad de recepción (basado en OCs, no en requerimientos)
            </p>
        </div>
        """, unsafe_allow_html=True)

        # Limpiar y convertir la columna Total OC manejando diferentes formatos
        if 'Total OC' in df_display.columns:
            # Convertir a string primero, remover comas, puntos como separadores de miles
            df_display['Total OC'] = df_display['Total OC'].astype(str).str.replace(',', '').str.replace('$', '').str.strip()
            # Convertir a numérico
            df_display['Total OC'] = pd.to_numeric(df_display['Total OC'], errors='coerce').fillna(0)

            monto_total = df_display['Total OC'].sum()

            # Estados conformes: Solo los estados finales del proceso
            # Según lógica de negocio: "Guardada" = inicio, "Aceptada" y "Recepción Conforme" = fin
            todos_estados = df_display['Estado OC'].dropna().str.strip().unique()

            # Buscar específicamente los estados finales
            estados_conformes = []
            for estado in todos_estados:
                estado_clean = estado.strip()
                if estado_clean in ['Aceptada', 'Recepción Conforme']:
                    estados_conformes.append(estado_clean)

            # Si no se encontraron los estados esperados, usar los predeterminados
            if not estados_conformes:
                estados_conformes = ['Recepción Conforme', 'Aceptada']

            monto_conforme = df_display[df_display['Estado OC'].isin(estados_conformes)]['Total OC'].sum()
            efectividad_recepcion = (monto_conforme / monto_total * 100) if monto_total > 0 else 0

        else:
            monto_total = 0
            monto_conforme = 0
            efectividad_recepcion = 0

        kpi1, kpi2, kpi3 = st.columns(3, gap="large")

        with kpi1:
            st.markdown(f"""
            <div class="kpi-card-professional">
                <div class="kpi-icon" style="background: var(--primary-blue);">💵</div>
                <div class="kpi-value">${monto_total:,.0f}</div>
                <div class="kpi-label">Monto Total Transado</div>
                <div class="kpi-trend">Volumen de negocio</div>
            </div>
            """, unsafe_allow_html=True)

        with kpi2:
            st.markdown(f"""
            <div class="kpi-card-professional">
                <div class="kpi-icon" style="background: var(--success-color);">✅</div>
                <div class="kpi-value">${monto_conforme:,.0f}</div>
                <div class="kpi-label">Recepción Conforme</div>
                <div class="kpi-trend">Ingresos validados</div>
            </div>
            """, unsafe_allow_html=True)

        with kpi3:
            st.markdown(f"""
            <div class="kpi-card-professional">
                <div class="kpi-icon" style="background: var(--secondary-blue);">📊</div>
                <div class="kpi-value">{efectividad_recepcion:.1f}%</div>
                <div class="kpi-label">Efectividad</div>
                <div class="kpi-trend">Tasa de éxito</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.divider()

        # ===== SECCIÓN 2: ANÁLISIS POR MONTO ($) =====
        st.markdown("""
        <div style="margin: 3rem 0 1.5rem 0;">
            <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                📊 Análisis por Monto Financiero
            </h3>
            <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
                Distribución y ranking por montos transados (1 requerimiento puede tener múltiples OCs)
            </p>
        </div>
        """, unsafe_allow_html=True)

        col_monto1, col_monto2 = st.columns(2, gap="large")

        with col_monto1:
            st.markdown("#### 📈 Distribución del Monto por Tipo de Compra")
            monto_por_tipo = df_display.groupby('tipo_compra')['Total OC'].sum().reset_index()

            # Paleta de colores profesional azul/celeste
            professional_blue_palette = ['#1E40AF', '#3B82F6', '#60A5FA', '#93C5FD', '#DBEAFE', '#EFF6FF']

            fig_pie = px.pie(monto_por_tipo, names='tipo_compra', values='Total OC', hole=0.4,
                           color_discrete_sequence=professional_blue_palette)
            fig_pie.update_traces(
                texttemplate='$%{value:,.0f}<br>(%{percent})',
                textfont_size=12,
                textfont_color='white',
                pull=[0.1 if i == 0 else 0 for i in range(len(monto_por_tipo))]
            )
            fig_pie.update_layout(
                template="plotly_white",
                height=400,
                showlegend=True,
                legend=dict(orientation="v", x=1.1, y=0.5, font=dict(color="#1E3A8A", size=10)),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_pie, use_container_width=True)

            st.markdown("#### 🏆 Top 3 Unidades por Monto Total")
            monto_por_unidad = df_display.groupby('unidad')['Total OC'].sum().nlargest(3)
            fig_bar_monto = px.bar(monto_por_unidad, y=monto_por_unidad.values, x=monto_por_unidad.index, text_auto=True)
            fig_bar_monto.update_traces(
                marker_color='#1E40AF',
                marker_line_color='#3B82F6',
                marker_line_width=1,
                textangle=0,
                textposition='outside',
                texttemplate='$%{y:,.0f}',
                textfont_color='#1F2937',
                textfont_size=11
            )
            fig_bar_monto.update_layout(
                xaxis_title=None,
                yaxis_title="Monto Total ($)",
                template="plotly_white",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                height=400,
                xaxis=dict(tickfont=dict(color="#1E3A8A")),
                yaxis=dict(tickfont=dict(color="#1E3A8A"))
            )
            st.plotly_chart(fig_bar_monto, use_container_width=True)

        with col_monto2:
            st.markdown("#### 🎯 Top 5 Unidades por Monto y Tipo")
            monto_por_unidad_tipo = df_display.groupby(['unidad', 'tipo_compra'])['Total OC'].sum().reset_index()
            top_unidades = df_display.groupby('unidad')['Total OC'].sum().nlargest(5).index
            df_top_unidades = monto_por_unidad_tipo[monto_por_unidad_tipo['unidad'].isin(top_unidades)]

            fig_stacked_bar = px.bar(df_top_unidades, y='unidad', x='Total OC', color='tipo_compra',
                                   orientation='h', category_orders={"unidad": top_unidades},
                                   color_discrete_sequence=professional_blue_palette)
            fig_stacked_bar.update_traces(
                texttemplate='$%{x:,.0f}',
                textposition='inside',
                textfont_color='white',
                textfont_size=10
            )
            fig_stacked_bar.update_layout(
                yaxis_title=None,
                xaxis_title="Suma de Total OC ($)",
                template="plotly_white",
                uniformtext_minsize=8,
                uniformtext_mode='hide',
                height=400,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                legend=dict(orientation="h", x=0, y=-0.2, font=dict(color="#1E3A8A", size=10)),
                xaxis=dict(tickfont=dict(color="#1E3A8A")),
                yaxis=dict(tickfont=dict(color="#1E3A8A"))
            )
            st.plotly_chart(fig_stacked_bar, use_container_width=True)
        st.divider()

        # ===== SECCIÓN 3: ANÁLISIS POR CANTIDAD (#) =====
        st.markdown("""
        <div style="margin: 3rem 0 1.5rem 0;">
            <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                📋 Análisis por Cantidad de Órdenes
            </h3>
            <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
                Rankings por volumen de OCs (un requerimiento puede generar varias OCs)
            </p>
        </div>
        """, unsafe_allow_html=True)

        col_cantidad1, col_cantidad2 = st.columns(2, gap="large")

        with col_cantidad1:
            st.markdown("#### 🥇 Top 3 Unidades por Cantidad de OCs")
            oc_por_unidad = df_display['unidad'].value_counts().head(3)
            fig_bar_cantidad = px.bar(oc_por_unidad, x=oc_por_unidad.values, y=oc_por_unidad.index,
                                     orientation='h', text=oc_por_unidad.values)
            fig_bar_cantidad.update_traces(
                marker_color='#3B82F6',
                marker_line_color='#1E3A8A',
                marker_line_width=2,
                textposition='outside',
                textfont_color='#1E3A8A',
                textfont_size=12
            )
            fig_bar_cantidad.update_layout(
                yaxis_title=None,
                xaxis_title="Cantidad de OC",
                yaxis={'categoryorder':'total ascending', 'tickfont': dict(color="#1E3A8A")},
                template="plotly_white",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                height=300,
                xaxis=dict(tickfont=dict(color="#1E3A8A"))
            )
            st.plotly_chart(fig_bar_cantidad, use_container_width=True)

        with col_cantidad2:
            # --- GRÁFICO COMPRAS ÁGILES ---
            st.markdown("#### 🚀 OCs de Compras Ágiles por Unidad")
            df_agiles = df_display[df_display['tipo_compra'] == 'Compra Ágil']
            if not df_agiles.empty:
                count_agiles = df_agiles['unidad'].value_counts().nlargest(10).sort_values(ascending=True)
                fig_agiles = px.bar(count_agiles, x=count_agiles.values, y=count_agiles.index,
                                   orientation='h', text=count_agiles.values)
                fig_agiles.update_traces(
                    marker_color='#60A5FA',
                    marker_line_color='#3B82F6',
                    marker_line_width=1,
                    textposition='outside',
                    textfont_color='#1E3A8A',
                    textfont_size=10
                )
                fig_agiles.update_layout(
                    yaxis_title=None,
                    xaxis_title="Cantidad de OCs",
                    template="plotly_white",
                    height=280,
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(tickfont=dict(color="#1E3A8A")),
                    yaxis=dict(tickfont=dict(color="#1E3A8A"))
                )
                st.plotly_chart(fig_agiles, use_container_width=True)
            else:
                st.info("No hay datos de Compra Ágil para mostrar.")

            # --- GRÁFICO LICITACIONES ---
            st.markdown("#### 📜 OCs de Licitaciones por Unidad")
            df_licitaciones = df_display[df_display['tipo_compra'] == 'Licitación']
            if not df_licitaciones.empty:
                count_licitaciones = df_licitaciones['unidad'].value_counts().nlargest(10).sort_values(ascending=True)
                fig_licitaciones = px.bar(count_licitaciones, x=count_licitaciones.values, y=count_licitaciones.index,
                                         orientation='h', text=count_licitaciones.values)
                fig_licitaciones.update_traces(
                    marker_color='#1E3A8A',
                    marker_line_color='#1E3A8A',
                    marker_line_width=1,
                    textposition='outside',
                    textfont_color='#1E3A8A',
                    textfont_size=10
                )
                fig_licitaciones.update_layout(
                    yaxis_title=None,
                    xaxis_title="Cantidad de OCs",
                    template="plotly_white",
                    height=280,
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(tickfont=dict(color="#1E3A8A")),
                    yaxis=dict(tickfont=dict(color="#1E3A8A"))
                )
                st.plotly_chart(fig_licitaciones, use_container_width=True)
            else:
                st.info("No hay datos de Licitación para mostrar.")



def pagina_compras_agiles():
    st.header("🔬 Análisis Detallado de Compras Ágiles")
    if st.button("⬅️ Volver al Dashboard Principal"):
        st.session_state.view = 'dashboard'
        st.rerun()
    
    if 'resumen_agil' in st.session_state:
        resumen = st.session_state.resumen_agil
        st.info(f"Resumen del último análisis: Se procesaron **{resumen['total']:,}** requerimientos de Compra Ágil. De estos, **{resumen['con_oc']:,}** tienen OC emitida y **{resumen['pendientes']:,}** se encuentran pendientes.")

    st.divider()
    
    st.subheader("📤 Cargar Nuevo Archivo para Análisis Específico de Compras Ágiles")
    file_resultado_oc_agil = st.file_uploader(
        "Sube el archivo 'ListadoResultadoOC' para actualizar el análisis.", 
        type=['xlsx', 'csv'], 
        label_visibility="collapsed",
        key="uploader_agil"
    )

    if file_resultado_oc_agil:
        with st.spinner("Procesando archivo para Compras Ágiles..."):
            try:
                with sqlite3.connect('compras.db') as conn:
                    last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
                    if last_sesion_id_df.empty:
                        st.warning("No hay datos de sesiones en la base de datos."); st.stop()
                    
                    last_sesion_id = int(last_sesion_id_df.iloc[0, 0])
                    query_proc = "SELECT * FROM procesados WHERE tipo_compra = 'Compra Ágil' AND sesion_id = ?"
                    df_procesados_agiles = pd.read_sql_query(query_proc, conn, params=(last_sesion_id,))
                    query_en_proc = "SELECT * FROM en_proceso WHERE tipo_compra = 'Compra Ágil' AND sesion_id = ?"
                    df_en_proceso_agiles = pd.read_sql_query(query_en_proc, conn, params=(last_sesion_id,))
                    st.session_state.df_en_proceso_agiles = df_en_proceso_agiles
                
                num_con_oc = len(df_procesados_agiles)
                num_pendientes = len(df_en_proceso_agiles)
                st.session_state.resumen_agil = {
                    "total": num_con_oc + num_pendientes,
                    "con_oc": num_con_oc,
                    "pendientes": num_pendientes
                }

                df_resultado = leer_archivo(file_resultado_oc_agil)
                if df_procesados_agiles.empty or df_resultado is None:
                    st.warning("No se encontraron datos de Compras Ágiles procesadas o el archivo subido está vacío."); st.stop()

                # Detectar columna de orden de compra
                col_oc_resultado = None
                patrones_oc = ['orden', 'oc', 'n°', 'numero', 'número', 'compra']
                for col in df_resultado.columns:
                    col_lower = str(col).lower().strip()
                    if any(patron in col_lower for patron in patrones_oc):
                        col_oc_resultado = col
                        break

                if not col_oc_resultado:
                    st.error(f"❌ No se pudo detectar la columna de orden de compra. Columnas disponibles: {', '.join(df_resultado.columns)}")
                    st.stop()

                # Normalizar columnas a texto
                df_procesados_agiles['orden_compra'] = df_procesados_agiles['orden_compra'].astype(str).str.strip().str.upper()
                df_resultado[col_oc_resultado] = df_resultado[col_oc_resultado].astype(str).str.strip().str.upper()

                df_merged_full = pd.merge(
                    df_procesados_agiles, df_resultado,
                    left_on='orden_compra', right_on=col_oc_resultado, how='left'
                )
                df_analisis_calculado = df_merged_full.dropna(subset=[col_oc_resultado]).copy()
                
                if not df_analisis_calculado.empty:
                    df_analisis_calculado['Estado OC'] = df_analisis_calculado['Estado OC'].astype(str)
                    terminos_cancelacion = ['cancelado', 'no aceptado', 'cancelada', 'cancelacion solicitaa']
                    def es_cancelada(estado):
                        estado_lower = estado.lower()
                        for termino in terminos_cancelacion:
                            if termino in estado_lower:
                                return True
                        return False
                    df_analisis_calculado['Estado OC'] = df_analisis_calculado['Estado OC'].apply(
                        lambda estado: 'Cancelada' if es_cancelada(estado) else estado
                    )
                
                st.session_state.df_agiles_analisis = df_analisis_calculado
                st.success("¡Análisis actualizado con el nuevo archivo!")
                st.rerun()

            except Exception as e:
                st.error(f"Ocurrió un error inesperado durante el análisis: {e}")
                st.exception(e)

    st.divider()

    if 'df_agiles_analisis' in st.session_state:
        df_analisis = st.session_state.df_agiles_analisis

        if df_analisis.empty and 'resumen_agil' in st.session_state and st.session_state.resumen_agil['total'] > 0:
             st.warning("El archivo subido no tuvo coincidencias con las Órdenes de Compra emitidas.")
        elif df_analisis.empty:
            st.info("El último archivo procesado no generó datos para analizar. Por favor, sube un nuevo archivo.")
        else:
            st.subheader("Reporte Detallado de Órdenes de Compra Emitidas (Solo Compras Ágiles)")
            
            total_oc_analizadas = len(df_analisis)
            df_analisis['Total OC'] = pd.to_numeric(df_analisis['Total OC'], errors='coerce').fillna(0)
            monto_total = df_analisis['Total OC'].sum()
            estados_conformes = ['Recepción Conforme', 'Aceptada']
            monto_conforme = df_analisis[df_analisis['Estado OC'].isin(estados_conformes)]['Total OC'].sum()
            eficiencia_monto = (monto_conforme / monto_total * 100) if monto_total > 0 else 0
            
            with st.container(border=True):
                kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
                kpi_col1.metric("OCs Ágiles Analizadas", f"{total_oc_analizadas:,}")
                kpi_col2.metric("Monto Total OC", f"${monto_total:,.0f}")
                kpi_col3.metric("Monto Recepción Conforme", f"${monto_conforme:,.0f}")
                kpi_col4.metric("% Eficiencia en Monto", f"{eficiencia_monto:.1f}%")

            col_left, col_right = st.columns(2, gap="large")

            with col_left:
                with st.container(border=True):
                    st.markdown("#### 📊 Distribución por Estado de OC")
                    estado_counts = df_analisis['Estado OC'].value_counts().reset_index()

                    # Colores azules para diferentes estados
                    estado_colors = ['#1E40AF', '#3B82F6', '#60A5FA', '#93C5FD', '#DBEAFE']

                    fig_donut_estado = px.pie(
                        estado_counts, names='Estado OC', values='count', hole=0.5,
                        color_discrete_sequence=estado_colors
                    )
                    fig_donut_estado.update_traces(
                        textfont_size=12,
                        textfont_color='white',
                        texttemplate='%{label}<br>%{value}<br>(%{percent})'
                    )
                    fig_donut_estado.update_layout(
                        height=400,
                        legend_title_text='Estados',
                        template="plotly_dark",
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        legend=dict(font=dict(color="white", size=10))
                    )
                    st.plotly_chart(fig_donut_estado, use_container_width=True)
            
            with col_right:
                 with st.container(border=True):
                    st.markdown("##### Requerimientos Ágiles en Proceso (Sin OC)")
                    if 'df_en_proceso_agiles' in st.session_state:
                        st.dataframe(st.session_state.df_en_proceso_agiles, height=380, use_container_width=True)

            with st.expander("📋 Ver Detalle Completo de Órdenes de Compra Ágiles Analizadas", expanded=False):
                st.dataframe(df_analisis, use_container_width=True)
    else:
        st.info("Aún no se ha cargado ningún archivo para el análisis detallado de Compras Ágiles.")

def pagina_placeholder(titulo):
    st.header(f"{titulo}")
    st.info("Página en construcción.")

def pagina_cargar_datos():
    st.header("🚀 Cargar Nuevos Datos")
    uploaded_experto = st.file_uploader("📋 Experto Bruto (Requerido)", type=['xlsx', 'csv'])
    uploaded_cancelados = st.file_uploader("❌ Cancelados-Experto (Requerido)", type=['xlsx', 'csv'])
    uploaded_precompra = st.file_uploader("🔄 Informe Precompra (Opcional)", type=['xlsx', 'csv'])
    
    if uploaded_experto and uploaded_cancelados:
        if st.button("🚀 Procesar Archivos", type="primary", use_container_width=True):
            procesar_y_actualizar(uploaded_experto, uploaded_cancelados, uploaded_precompra)

def pagina_configuracion():
    st.header("⚙️ Configuración")
    st.warning("⚠️ **Acción destructiva:** Esto eliminará todos los datos históricos.")
    if st.button("🗑️ Eliminar Base de Datos", type="secondary"):
        st.session_state["delete_db_on_next_run"] = True
        st.rerun()

def procesar_y_actualizar(uploaded_experto, uploaded_cancelados, uploaded_precompra):
    with st.spinner("🔄 Procesando datos..."):
        try:
            df_experto = leer_archivo(uploaded_experto)
            df_cancelados = leer_archivo(uploaded_cancelados)
            df_precompra = leer_archivo(uploaded_precompra)
            if df_experto is None or df_cancelados is None:
                st.error("Error al leer los archivos requeridos.")
                return
            resultado = st.session_state.processor.procesar_datos(df_experto, df_cancelados, df_precompra)
            if resultado:
                st.success("✅ ¡Datos procesados! Redirigiendo al dashboard...")
                if 'df_oc_analisis' in st.session_state: del st.session_state.df_oc_analisis
                if 'df_agiles_analisis' in st.session_state: del st.session_state.df_agiles_analisis
                st.session_state.view = 'dashboard'
                time.sleep(2)
                st.rerun()
        except Exception as e:
            st.error(f"❌ Error durante el procesamiento: {str(e)}")

def leer_archivo(uploaded_file):
    if not uploaded_file:
        return None

    try:
        file_extension = uploaded_file.name.lower().split('.')[-1]

        if file_extension in ['xlsx', 'xls']:
            return pd.read_excel(uploaded_file)
        elif file_extension == 'csv':
            # Intentar múltiples encodings y separadores
            encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            separadores = [',', ';', '\t', '|']

            for encoding in encodings:
                for sep in separadores:
                    try:
                        df = pd.read_csv(uploaded_file, encoding=encoding, sep=sep)
                        # Verificar que el archivo se leyó correctamente
                        if len(df.columns) > 1 and len(df) > 0:
                            st.info(f"✅ Archivo CSV leído con encoding '{encoding}' y separador '{sep}'")
                            return df
                    except Exception:
                        continue

            # Si nada funciona, intentar con pandas sniff
            try:
                uploaded_file.seek(0)  # Resetear el cursor del archivo
                df = pd.read_csv(uploaded_file, encoding='utf-8', sep=None, engine='python')
                if len(df.columns) > 1 and len(df) > 0:
                    st.info("✅ Archivo CSV leído con detección automática de separador")
                    return df
            except Exception:
                pass

            st.error(f"❌ No se pudo leer el archivo CSV '{uploaded_file.name}' con ningún encoding o separador estándar")
            return None
        else:
            st.error(f"❌ Formato de archivo no soportado: {file_extension}")
            return None

    except Exception as e:
        st.error(f"❌ Error inesperado al leer {uploaded_file.name}: {e}")
        return None

def mostrar_dashboard(stats):
    st.sidebar.title("Navegación Principal")
    selected_page = st.sidebar.radio("Seleccionar Departamento:", ["Ordenes de Compra", "Finanzas", "Recursos Humanos", "Operaciones"], key="nav_main")

    # Subcategorías para Ordenes de Compra
    if selected_page == "Ordenes de Compra":
        st.sidebar.markdown("##### 📂 Análisis por Tipo")
        sub_page = st.sidebar.radio(
            "Seleccionar análisis:",
            ["📊 Dashboard General", "🚀 Compras Ágiles", "📜 Licitaciones"],
            key="nav_sub_oc",
            label_visibility="collapsed"
        )
    else:
        sub_page = None

    st.sidebar.divider()
    st.sidebar.title("Análisis Especiales")

    if st.sidebar.button("📅 Análisis Mes a Mes"):
        st.session_state.view = 'analisis_mensual'
        st.rerun()

    st.sidebar.divider()
    st.sidebar.title("Administración")
    if st.sidebar.button("📤 Cargar Nuevos Datos"):
        st.session_state.view = 'upload'
        st.rerun()
    if st.sidebar.button("⚙️ Configuración"):
        st.session_state.view = 'config'
        st.rerun()

    if selected_page == "Ordenes de Compra":
        if sub_page == "📊 Dashboard General":
            pagina_ordenes_compra(stats)
        elif sub_page == "🚀 Compras Ágiles":
            pagina_compras_agiles_detallado(stats)
        elif sub_page == "📜 Licitaciones":
            pagina_licitaciones_detallado(stats)
    elif selected_page == "Finanzas":
        pagina_placeholder("💰 Finanzas")
    elif selected_page == "Recursos Humanos":
        pagina_placeholder("👥 Recursos Humanos")
    elif selected_page == "Operaciones":
        pagina_placeholder("⚙️ Operaciones")

def pagina_compras_agiles_detallado(stats):
    """Página detallada de análisis de Compras Ágiles con flujo completo"""
    st.markdown('<h1 style="text-align: center; margin-bottom: 2rem;">🚀 Análisis Detallado: Compras Ágiles</h1>', unsafe_allow_html=True)

    try:
        with sqlite3.connect('compras.db') as conn:
            # Obtener la última sesión
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
            if last_sesion_id_df.empty:
                st.warning("No hay datos disponibles.")
                return

            last_sesion_id = int(last_sesion_id_df.iloc[0, 0])

            # Obtener estadísticas de la sesión
            sesion_stats = pd.read_sql_query("SELECT * FROM sesiones WHERE id = ?", conn, params=(last_sesion_id,))
            total_brutos_general = sesion_stats['total_brutos'].iloc[0]
            total_cancelados_general = sesion_stats['req_cancelados'].iloc[0]

            # Obtener datos de Compras Ágiles
            df_procesados_ca = pd.read_sql_query(
                "SELECT * FROM procesados WHERE sesion_id = ? AND tipo_compra = 'Compra Ágil'",
                conn, params=(last_sesion_id,)
            )
            df_en_proceso_ca = pd.read_sql_query(
                "SELECT * FROM en_proceso WHERE sesion_id = ? AND tipo_compra = 'Compra Ágil'",
                conn, params=(last_sesion_id,)
            )

            # Calcular métricas de Compra Ágil
            total_procesados_ca = len(df_procesados_ca)
            total_en_proceso_ca = len(df_en_proceso_ca)
            total_ca_neto = total_procesados_ca + total_en_proceso_ca

            # Necesitamos saber cuántos cancelados eran de Compra Ágil
            # Para esto, tendríamos que tener esa información en los datos originales
            # Por ahora, estimaremos proporcionalmente
            st.info("ℹ️ Nota: Los datos de cancelados se calculan proporcionalmente del total general")

            # KPIs principales de Compra Ágil
            st.markdown("""
            <div style="text-align: center; margin: 2rem 0;">
                <h2 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                    Indicadores de Compras Ágiles
                </h2>
                <p style="color: var(--text-secondary); font-size: 1rem; margin: 0;">
                    Flujo completo: Ingresados → Procesados
                </p>
            </div>
            """, unsafe_allow_html=True)

            col1, col2, col3, col4 = st.columns(4, gap="medium")

            with col1:
                st.markdown(f"""
                <div class="kpi-card-professional">
                    <div class="kpi-icon" style="background: var(--sky-blue);">📥</div>
                    <div class="kpi-value">{total_ca_neto:,}</div>
                    <div class="kpi-label">Total CA Neto</div>
                    <div class="kpi-trend">Después de cancelados</div>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                st.markdown(f"""
                <div class="kpi-card-professional">
                    <div class="kpi-icon" style="background: var(--success-color);">✅</div>
                    <div class="kpi-value">{total_procesados_ca:,}</div>
                    <div class="kpi-label">Procesados</div>
                    <div class="kpi-trend">Con OC emitida</div>
                </div>
                """, unsafe_allow_html=True)

            with col3:
                st.markdown(f"""
                <div class="kpi-card-professional">
                    <div class="kpi-icon" style="background: var(--warning-color);">⏳</div>
                    <div class="kpi-value">{total_en_proceso_ca:,}</div>
                    <div class="kpi-label">En Proceso</div>
                    <div class="kpi-trend">Pendientes</div>
                </div>
                """, unsafe_allow_html=True)

            with col4:
                eficiencia_ca = (total_procesados_ca / total_ca_neto * 100) if total_ca_neto > 0 else 0
                st.markdown(f"""
                <div class="kpi-card-professional">
                    <div class="kpi-icon">🎯</div>
                    <div class="kpi-value">{eficiencia_ca:.1f}%</div>
                    <div class="kpi-label">Eficiencia CA</div>
                    <div class="kpi-trend">Tasa de éxito</div>
                </div>
                """, unsafe_allow_html=True)

            st.divider()

            # Sección de análisis financiero
            st.markdown("""
            <div style="margin: 2rem 0 1.5rem 0;">
                <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                    💰 Análisis Financiero de Compras Ágiles
                </h3>
                <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
                    Carga los archivos para análisis monetario (pueden ser de cualquier mes/año)
                </p>
            </div>
            """, unsafe_allow_html=True)

            col_upload1, col_upload2 = st.columns(2, gap="medium")

            with col_upload1:
                file_resultado_oc = st.file_uploader(
                    "📊 ListadoResultadoOC (Obligatorio)",
                    type=['xlsx', 'csv'],
                    key="uploader_ca_resultado"
                )

            with col_upload2:
                file_experto_historico = st.file_uploader(
                    "📁 Experto Histórico (Obligatorio)",
                    type=['xlsx', 'csv'],
                    help="Puede ser de cualquier mes/año. Se usa para obtener unidades y datos adicionales",
                    key="uploader_ca_experto_historico"
                )

            if file_resultado_oc and file_experto_historico:
                with st.spinner("Procesando datos financieros..."):
                    df_resultado = leer_archivo(file_resultado_oc)
                    df_experto_hist = leer_archivo(file_experto_historico)

                    if df_resultado is not None and df_experto_hist is not None:
                        # Detectar columna de orden de compra en resultado
                        col_oc_resultado = None
                        patrones_oc = ['orden', 'oc', 'n°', 'numero', 'número', 'compra']
                        for col in df_resultado.columns:
                            col_lower = str(col).lower().strip()
                            if any(patron in col_lower for patron in patrones_oc):
                                col_oc_resultado = col
                                break

                        if not col_oc_resultado:
                            st.error(f"❌ No se pudo detectar la columna de orden de compra en ListadoResultadoOC. Columnas disponibles: {', '.join(df_resultado.columns)}")
                            st.stop()

                        # Filtrar OCs con código AG del archivo resultado
                        df_resultado_ag = df_resultado[
                            df_resultado[col_oc_resultado].astype(str).str.upper().str.contains('AG', na=False)
                        ].copy()

                        st.info(f"📊 Total de OCs con código AG encontradas: {len(df_resultado_ag)}")

                        # Detectar columnas en experto histórico
                        COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC = st.session_state.processor._detectar_columnas(df_experto_hist)

                        if not COL_NUM_REQ or not COL_ESTADO_OC:
                            st.error("❌ No se pudieron detectar las columnas necesarias en el archivo Experto")
                            st.stop()

                        # Detectar columnas adicionales (unidad, título, comprador)
                        col_titulo, col_unidad, col_comprador = st.session_state.processor._detectar_columnas_adicionales(
                            df_experto_hist, COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC
                        )

                        # Normalizar números de OC para el merge
                        df_resultado_ag['oc_normalizada'] = df_resultado_ag[col_oc_resultado].astype(str).str.strip().str.upper()
                        df_experto_hist['oc_normalizada'] = df_experto_hist[COL_ESTADO_OC].astype(str).str.strip().str.upper()

                        # Hacer merge por número de orden de compra
                        df_analisis_ca = pd.merge(
                            df_resultado_ag,
                            df_experto_hist,
                            left_on='oc_normalizada',
                            right_on='oc_normalizada',
                            how='left',
                            suffixes=('', '_experto')
                        ).copy()

                        matches = df_analisis_ca[col_unidad].notna().sum() if col_unidad else 0
                        st.success(f"✅ Merge por número de OC completado. {matches} de {len(df_analisis_ca)} OCs con match")

                        # Debug: Mostrar OCs sin match
                        sin_match = df_analisis_ca[df_analisis_ca[col_unidad].isna()] if col_unidad else df_analisis_ca
                        if len(sin_match) > 0:
                            st.warning(f"⚠️ {len(sin_match)} OCs sin match")
                            with st.expander("🔍 Ver detalles de OCs sin match"):
                                debug_cols = [col_oc_resultado, 'oc_normalizada']
                                if len(sin_match) > 0:
                                    st.dataframe(sin_match[debug_cols].head(10), use_container_width=True)

                        # Renombrar columnas para consistencia
                        if col_unidad and col_unidad != 'unidad':
                            df_analisis_ca['unidad'] = df_analisis_ca[col_unidad]

                        # Limpiar y convertir Total OC
                        if 'Total OC' in df_analisis_ca.columns:
                            df_analisis_ca['Total OC'] = df_analisis_ca['Total OC'].astype(str).str.replace(',', '').str.replace('$', '').str.strip()
                            df_analisis_ca['Total OC'] = pd.to_numeric(df_analisis_ca['Total OC'], errors='coerce').fillna(0)

                            monto_total_ca = df_analisis_ca['Total OC'].sum()

                            # Estados conformes
                            estados_conformes = ['Recepción Conforme', 'Aceptada']
                            monto_conforme_ca = df_analisis_ca[df_analisis_ca['Estado OC'].isin(estados_conformes)]['Total OC'].sum()
                            efectividad_monto = (monto_conforme_ca / monto_total_ca * 100) if monto_total_ca > 0 else 0

                            # KPIs Financieros
                            st.markdown("### 💵 Indicadores Financieros")
                            col_f1, col_f2, col_f3 = st.columns(3, gap="large")

                            with col_f1:
                                st.markdown(f"""
                                <div class="kpi-card-professional">
                                    <div class="kpi-icon" style="background: var(--primary-blue);">💵</div>
                                    <div class="kpi-value">${monto_total_ca:,.0f}</div>
                                    <div class="kpi-label">Monto Total CA</div>
                                    <div class="kpi-trend">Transado</div>
                                </div>
                                """, unsafe_allow_html=True)

                            with col_f2:
                                st.markdown(f"""
                                <div class="kpi-card-professional">
                                    <div class="kpi-icon" style="background: var(--success-color);">✅</div>
                                    <div class="kpi-value">${monto_conforme_ca:,.0f}</div>
                                    <div class="kpi-label">Recepción Conforme</div>
                                    <div class="kpi-trend">Validado</div>
                                </div>
                                """, unsafe_allow_html=True)

                            with col_f3:
                                st.markdown(f"""
                                <div class="kpi-card-professional">
                                    <div class="kpi-icon" style="background: var(--secondary-blue);">📊</div>
                                    <div class="kpi-value">{efectividad_monto:.1f}%</div>
                                    <div class="kpi-label">Efectividad $</div>
                                    <div class="kpi-trend">Tasa conforme</div>
                                </div>
                                """, unsafe_allow_html=True)

                            st.divider()

                            # Análisis por Unidad
                            st.markdown("### 🏢 Análisis por Unidad Solicitante")

                            col_u1, col_u2 = st.columns(2, gap="large")

                            with col_u1:
                                st.markdown("#### 💰 Top 5 Unidades por Monto")
                                if 'unidad' in df_analisis_ca.columns:
                                    monto_por_unidad = df_analisis_ca.groupby('unidad')['Total OC'].sum().nlargest(5).sort_values(ascending=True)
                                    fig_monto_unidad = px.bar(
                                        monto_por_unidad,
                                        x=monto_por_unidad.values,
                                        y=monto_por_unidad.index,
                                        orientation='h',
                                        text=monto_por_unidad.values
                                    )
                                    fig_monto_unidad.update_traces(
                                        marker_color='#3B82F6',
                                        texttemplate='$%{text:,.0f}',
                                        textposition='outside',
                                        textfont_color='#1E3A8A'
                                    )
                                    fig_monto_unidad.update_layout(
                                        xaxis_title="Monto Total ($)",
                                        yaxis_title=None,
                                        template="plotly_white",
                                        height=400,
                                        showlegend=False
                                    )
                                    st.plotly_chart(fig_monto_unidad, use_container_width=True)

                            with col_u2:
                                st.markdown("#### 📊 Top 5 Unidades por Cantidad de OCs")
                                if 'unidad' in df_analisis_ca.columns:
                                    count_por_unidad = df_analisis_ca['unidad'].value_counts().head(5).sort_values(ascending=True)
                                    fig_count_unidad = px.bar(
                                        count_por_unidad,
                                        x=count_por_unidad.values,
                                        y=count_por_unidad.index,
                                        orientation='h',
                                        text=count_por_unidad.values
                                    )
                                    fig_count_unidad.update_traces(
                                        marker_color='#60A5FA',
                                        textposition='outside',
                                        textfont_color='#1E3A8A'
                                    )
                                    fig_count_unidad.update_layout(
                                        xaxis_title="Cantidad de OCs",
                                        yaxis_title=None,
                                        template="plotly_white",
                                        height=400,
                                        showlegend=False
                                    )
                                    st.plotly_chart(fig_count_unidad, use_container_width=True)

                            # Tabla detallada por unidad
                            st.markdown("#### 📋 Resumen Detallado por Unidad")
                            if 'unidad' in df_analisis_ca.columns:
                                resumen_unidades = df_analisis_ca.groupby('unidad').agg({
                                    'Total OC': ['sum', 'mean', 'count']
                                }).round(0)
                                resumen_unidades.columns = ['Monto Total', 'Monto Promedio', 'Cantidad OCs']
                                resumen_unidades = resumen_unidades.sort_values('Monto Total', ascending=False)
                                resumen_unidades['Monto Total'] = resumen_unidades['Monto Total'].apply(lambda x: f"${x:,.0f}")
                                resumen_unidades['Monto Promedio'] = resumen_unidades['Monto Promedio'].apply(lambda x: f"${x:,.0f}")
                                resumen_unidades['Cantidad OCs'] = resumen_unidades['Cantidad OCs'].astype(int)

                                st.dataframe(resumen_unidades, use_container_width=True)

                            st.divider()

                            # Datos detallados
                            with st.expander("📄 Ver Datos Completos de Compras Ágiles Procesadas"):
                                st.dataframe(df_analisis_ca, use_container_width=True)

            # Requerimientos en proceso
            st.divider()
            st.markdown("### ⏳ Compras Ágiles En Proceso (Sin OC)")
            if not df_en_proceso_ca.empty:
                st.dataframe(df_en_proceso_ca, use_container_width=True, height=400)
            else:
                st.success("✅ ¡Excelente! No hay compras ágiles pendientes.")

    except Exception as e:
        st.error(f"Error al generar análisis: {e}")
        st.exception(e)

def pagina_licitaciones_detallado(stats):
    """Página detallada de análisis de Licitaciones"""
    st.markdown('<h1 style="text-align: center; margin-bottom: 2rem;">📜 Análisis Detallado: Licitaciones</h1>', unsafe_allow_html=True)

    # ===== KPIs DE REQUERIMIENTOS (BASE DE DATOS) - PRIMERO =====
    try:
        with sqlite3.connect('compras.db') as conn:
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
            if not last_sesion_id_df.empty:
                last_sesion_id = int(last_sesion_id_df.iloc[0, 0])

                # Obtener datos de Licitaciones
                df_procesados_lic = pd.read_sql_query(
                    "SELECT * FROM procesados WHERE sesion_id = ? AND tipo_compra = 'Licitación'",
                    conn, params=(last_sesion_id,)
                )
                df_en_proceso_lic = pd.read_sql_query(
                    "SELECT * FROM en_proceso WHERE sesion_id = ? AND tipo_compra = 'Licitación'",
                    conn, params=(last_sesion_id,)
                )

                total_procesados_lic = len(df_procesados_lic)
                total_en_proceso_lic = len(df_en_proceso_lic)
                total_lic_neto = total_procesados_lic + total_en_proceso_lic

                # KPIs principales de Licitaciones
                st.markdown("""
                <div style="text-align: center; margin: 2rem 0;">
                    <h2 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
                        Indicadores de Requerimientos
                    </h2>
                </div>
                """, unsafe_allow_html=True)

                col1, col2, col3, col4 = st.columns(4, gap="medium")

                with col1:
                    st.markdown(f"""
                    <div class="kpi-card-professional">
                        <div class="kpi-icon" style="background: var(--sky-blue);">📥</div>
                        <div class="kpi-value">{total_lic_neto:,}</div>
                        <div class="kpi-label">Total Licitaciones</div>
                        <div class="kpi-trend">Neto</div>
                    </div>
                    """, unsafe_allow_html=True)

                with col2:
                    st.markdown(f"""
                    <div class="kpi-card-professional">
                        <div class="kpi-icon" style="background: var(--success-color);">✅</div>
                        <div class="kpi-value">{total_procesados_lic:,}</div>
                        <div class="kpi-label">Procesadas</div>
                        <div class="kpi-trend">Con OC</div>
                    </div>
                    """, unsafe_allow_html=True)

                with col3:
                    st.markdown(f"""
                    <div class="kpi-card-professional">
                        <div class="kpi-icon" style="background: var(--warning-color);">⏳</div>
                        <div class="kpi-value">{total_en_proceso_lic:,}</div>
                        <div class="kpi-label">En Proceso</div>
                        <div class="kpi-trend">Pendientes</div>
                    </div>
                    """, unsafe_allow_html=True)

                with col4:
                    eficiencia_lic = (total_procesados_lic / total_lic_neto * 100) if total_lic_neto > 0 else 0
                    st.markdown(f"""
                    <div class="kpi-card-professional">
                        <div class="kpi-icon">🎯</div>
                        <div class="kpi-value">{eficiencia_lic:.1f}%</div>
                        <div class="kpi-label">Eficiencia</div>
                        <div class="kpi-trend">Tasa éxito</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.divider()
    except Exception as e:
        st.warning(f"No se pudieron cargar los KPIs de requerimientos: {e}")

    # ===== SECCIÓN: ANÁLISIS DE RESULTADOS LICITACIONES =====
    st.markdown("""
    <div style="margin: 2rem 0 1.5rem 0;">
        <h3 style="color: var(--text-primary); font-weight: 600; margin-bottom: 0.5rem;">
            📋 Análisis de Resultados de Licitaciones
        </h3>
        <p style="color: var(--text-secondary); font-size: 0.9rem; margin: 0;">
            Carga el archivo de resultados de licitaciones para ver estados, montos estimados y adjudicados
        </p>
    </div>
    """, unsafe_allow_html=True)

    col_upload1, col_upload2 = st.columns(2, gap="medium")

    with col_upload1:
        file_resultados_lic = st.file_uploader(
            "📊 ListadoResultadoLicitacion (Obligatorio)",
            type=['xlsx', 'csv'],
            key="uploader_resultados_licitaciones"
        )

    with col_upload2:
        file_seguimiento_lic = st.file_uploader(
            "📋 Seguimiento Licitaciones (Obligatorio)",
            type=['xlsx', 'csv'],
            help="Contiene información adicional: responsable, OC, monto adjudicado",
            key="uploader_seguimiento_licitaciones"
        )

    if file_resultados_lic and file_seguimiento_lic:
        with st.spinner("Procesando resultados de licitaciones..."):
            df_resultado = leer_archivo(file_resultados_lic)
            df_seguimiento = leer_archivo(file_seguimiento_lic)

            if df_resultado is not None and df_seguimiento is not None:
                # Detectar columnas de ID para merge
                col_id_resultado = None
                col_id_seguimiento = None

                for col in df_resultado.columns:
                    if 'nro' in str(col).lower() and 'adquisici' in str(col).lower():
                        col_id_resultado = col
                        break

                for col in df_seguimiento.columns:
                    if 'id' in str(col).lower() and 'mercado' in str(col).lower():
                        col_id_seguimiento = col
                        break

                if not col_id_resultado or not col_id_seguimiento:
                    st.error(f"❌ No se pudieron detectar las columnas de ID. Resultado: {col_id_resultado}, Seguimiento: {col_id_seguimiento}")
                    st.stop()

                # Normalizar IDs para merge
                df_resultado['id_normalizado'] = df_resultado[col_id_resultado].astype(str).str.strip().str.upper()
                df_seguimiento['id_normalizado'] = df_seguimiento[col_id_seguimiento].astype(str).str.strip().str.upper()

                # Hacer merge
                df_licitaciones = pd.merge(
                    df_resultado,
                    df_seguimiento,
                    on='id_normalizado',
                    how='left',
                    suffixes=('', '_seg')
                )

                matches = df_licitaciones['Responsable'].notna().sum() if 'Responsable' in df_licitaciones.columns else 0
                st.success(f"✅ Merge completado: {len(df_licitaciones)} licitaciones. {matches} con información de seguimiento")

                # Detectar columnas importantes
                col_estado = None
                col_monto_estimado = None
                col_monto_adjudicado = None
                col_tipo = None
                col_lineas = None
                col_ofertas = None

                for col in df_licitaciones.columns:
                    col_lower = str(col).lower().strip()
                    if 'estado' in col_lower and 'licitaci' in col_lower:
                        col_estado = col
                    if 'estimado' in col_lower and 'total' in col_lower:
                        col_monto_estimado = col
                    if '$' in col_lower and 'adjudicado' in col_lower:
                        col_monto_adjudicado = col
                    if col == 'TIPO':
                        col_tipo = col
                    if 'lineas' in col_lower and 'licitadas' in col_lower:
                        col_lineas = col
                    if 'ofertas' in col_lower and 'recibidas' in col_lower:
                        col_ofertas = col

                # Si no encontró monto adjudicado con $, buscar alternativa
                if not col_monto_adjudicado:
                    for col in df_licitaciones.columns:
                        if 'adjudicado' in str(col).lower():
                            col_monto_adjudicado = col
                            break

                st.info(f"📋 Columnas detectadas: Estado='{col_estado}', Estimado='{col_monto_estimado}', Adjudicado='{col_monto_adjudicado}', Tipo='{col_tipo}'")

                # Limpiar montos
                if col_monto_estimado:
                    df_licitaciones[col_monto_estimado] = pd.to_numeric(
                        df_licitaciones[col_monto_estimado].astype(str).str.replace(',', '').str.replace('$', '').str.strip(),
                        errors='coerce'
                    ).fillna(0)

                if col_monto_adjudicado:
                    df_licitaciones[col_monto_adjudicado] = pd.to_numeric(
                        df_licitaciones[col_monto_adjudicado].astype(str).str.replace(',', '').str.replace('$', '').str.strip(),
                        errors='coerce'
                    ).fillna(0)

                if col_lineas:
                    df_licitaciones[col_lineas] = pd.to_numeric(
                        df_licitaciones[col_lineas].astype(str).str.replace(',', '').str.strip(),
                        errors='coerce'
                    ).fillna(0)

                if col_ofertas:
                    df_licitaciones[col_ofertas] = pd.to_numeric(
                        df_licitaciones[col_ofertas].astype(str).str.replace(',', '').str.strip(),
                        errors='coerce'
                    ).fillna(0)

                # ===== KPIs PRINCIPALES =====
                st.markdown("### 📊 Indicadores Principales")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    total_licitaciones = len(df_licitaciones)
                    st.metric("📜 Total Licitaciones", f"{total_licitaciones:,}")

                with col2:
                    if col_monto_estimado:
                        total_estimado = df_licitaciones[col_monto_estimado].sum()
                        st.metric("💵 Monto Estimado Total", f"${total_estimado:,.0f}")

                with col3:
                    if col_monto_adjudicado:
                        df_adjudicadas = df_licitaciones[df_licitaciones[col_monto_adjudicado] > 0]
                        total_adjudicadas = len(df_adjudicadas)
                        st.metric("✅ Licitaciones Adjudicadas", f"{total_adjudicadas:,}")

                with col4:
                    if col_monto_adjudicado:
                        total_adjudicado = df_adjudicadas[col_monto_adjudicado].sum()
                        st.metric("💰 Monto Adjudicado Total", f"${total_adjudicado:,.0f}")

                st.divider()

                # ===== DIVISIÓN POR TIPO (CSUM vs ADQ) =====
                if col_tipo and col_monto_estimado and col_monto_adjudicado:
                    st.markdown("### 🔄 Análisis por Tipo de Licitación")

                    # Filtrar por tipo
                    df_csum = df_licitaciones[df_licitaciones[col_tipo].astype(str).str.upper() == 'CSUM']
                    df_adq = df_licitaciones[df_licitaciones[col_tipo].astype(str).str.upper() == 'ADQ']

                    col_tipo1, col_tipo2 = st.columns(2)

                    with col_tipo1:
                        st.markdown("#### 📋 CSUM (Convenio de Suministro)")

                        csum_total = len(df_csum)
                        csum_monto_estimado = df_csum[col_monto_estimado].sum()
                        csum_adjudicadas = len(df_csum[df_csum[col_monto_adjudicado] > 0])
                        csum_monto_adjudicado = df_csum[df_csum[col_monto_adjudicado] > 0][col_monto_adjudicado].sum()

                        st.metric("Total Licitaciones", f"{csum_total:,}")
                        st.metric("Monto Estimado", f"${csum_monto_estimado:,.0f}")
                        st.metric("Adjudicadas", f"{csum_adjudicadas:,}")
                        st.metric("Monto Adjudicado", f"${csum_monto_adjudicado:,.0f}")

                    with col_tipo2:
                        st.markdown("#### 🛒 ADQ (Adquisición)")

                        adq_total = len(df_adq)
                        adq_monto_estimado = df_adq[col_monto_estimado].sum()
                        adq_adjudicadas = len(df_adq[df_adq[col_monto_adjudicado] > 0])
                        adq_monto_adjudicado = df_adq[df_adq[col_monto_adjudicado] > 0][col_monto_adjudicado].sum()

                        st.metric("Total Licitaciones", f"{adq_total:,}")
                        st.metric("Monto Estimado", f"${adq_monto_estimado:,.0f}")
                        st.metric("Adjudicadas", f"{adq_adjudicadas:,}")
                        st.metric("Monto Adjudicado", f"${adq_monto_adjudicado:,.0f}")

                    st.divider()

                st.divider()

                # Tabla de distribución por estado
                if col_estado:
                    st.markdown("### 📊 Distribución por Estado")

                    estados_count = df_licitaciones[col_estado].value_counts().reset_index()
                    estados_count.columns = ['Estado', 'Cantidad']

                    col_tabla, col_grafico = st.columns([1, 2])

                    with col_tabla:
                        st.dataframe(estados_count, use_container_width=True)

                    with col_grafico:
                        import plotly.express as px
                        fig = px.bar(estados_count, x='Cantidad', y='Estado', orientation='h',
                                    text='Cantidad', color='Cantidad', color_continuous_scale='Teal')
                        fig.update_traces(textposition='outside')
                        fig.update_layout(showlegend=False, height=400)
                        st.plotly_chart(fig, use_container_width=True)

                st.divider()

                # ===== ANÁLISIS DE LÍNEAS Y OFERTAS =====
                if col_lineas and col_ofertas:
                    st.markdown("### 📦 Análisis de Líneas Licitadas y Ofertas")

                    total_lineas = df_licitaciones[col_lineas].sum()
                    total_ofertas = df_licitaciones[col_ofertas].sum()
                    promedio_ofertas = df_licitaciones[col_ofertas].mean()

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("📦 Total Líneas Licitadas", f"{int(total_lineas):,}")
                    with col2:
                        st.metric("📝 Total Ofertas Recibidas", f"{int(total_ofertas):,}")
                    with col3:
                        st.metric("📊 Promedio Ofertas/Licitación", f"{promedio_ofertas:.1f}")

                    st.divider()

                    # ===== RANKING DE RESPONSABLES =====
                    if 'Responsable' in df_licitaciones.columns:
                        st.markdown("### 🏆 Ranking de Responsables")

                        # Crear tabs
                        tab1, tab2 = st.tabs(["📦 Por Líneas Licitadas", "📜 Por Cantidad de Licitaciones"])

                        with tab1:
                            st.markdown("#### 🥇 Top Responsables por Líneas Licitadas")
                            ranking_lineas = df_licitaciones.groupby('Responsable').agg({
                                col_lineas: 'sum',
                                'Responsable': 'count'
                            }).rename(columns={col_lineas: 'Total Líneas', 'Responsable': 'Cantidad Licitaciones'})
                            ranking_lineas = ranking_lineas.sort_values('Total Líneas', ascending=False)

                            col_rank1, col_rank2 = st.columns([1, 2])

                            with col_rank1:
                                st.dataframe(ranking_lineas.head(10), use_container_width=True)

                            with col_rank2:
                                import plotly.express as px
                                fig = px.bar(ranking_lineas.head(10).reset_index(),
                                           x='Total Líneas', y='Responsable', orientation='h',
                                           text='Total Líneas', color='Total Líneas',
                                           color_continuous_scale='Viridis')
                                fig.update_traces(textposition='outside')
                                fig.update_layout(showlegend=False, height=400, yaxis={'categoryorder':'total ascending'})
                                st.plotly_chart(fig, use_container_width=True)

                        with tab2:
                            st.markdown("#### 🥇 Top Responsables por Cantidad de Licitaciones")
                            ranking_cantidad = df_licitaciones.groupby('Responsable').agg({
                                'Responsable': 'count',
                                col_lineas: 'sum'
                            }).rename(columns={'Responsable': 'Cantidad Licitaciones', col_lineas: 'Total Líneas'})
                            ranking_cantidad = ranking_cantidad.sort_values('Cantidad Licitaciones', ascending=False)

                            col_rank3, col_rank4 = st.columns([1, 2])

                            with col_rank3:
                                st.dataframe(ranking_cantidad.head(10), use_container_width=True)

                            with col_rank4:
                                fig2 = px.bar(ranking_cantidad.head(10).reset_index(),
                                            x='Cantidad Licitaciones', y='Responsable', orientation='h',
                                            text='Cantidad Licitaciones', color='Cantidad Licitaciones',
                                            color_continuous_scale='Teal')
                                fig2.update_traces(textposition='outside')
                                fig2.update_layout(showlegend=False, height=400, yaxis={'categoryorder':'total ascending'})
                                st.plotly_chart(fig2, use_container_width=True)

                st.divider()

    # ===== SECCIÓN: ANÁLISIS FINANCIERO ADICIONAL (OC) =====
    try:
        with sqlite3.connect('compras.db') as conn:
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
            if not last_sesion_id_df.empty:
                last_sesion_id = int(last_sesion_id_df.iloc[0, 0])
                df_procesados_lic = pd.read_sql_query(
                    "SELECT * FROM procesados WHERE sesion_id = ? AND tipo_compra = 'Licitación'",
                    conn, params=(last_sesion_id,)
                )

            # Análisis financiero de Licitaciones
            st.markdown("### 💰 Análisis Financiero de Licitaciones")

            file_resultado_oc = st.file_uploader(
                "Sube el archivo 'ListadoResultadoOC'",
                type=['xlsx', 'csv'],
                key="uploader_lic_detallado"
            )

            if file_resultado_oc:
                with st.spinner("Procesando..."):
                    df_resultado = leer_archivo(file_resultado_oc)
                    if df_resultado is not None and not df_procesados_lic.empty:
                        # Detectar columna de orden de compra
                        col_oc_resultado = None
                        patrones_oc = ['orden', 'oc', 'n°', 'numero', 'número', 'compra']
                        for col in df_resultado.columns:
                            col_lower = str(col).lower().strip()
                            if any(patron in col_lower for patron in patrones_oc):
                                col_oc_resultado = col
                                break

                        if not col_oc_resultado:
                            st.error(f"❌ No se pudo detectar la columna de orden de compra. Columnas disponibles: {', '.join(df_resultado.columns)}")
                            st.stop()

                        # Normalizar columnas a texto
                        df_procesados_lic['orden_compra'] = df_procesados_lic['orden_compra'].astype(str).str.strip().str.upper()
                        df_resultado[col_oc_resultado] = df_resultado[col_oc_resultado].astype(str).str.strip().str.upper()

                        df_analisis_lic = pd.merge(
                            df_procesados_lic, df_resultado,
                            left_on='orden_compra', right_on=col_oc_resultado, how='left'
                        ).copy()

                        if 'Total OC' in df_analisis_lic.columns:
                            df_analisis_lic['Total OC'] = df_analisis_lic['Total OC'].astype(str).str.replace(',', '').str.replace('$', '').str.strip()
                            df_analisis_lic['Total OC'] = pd.to_numeric(df_analisis_lic['Total OC'], errors='coerce').fillna(0)

                            monto_total_lic = df_analisis_lic['Total OC'].sum()
                            estados_conformes = ['Recepción Conforme', 'Aceptada']
                            monto_conforme_lic = df_analisis_lic[df_analisis_lic['Estado OC'].isin(estados_conformes)]['Total OC'].sum()
                            efectividad_monto = (monto_conforme_lic / monto_total_lic * 100) if monto_total_lic > 0 else 0

                            col_f1, col_f2, col_f3 = st.columns(3, gap="large")

                            with col_f1:
                                st.metric("💵 Monto Total", f"${monto_total_lic:,.0f}")
                            with col_f2:
                                st.metric("✅ Recepción Conforme", f"${monto_conforme_lic:,.0f}")
                            with col_f3:
                                st.metric("📊 Efectividad", f"{efectividad_monto:.1f}%")

                            st.divider()

                            # Análisis por unidad
                            col_u1, col_u2 = st.columns(2, gap="large")

                            with col_u1:
                                st.markdown("#### 💰 Top 5 Unidades por Monto")
                                if 'unidad' in df_analisis_lic.columns:
                                    monto_por_unidad = df_analisis_lic.groupby('unidad')['Total OC'].sum().nlargest(5).sort_values(ascending=True)
                                    fig = px.bar(monto_por_unidad, x=monto_por_unidad.values, y=monto_por_unidad.index, orientation='h')
                                    fig.update_traces(marker_color='#1E40AF', texttemplate='$%{x:,.0f}', textposition='outside')
                                    fig.update_layout(xaxis_title="Monto ($)", yaxis_title=None, template="plotly_white", height=400)
                                    st.plotly_chart(fig, use_container_width=True)

                            with col_u2:
                                st.markdown("#### 📊 Top 5 Unidades por Cantidad")
                                if 'unidad' in df_analisis_lic.columns:
                                    count_por_unidad = df_analisis_lic['unidad'].value_counts().head(5).sort_values(ascending=True)
                                    fig = px.bar(count_por_unidad, x=count_por_unidad.values, y=count_por_unidad.index, orientation='h')
                                    fig.update_traces(marker_color='#3B82F6', textposition='outside')
                                    fig.update_layout(xaxis_title="Cantidad", yaxis_title=None, template="plotly_white", height=400)
                                    st.plotly_chart(fig, use_container_width=True)

                            with st.expander("📄 Ver Datos Completos"):
                                st.dataframe(df_analisis_lic, use_container_width=True)

            # En proceso
            st.divider()
            st.markdown("### ⏳ Licitaciones En Proceso")
            if not df_en_proceso_lic.empty:
                st.dataframe(df_en_proceso_lic, use_container_width=True, height=400)
            else:
                st.success("✅ No hay licitaciones pendientes.")

    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)

def pagina_analisis_mensual():
    """Página para análisis mes a mes de requerimientos"""
    st.header("📅 Análisis Mes a Mes de Requerimientos")

    if st.button("⬅️ Volver al Dashboard Principal"):
        st.session_state.view = 'dashboard'
        st.rerun()

    st.markdown("""
    <div style="margin: 1rem 0;">
        <p style="color: var(--text-secondary); font-size: 1rem;">
            Este análisis te permite ver la evolución de los requerimientos agrupados por rangos numéricos,
            que típicamente corresponden a meses diferentes. También muestra el desfase entre la creación del
            requerimiento y la emisión de la orden de compra.
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    try:
        with sqlite3.connect('compras.db') as conn:
            # Obtener la última sesión
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
            if last_sesion_id_df.empty:
                st.warning("No hay datos disponibles. Por favor, carga datos primero.")
                return

            last_sesion_id = int(last_sesion_id_df.iloc[0, 0])

            # Obtener todos los requerimientos
            df_procesados = pd.read_sql_query("SELECT * FROM procesados WHERE sesion_id = ?", conn, params=(last_sesion_id,))
            df_en_proceso = pd.read_sql_query("SELECT * FROM en_proceso WHERE sesion_id = ?", conn, params=(last_sesion_id,))

            # Combinar y agregar estado
            df_procesados['estado_general'] = 'Procesado'
            df_en_proceso['estado_general'] = 'En Proceso'

            df_todos = pd.concat([df_procesados, df_en_proceso], ignore_index=True)

            if df_todos.empty:
                st.info("No hay datos para analizar.")
                return

            # Convertir numero_req a numérico
            df_todos['numero_req_num'] = pd.to_numeric(df_todos['numero_req'], errors='coerce')
            df_todos = df_todos.dropna(subset=['numero_req_num'])

            if df_todos.empty:
                st.warning("No se pudieron procesar los números de requerimiento.")
                return

            # Determinar rangos automáticamente (grupos de ~200 requerimientos)
            min_req = int(df_todos['numero_req_num'].min())
            max_req = int(df_todos['numero_req_num'].max())

            st.markdown("### 🎯 Configuración de Rangos")
            col1, col2 = st.columns(2)

            with col1:
                tamano_rango = st.number_input(
                    "Tamaño de cada rango (aprox. por mes)",
                    min_value=50,
                    max_value=500,
                    value=200,
                    step=50,
                    help="Ajusta según cuántos requerimientos se crean típicamente por mes"
                )

            with col2:
                st.metric("Rango Total", f"{min_req} - {max_req}", f"{max_req - min_req + 1} requerimientos")

            # Crear rangos
            rangos = []
            inicio = min_req
            while inicio <= max_req:
                fin = min(inicio + tamano_rango - 1, max_req)
                rangos.append((inicio, fin))
                inicio = fin + 1

            # Analizar cada rango
            datos_rangos = []
            for inicio, fin in rangos:
                df_rango = df_todos[(df_todos['numero_req_num'] >= inicio) & (df_todos['numero_req_num'] <= fin)]

                if not df_rango.empty:
                    total = len(df_rango)
                    procesados = len(df_rango[df_rango['estado_general'] == 'Procesado'])
                    en_proceso = len(df_rango[df_rango['estado_general'] == 'En Proceso'])
                    eficiencia = (procesados / total * 100) if total > 0 else 0

                    # Distribución por tipo de compra
                    tipos = df_rango['tipo_compra'].value_counts().to_dict()

                    datos_rangos.append({
                        'Rango': f"{inicio}-{fin}",
                        'Inicio': inicio,
                        'Fin': fin,
                        'Total': total,
                        'Procesados': procesados,
                        'En Proceso': en_proceso,
                        'Eficiencia (%)': round(eficiencia, 1),
                        'Compra Ágil': tipos.get('Compra Ágil', 0),
                        'Convenio Marco': tipos.get('Convenio Marco', 0),
                        'Trato Directo': tipos.get('Trato Directo', 0),
                        'Licitación': tipos.get('Licitación', 0),
                        'Otros': sum([v for k, v in tipos.items() if k not in ['Compra Ágil', 'Convenio Marco', 'Trato Directo', 'Licitación']])
                    })

            df_rangos = pd.DataFrame(datos_rangos)

            # Visualizaciones
            st.divider()
            st.markdown("### 📊 Análisis por Rangos de Requerimientos")

            # Tabla resumen
            with st.expander("📋 Ver Tabla Detallada", expanded=True):
                st.dataframe(
                    df_rangos[['Rango', 'Total', 'Procesados', 'En Proceso', 'Eficiencia (%)',
                              'Compra Ágil', 'Convenio Marco', 'Trato Directo', 'Licitación']],
                    use_container_width=True,
                    hide_index=True
                )

            # Gráficos
            col_g1, col_g2 = st.columns(2)

            with col_g1:
                st.markdown("#### 📈 Evolución de Procesamiento")
                fig_evol = px.bar(
                    df_rangos,
                    x='Rango',
                    y=['Procesados', 'En Proceso'],
                    title='Requerimientos por Rango',
                    labels={'value': 'Cantidad', 'variable': 'Estado'},
                    color_discrete_map={'Procesados': '#10B981', 'En Proceso': '#F59E0B'},
                    barmode='stack'
                )
                fig_evol.update_layout(
                    template="plotly_white",
                    height=400,
                    xaxis_title="Rango de Requerimientos",
                    yaxis_title="Cantidad",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig_evol, use_container_width=True)

            with col_g2:
                st.markdown("#### 🎯 Eficiencia por Rango")
                fig_efic = px.line(
                    df_rangos,
                    x='Rango',
                    y='Eficiencia (%)',
                    title='% Eficiencia de Procesamiento',
                    markers=True
                )
                fig_efic.add_hline(
                    y=85,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Meta: 85%",
                    annotation_position="right"
                )
                fig_efic.update_traces(line_color='#1E40AF', marker=dict(size=10, color='#3B82F6'))
                fig_efic.update_layout(
                    template="plotly_white",
                    height=400,
                    xaxis_title="Rango de Requerimientos",
                    yaxis_title="Eficiencia (%)"
                )
                st.plotly_chart(fig_efic, use_container_width=True)

            # Gráfico de distribución por tipo
            st.markdown("#### 🏢 Distribución por Tipo de Compra por Rango")
            fig_tipos = px.bar(
                df_rangos,
                x='Rango',
                y=['Compra Ágil', 'Convenio Marco', 'Trato Directo', 'Licitación'],
                title='Tipos de Compra por Rango',
                labels={'value': 'Cantidad', 'variable': 'Tipo de Compra'},
                color_discrete_sequence=['#1E40AF', '#3B82F6', '#60A5FA', '#93C5FD'],
                barmode='stack'
            )
            fig_tipos.update_layout(
                template="plotly_white",
                height=400,
                xaxis_title="Rango de Requerimientos",
                yaxis_title="Cantidad",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_tipos, use_container_width=True)

            # Resumen estadístico
            st.divider()
            st.markdown("### 📊 Resumen Estadístico")
            col_s1, col_s2, col_s3, col_s4 = st.columns(4)

            with col_s1:
                st.metric("Total de Rangos Analizados", len(df_rangos))
            with col_s2:
                st.metric("Rango con Mayor Eficiencia", df_rangos.loc[df_rangos['Eficiencia (%)'].idxmax(), 'Rango'])
            with col_s3:
                st.metric("Rango con Más Requerimientos", df_rangos.loc[df_rangos['Total'].idxmax(), 'Rango'])
            with col_s4:
                eficiencia_promedio = df_rangos['Eficiencia (%)'].mean()
                st.metric("Eficiencia Promedio", f"{eficiencia_promedio:.1f}%")

    except Exception as e:
        st.error(f"Error al generar el análisis mensual: {e}")
        st.exception(e)

def main():
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem;">
        <h1 style="background: linear-gradient(135deg, #1E40AF, #3B82F6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 3rem; font-weight: 800; margin-bottom: 0.5rem;">
            📊 Sistema KPI Compras
        </h1>
        <p style="color: #B0B0B0; font-size: 1.2rem; font-weight: 400; margin: 0;">
            Dashboard Ejecutivo de Procesamiento Automático
        </p>
    </div>
    """, unsafe_allow_html=True)
    if 'processor' not in st.session_state:
        st.session_state.processor = ComprasProcessor()
    if st.session_state.get("delete_db_on_next_run", False):
        if os.path.exists('compras.db'):
            os.remove('compras.db')
            st.session_state.clear()
        st.success("✅ Base de datos eliminada. Refresca la página para continuar.")
        st.stop()
    if 'view' not in st.session_state:
        stats = st.session_state.processor.obtener_estadisticas()
        st.session_state.view = 'dashboard' if stats else 'upload'
    
    if st.session_state.view == 'upload':
        pagina_cargar_datos()
    elif st.session_state.view == 'config':
        pagina_configuracion()
    elif st.session_state.view == 'compras_agiles':
        pagina_compras_agiles()
    elif st.session_state.view == 'analisis_mensual':
        pagina_analisis_mensual()
    else: # 'dashboard'
        stats = st.session_state.processor.obtener_estadisticas()
        if stats is None:
            st.session_state.view = 'upload'
            st.rerun()
        else:
            mostrar_dashboard(stats)

if __name__ == "__main__":
    main()