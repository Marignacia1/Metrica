import pandas as pd
import sqlite3
import plotly.express as px
import os

def leer_archivo(file_storage):
    """Lee archivos Excel o CSV subidos desde Flask."""
    try:
        filename = file_storage.filename
        if filename.endswith('.csv'):
            return pd.read_csv(file_storage)
        elif filename.endswith(('.xls', '.xlsx')):
            return pd.read_excel(file_storage)
    except Exception as e:
        print(f"Error leyendo archivo: {e}")
    return None

def obtener_datos_sesion():
    """Recupera la última sesión y sus datos de la BD."""
    try:
        with sqlite3.connect('compras.db') as conn:
            # Obtener ID de la última sesión
            cursor = conn.cursor()
            cursor.execute("SELECT id, fecha FROM sesiones ORDER BY fecha DESC LIMIT 1")
            row = cursor.fetchone()
            
            if not row:
                return None, None, None
            
            ultima_sesion = {'id': row[0], 'fecha': row[1]}
            
            # Cargar DataFrames
            df_procesados = pd.read_sql_query("SELECT * FROM procesados WHERE sesion_id = ?", conn, params=(row[0],))
            df_en_proceso = pd.read_sql_query("SELECT * FROM en_proceso WHERE sesion_id = ?", conn, params=(row[0],))
            
            return ultima_sesion, df_procesados, df_en_proceso
    except Exception as e:
        print(f"Error base de datos: {e}")
        return None, None, None

def generar_graficos_financieros(df):
    """Genera diccionarios para KPIs y Gráficos."""
    if df.empty:
        return None
    
    # KPIs Básicos
    total_monto = df['total_oc'].sum()
    kpis = {
        'total_gasto': f"${total_monto:,.0f}",
        'cantidad_ocs': len(df)
    }
    
    # Gráfico Top Unidades (Monto)
    top_unidades = df.groupby('unidad')['total_oc'].sum().sort_values(ascending=True).tail(10).reset_index()
    fig = px.bar(top_unidades, x='total_oc', y='unidad', orientation='h', title="Top Unidades por Monto")
    
    return {
        'kpis_financieros': kpis,
        'graficos': {'top_unidades_monto': fig.to_json()},
        'tabla_resumen_unidades': top_unidades.to_dict('records')
    }

def generar_pdf_licitacion(licitacion, path):
    """Placeholder para generación de PDF."""
    from reportlab.pdfgen import canvas
    c = canvas.Canvas(path)
    c.drawString(100, 750, f"Ficha de Licitación: {licitacion.get('nombre_licitacion', 'N/A')}")
    c.drawString(100, 730, f"ID: {licitacion.get('id_licitacion', 'N/A')}")
    c.save()