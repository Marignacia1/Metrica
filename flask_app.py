# flask_app.py

from flask import Flask, render_template, request, redirect, url_for, session, flash, send_from_directory, jsonify, Response
import os
import json
from functools import wraps
import pandas as pd
import plotly
import plotly.express as px
import sqlite3
import re
import time
from queue import Queue

# --- Importaciones de Nuestros Módulos ---
from auth import AuthManager
from data_processor import ComprasProcessor, leer_archivo, obtener_datos_sesion
from licitaciones_manager import LicitacionesManager
from init_db import crear_tablas_iniciales

# Importar generador de PDF de licitaciones (si existe)
try:
    from generador_licitaciones_pdf import generar_pdf_licitacion
    PDF_LICITACIONES_DISPONIBLE = True
except ImportError:
    PDF_LICITACIONES_DISPONIBLE = False
    def generar_pdf_licitacion(*args, **kwargs):
        return None
# ==============================================================================
# --- 1. CONFIGURACIÓN E INICIALIZACIÓN ---
# ==============================================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

crear_tablas_iniciales()
auth_manager = AuthManager()

# --- Gestor de Notificaciones en Tiempo Real ---
class NotificationManager:
    def __init__(self):
        self.listeners = []

    def listen(self):
        q = Queue(maxsize=5)
        self.listeners.append(q)
        return q

    def broadcast(self, msg):
        message = f"data: {json.dumps(msg)}\n\n"
        for i in reversed(range(len(self.listeners))):
            try:
                self.listeners[i].put_nowait(message)
            except Queue.Full:
                del self.listeners[i]

notification_manager = NotificationManager()

# ==============================================================================
# --- 2. DECORADORES Y AUTENTICACIÓN ---
# ==============================================================================

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'autenticado' not in session:
            flash('Debes iniciar sesión para ver esta página.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def permission_required(permiso):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('usuario', {}).get('permisos', {}).get(permiso, False):
                flash('No tienes los permisos necesarios para acceder a esta sección.', 'danger')
                return redirect(url_for('dashboard_general'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'autenticado' in session: return redirect(url_for('dashboard_general'))
    if request.method == 'POST':
        usuario = auth_manager.login(request.form.get('username'), request.form.get('password'))
        if usuario:
            session['autenticado'] = True; session['usuario'] = usuario
            flash(f"¡Bienvenido, {usuario['nombre_completo']}!", 'success')
            return redirect(url_for('dashboard_general'))
        else:
            flash('Usuario o contraseña incorrectos.', 'error')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('Has cerrado sesión exitosamente.', 'info')
    return redirect(url_for('login'))

# ==============================================================================
# --- 4. FUNCIONES AUXILIARES ---
# ==============================================================================

def generar_graficos_financieros(df_financiero):
    """
    Genera todos los gráficos financieros a partir de un DataFrame con datos de oc_financiero
    """
    try:
        graficos_financieros = {}

        print(f"DEBUG generar_graficos - Columnas recibidas: {df_financiero.columns.tolist()}")

        # Determinar si las columnas vienen de la BD (minúsculas) o del procesamiento (con espacios)
        # Renombrar columnas al formato esperado por los gráficos
        columnas_a_renombrar = {}

        # Mapeo de posibles nombres a nombres estándar
        if 'total_oc' in df_financiero.columns:
            columnas_a_renombrar['total_oc'] = 'Total OC'
        if 'estado_oc' in df_financiero.columns:
            columnas_a_renombrar['estado_oc'] = 'Estado OC'
        # La columna unidad ya debe venir con el nombre correcto

        df_validas = df_financiero.rename(columns=columnas_a_renombrar)

        print(f"DEBUG generar_graficos - Columnas después del rename: {df_validas.columns.tolist()}")
        print(f"DEBUG generar_graficos - Primeros 3 valores de Total OC: {df_validas['Total OC'].head(3).tolist()}")
        print(f"DEBUG generar_graficos - Tipo de dato Total OC: {df_validas['Total OC'].dtype}")

        # DEBUG: Ver qué estados hay
        if 'Estado OC' in df_validas.columns:
            estados_unicos = df_validas['Estado OC'].value_counts()
            print(f"DEBUG generar_graficos - Estados disponibles y sus cantidades:")
            for estado, cantidad in estados_unicos.items():
                monto_estado = df_validas[df_validas['Estado OC'] == estado]['Total OC'].sum()
                print(f"  - '{estado}': {cantidad} OCs, Monto: ${monto_estado:,.0f}")

        # Calcular KPIs financieros
        monto_total = df_validas['Total OC'].sum()

        # BUSCAR estado conforme de forma más flexible
        estados_conformes = ['Recepción Conforme', 'Aceptada']
        if 'Estado OC' in df_validas.columns:
            monto_conforme = df_validas[df_validas['Estado OC'].isin(estados_conformes)]['Total OC'].sum()
        else:
            monto_conforme = 0

        efectividad = (monto_conforme / monto_total * 100) if monto_total > 0 else 0

        print(f"DEBUG generar_graficos - Monto total calculado: ${monto_total:,.0f}")
        print(f"DEBUG generar_graficos - Monto conforme (Recepción Conforme + Aceptada): ${monto_conforme:,.0f}")
        print(f"DEBUG generar_graficos - Efectividad: {efectividad:.1f}%")

        kpis_financieros = {
            'monto_total': f"${monto_total:,.0f}",
            'monto_conforme': f"${monto_conforme:,.0f}",
            'efectividad': f"{efectividad:.1f}%"
        }

        # 1. Distribución del Monto por Tipo de Compra (Pie Chart)
        if 'tipo_compra' in df_validas.columns and not df_validas.empty:
            monto_por_tipo = df_validas.groupby('tipo_compra')['Total OC'].sum().reset_index()
            fig_pie_tipo = px.pie(
                monto_por_tipo,
                names='tipo_compra',
                values='Total OC',
                hole=0.4,
                color_discrete_sequence=['#1E40AF', '#3B82F6', '#60A5FA', '#93C5FD', '#DBEAFE']
            )
            fig_pie_tipo.update_traces(
                texttemplate='$%{value:,.0f}<br>(%{percent})',
                textfont_size=11,
                textfont_color='white'
            )
            fig_pie_tipo.update_layout(
                template="plotly_white",
                height=350,
                showlegend=True,
                margin=dict(t=20, b=20, l=20, r=20)
            )
            graficos_financieros['distribucion_monto_tipo'] = fig_pie_tipo.to_html(full_html=False, include_plotlyjs='cdn')

        # 2. Top 5 Unidades por Monto y Tipo (Gráfico Apilado con Totales)
        if 'unidad' in df_validas.columns and not df_validas.empty:
            # Calcular el total por unidad para encontrar las top 5
            total_por_unidad = df_validas.groupby('unidad')['Total OC'].sum()
            top_5_unidades_nombres = total_por_unidad.nlargest(5).index

            # Agrupar por unidad y tipo de compra para los segmentos apilados
            monto_por_unidad_tipo = df_validas.groupby(['unidad', 'tipo_compra'])['Total OC'].sum().reset_index()

            # Filtrar los datos para incluir solo las top 5 unidades
            data_top_5 = monto_por_unidad_tipo[monto_por_unidad_tipo['unidad'].isin(top_5_unidades_nombres)].copy()

            # Añadir el total a cada fila para poder ordenar el gráfico
            data_top_5['total_unidad'] = data_top_5['unidad'].map(total_por_unidad)

            # Crear el gráfico de barras apiladas SIN texto en las barras
            fig_bar_monto = px.bar(
                data_top_5.sort_values('total_unidad', ascending=True),
                y='unidad',
                x='Total OC',
                color='tipo_compra',
                orientation='h',
                labels={'Total OC': 'Monto Total ($)', 'unidad': 'Unidad', 'tipo_compra': 'Tipo de Compra'},
                color_discrete_map={
                    'Compra Ágil': '#1E40AF',
                    'Convenio Marco': '#3B82F6',
                    'Licitación': '#60A5FA',
                    'Convenio de Suministros Vigentes': '#93C5FD',
                    'Trato Directo': '#DBEAFE'
                },
                template="plotly_white"
            )

            # --- LÓGICA MEJORADA PARA AÑADIR LOS TOTALES AL FINAL DE CADA BARRA ---
            # Obtenemos los totales ordenados de la misma forma que el gráfico
            totales_ordenados = total_por_unidad.loc[data_top_5.sort_values('total_unidad', ascending=True)['unidad'].unique()]

            # Añadimos una anotación con el total para cada barra
            for i, (unidad, total) in enumerate(totales_ordenados.items()):
                fig_bar_monto.add_annotation(
                    x=total,
                    y=i,
                    text=f"${total:,.0f}".replace(',', '.'), # Formato con punto de miles
                    showarrow=False,
                    xanchor='left', # Anclar el texto a la izquierda
                    xshift=5,     # Un pequeño espacio desde el final de la barra
                    font=dict(color="#1E3A8A", size=11)
                )
            # --- FIN DE LA LÓGICA MEJORADA ---

            fig_bar_monto.update_layout(
                height=400,
                xaxis_title="Monto Total ($)",
                yaxis_title="",
                margin=dict(t=30, b=40, l=250, r=20),
                yaxis=dict(automargin=True),
                legend_title_text='Tipo de Compra',
                barmode='stack',
                # Extendemos el eje X para dar espacio a las etiquetas de texto
                xaxis=dict(range=[0, totales_ordenados.max() * 1.25])
            )

            # Generar HTML del gráfico
            graficos_financieros['top_unidades_monto'] = fig_bar_monto.to_html(
                    full_html=False,
                    include_plotlyjs='cdn',
                    config={'displayModeBar': False}
                )
        # 3. Top 5 Unidades por Cantidad
        if 'unidad' in df_validas.columns and not df_validas.empty:
            top_unidades_cantidad = df_validas['unidad'].value_counts().nlargest(5).sort_values(ascending=True)
            fig_bar_cantidad = px.bar(
                y=top_unidades_cantidad.index,
                x=top_unidades_cantidad.values,
                orientation='h',
                text=top_unidades_cantidad.values
            )
            fig_bar_cantidad.update_traces(
                marker_color='#3B82F6',
                textposition='outside',
                textfont_color='#1E3A8A'
            )
            fig_bar_cantidad.update_layout(
                template="plotly_white",
                height=350,
                showlegend=False,
                xaxis_title="Cantidad de OCs",
                yaxis_title="",
                margin=dict(t=20, b=20, l=200, r=20)
            )
            # El segundo gráfico NO necesita cargar Plotly de nuevo
            graficos_financieros['top_unidades_cantidad'] = fig_bar_cantidad.to_html(
                full_html=False,
                include_plotlyjs=False,
                config={'displayModeBar': False}
            )

        # 4. Resumen Detallado por Unidad
        tabla_resumen_unidades = None
        if 'unidad' in df_validas.columns and not df_validas.empty:
            resumen_unidades = df_validas.groupby('unidad').agg({
                'Total OC': ['sum', 'count', 'mean']
            }).round(0)

            resumen_unidades.columns = ['Monto Total', 'Cantidad OCs', 'Promedio por OC']
            resumen_unidades['Monto Total'] = resumen_unidades['Monto Total'].apply(lambda x: f"${x:,.0f}")
            resumen_unidades['Promedio por OC'] = resumen_unidades['Promedio por OC'].apply(lambda x: f"${x:,.0f}")
            resumen_unidades['Cantidad OCs'] = resumen_unidades['Cantidad OCs'].astype(int)
            resumen_unidades = resumen_unidades.sort_values('Cantidad OCs', ascending=False)

            tabla_resumen_unidades = resumen_unidades.to_html(
                classes='table table-striped table-hover',
                border=0
            )

        # 5. Distribución por Estado
        tabla_estados = None
        grafico_estados = None
        estados_disponibles = []

        if 'Estado OC' in df_validas.columns and not df_validas.empty:
            estados_df = df_validas.groupby('Estado OC').agg({
                'Estado OC': 'count',
                'Total OC': 'sum'
            }).rename(columns={'Estado OC': 'Cantidad'})
            estados_df['Monto Total'] = estados_df['Total OC'].apply(lambda x: f"${x:,.0f}")
            estados_df = estados_df[['Cantidad', 'Monto Total']].sort_values('Cantidad', ascending=False)

            tabla_estados = estados_df.to_html(classes='table table-striped table-hover', border=0)

            fig_estados = px.bar(
                estados_df.reset_index(),
                x='Cantidad',
                y='Estado OC',
                orientation='h',
                text='Cantidad',
                color='Cantidad',
                color_continuous_scale='Blues'
            )
            fig_estados.update_traces(textposition='outside')
            fig_estados.update_layout(
                showlegend=False,
                height=400,
                xaxis_title="Cantidad de OCs",
                yaxis_title=None,
                template="plotly_white",
                margin=dict(t=20, b=20, l=180, r=20)
            )
            grafico_estados = fig_estados.to_html(full_html=False, include_plotlyjs='cdn')

            estados_disponibles = ['Todos'] + sorted(df_validas['Estado OC'].dropna().unique().tolist())

        resultado = {
            'kpis_financieros': kpis_financieros,
            'graficos': graficos_financieros,
            'tabla_resumen_unidades': tabla_resumen_unidades,
            'tabla_estados': tabla_estados,
            'grafico_estados': grafico_estados,
            'estados_disponibles': estados_disponibles,
            'df_analisis': df_validas
        }

        print(f"DEBUG generar_graficos - Gráficos generados: {list(graficos_financieros.keys())}")
        print(f"DEBUG generar_graficos - KPIs: {kpis_financieros}")
        print(f"DEBUG generar_graficos - Estados disponibles: {estados_disponibles if estados_disponibles else 'None'}")
        print(f"DEBUG generar_graficos - tabla_resumen_unidades existe: {tabla_resumen_unidades is not None}")
        print(f"DEBUG generar_graficos - top_unidades_monto existe: {'top_unidades_monto' in graficos_financieros}")
        print(f"DEBUG generar_graficos - top_unidades_cantidad existe: {'top_unidades_cantidad' in graficos_financieros}")

        return resultado

    except Exception as e:
        print(f"❌ Error generando gráficos financieros: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ==============================================================================
# --- 5. RUTAS PRINCIPALES ---
# ==============================================================================

@app.route('/', methods=['GET', 'POST'])
@login_required
def dashboard_general():
    ultima_sesion, df_procesados, df_en_proceso = obtener_datos_sesion()
    kpis = {}
    graficos = {}
    tabla_distribucion = []
    df_procesados_html = None
    df_en_proceso_html = None
    analisis_financiero = None

    if ultima_sesion:
        # Calcular KPIs de requerimientos
        total_brutos = ultima_sesion['total_brutos']
        req_procesados = ultima_sesion['req_procesados']
        req_en_proceso = ultima_sesion['req_en_proceso']
        req_cancelados = ultima_sesion.get('req_cancelados', 0)
        eficiencia = ultima_sesion['eficiencia']
        total_neto = total_brutos - req_cancelados
        eficiencia_operacional = (total_neto / total_brutos * 100) if total_brutos > 0 else 0

        kpis = {
            'total_brutos': total_brutos, 'req_cancelados': req_cancelados,
            'total_neto': total_neto, 'req_procesados': req_procesados,
            'req_en_proceso': req_en_proceso, 'eficiencia': round(eficiencia, 1),
            'eficiencia_operacional': round(eficiencia_operacional, 1)
        }

        # Generar tablas y gráficos de requerimientos
        if df_procesados is not None and df_en_proceso is not None:
            # ... (código para tabla_distribucion, df_procesados_html, etc. se mantiene igual)
            df_proc_copy = df_procesados.copy()
            df_en_proc_copy = df_en_proceso.copy()
            df_proc_copy['estado_calc'] = 'Procesados'
            df_en_proc_copy['estado_calc'] = 'En Proceso'
            df_combinado = pd.concat([df_proc_copy, df_en_proc_copy], ignore_index=True)
            if not df_combinado.empty:
                distribucion = df_combinado.groupby('tipo_compra')['estado_calc'].value_counts().unstack(fill_value=0)
                for tipo, row in distribucion.iterrows():
                    procesados, en_proceso = row.get('Procesados', 0), row.get('En Proceso', 0)
                    cantidad = procesados + en_proceso
                    eficiencia_tipo = (procesados / cantidad * 100) if cantidad > 0 else 0
                    tabla_distribucion.append({
                        'tipo_compra': tipo, 'cantidad': cantidad,
                        'subtipo': f"Procesados: {procesados} | En Proceso: {en_proceso} | Eficiencia: {eficiencia_tipo:.1f}%"
                    })
            if not df_procesados.empty:
                cols = ['numero_req', 'titulo', 'unidad', 'comprador', 'orden_compra']
                df_procesados_html = df_procesados[[c for c in cols if c in df_procesados.columns]].to_html(classes='table table-striped table-hover table-sm', index=False, border=0)
            if not df_en_proceso.empty:
                cols = ['numero_req', 'titulo', 'unidad', 'comprador', 'estado']
                df_en_proceso_html = df_en_proceso[[c for c in cols if c in df_en_proceso.columns]].to_html(classes='table table-striped table-hover table-sm', index=False, border=0)
            if not df_procesados.empty:
                counts = df_procesados['tipo_compra'].value_counts()
                fig_tipos = px.pie(values=counts.values, names=counts.index, hole=0.4, color_discrete_sequence=px.colors.sequential.Blues_r)
                fig_tipos.update_traces(textposition='inside', textinfo='percent+label')
                fig_tipos.update_layout(height=300, showlegend=True, margin=dict(t=0, b=0, l=0, r=0))
                graficos['distribucion_tipos'] = fig_tipos.to_html(full_html=False, include_plotlyjs='cdn')
            if not df_procesados.empty and 'unidad' in df_procesados.columns:
                top_unidades = df_procesados['unidad'].value_counts().head(10).sort_values(ascending=True)
                fig_unidades = px.bar(x=top_unidades.values, y=top_unidades.index, orientation='h', text=top_unidades.values)
                fig_unidades.update_traces(marker_color='#3B82F6', textposition='outside')
                fig_unidades.update_layout(height=350, showlegend=False, xaxis_title="Cantidad", yaxis_title="", margin=dict(t=20, b=20, l=150, r=20))
                graficos['top_unidades'] = fig_unidades.to_html(full_html=False, include_plotlyjs='cdn')

    else:
        if session.get('usuario', {}).get('permisos', {}).get('cargar_archivos', False):
            flash('No hay datos para mostrar. Carga nuevos archivos.', 'info')
            return redirect(url_for('cargar_datos'))
        else:
            flash('Aún no hay datos cargados en el sistema.', 'warning')

    # Cargar análisis financiero existente o procesar nuevo
    if request.method == 'POST':
        archivo_resultado_oc = request.files.get('resultado_oc')
        archivo_experto_historico = request.files.get('experto_historico')
        if archivo_resultado_oc and archivo_experto_historico:
            df_resultado = leer_archivo(archivo_resultado_oc)
            df_experto_hist = leer_archivo(archivo_experto_historico)
            if df_resultado is not None and df_experto_hist is not None:
                processor = ComprasProcessor()
                resultado_analisis = processor.procesar_analisis_financiero(
                    df_resultado, df_experto_hist,
                    sesion_id=ultima_sesion['id'] if ultima_sesion else None,
                    tipo_filtro=None
                )
                for mensaje in resultado_analisis['messages']:
                    flash(mensaje['text'], mensaje['category'])
                if resultado_analisis['success']:
                    analisis_financiero = generar_graficos_financieros(resultado_analisis['df_validas'])
                    analisis_financiero['unmatched_ocs'] = resultado_analisis.get('unmatched_ocs', [])
            else:
                flash('Error al leer los archivos. Verifica el formato.', 'error')
    elif ultima_sesion:
        try:
            with sqlite3.connect('compras.db') as conn:
                query = "SELECT * FROM oc_financiero WHERE sesion_id = ?"
                df_financiero = pd.read_sql_query(query, conn, params=(ultima_sesion['id'],))
            if not df_financiero.empty:
                analisis_financiero = generar_graficos_financieros(df_financiero)
        except Exception as e:
            print(f"Error al cargar análisis financiero: {str(e)}")

    return render_template(
        'dashboard_general.html',
        kpis=kpis,
        graficos=graficos,
        tabla_distribucion=tabla_distribucion,
        analisis_financiero=analisis_financiero,
        df_procesados_html=df_procesados_html,
        df_en_proceso_html=df_en_proceso_html
    )

@app.route('/filtrar-estado', methods=['POST'])
@login_required
def filtrar_por_estado():
    """Ruta para filtrar OCs por estado específico"""
    import sqlite3

    estado_seleccionado = request.form.get('estado_filtro')

    try:
        # Obtener la última sesión
        ultima_sesion, _, _ = obtener_datos_sesion()

        if not ultima_sesion:
            return render_template('fragments/tabla_ocs.html', ocs_filtradas=None, mensaje="No hay sesión activa")

        # Obtener datos financieros de la BD
        with sqlite3.connect('compras.db') as conn:
            query = "SELECT * FROM oc_financiero WHERE sesion_id = ?"
            df_ocs = pd.read_sql_query(query, conn, params=(ultima_sesion['id'],))

        if df_ocs.empty:
            return render_template('fragments/tabla_ocs.html', ocs_filtradas=None, mensaje="No hay datos financieros")

        # Filtrar por estado si no es "Todos"
        if not estado_seleccionado: # Si el usuario seleccionó "-", el valor es vacío
    # No mostramos nada, solo un mensaje
            mensaje = "Selecciona un estado para ver los resultados."
            return render_template('fragments/tabla_ocs.html', ocs_filtradas=None, mensaje=mensaje)

        elif estado_seleccionado == 'Todos':
    # Si es "Todos", mostramos todo
         df_filtrado = df_ocs.copy()
        else:
    # Si es cualquier otro estado, filtramos
         df_filtrado = df_ocs[df_ocs['estado_oc'] == estado_seleccionado].copy()

        # Preparar columnas para mostrar
        columnas_mostrar = ['numero_oc', 'nombre_oc', 'estado_oc', 'total_oc', 'tipo_compra', 'unidad', 'nombre_proveedor']
        columnas_existentes = [col for col in columnas_mostrar if col in df_filtrado.columns]

        if df_filtrado.empty:
            mensaje = f"No se encontraron OCs con estado: {estado_seleccionado}"
            return render_template('fragments/tabla_ocs.html', ocs_filtradas=None, mensaje=mensaje)

        # Formatear el monto
        if 'total_oc' in df_filtrado.columns:
            df_filtrado['total_oc'] = df_filtrado['total_oc'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "$0")

        # Convertir a HTML
        tabla_html = df_filtrado[columnas_existentes].to_html(
            classes='table table-striped table-hover',
            index=False,
            border=0
        )

        return render_template('fragments/tabla_ocs.html',
                             ocs_filtradas=tabla_html,
                             total=len(df_filtrado),
                             estado=estado_seleccionado)

    except Exception as e:
        return render_template('fragments/tabla_ocs.html',
                             ocs_filtradas=None,
                             mensaje=f"Error: {str(e)}")

@app.route('/cargar', methods=['GET', 'POST'])
@login_required
@permission_required('cargar_archivos')
def cargar_datos():
    if request.method == 'POST':
        try:
            # ... (código de lectura de archivos sin cambios) ...
            archivo_experto = request.files.get('experto')
            archivo_cancelados = request.files.get('cancelados')
            archivo_precompra = request.files.get('precompra')

            if not archivo_experto or not archivo_cancelados:
                flash('Debes subir al menos el archivo de experto y cancelados.', 'error')
                return redirect(url_for('cargar_datos'))
            
            df_experto = leer_archivo(archivo_experto)
            df_cancelados = leer_archivo(archivo_cancelados)
            df_precompra = leer_archivo(archivo_precompra) if archivo_precompra else None

            if df_experto is None or df_cancelados is None:
                flash('Error al leer los archivos. Verifica el formato.', 'error')
                return redirect(url_for('cargar_datos'))
            
            processor = ComprasProcessor()
            resultado = processor.procesar_datos(df_experto, df_cancelados, df_precompra)
            
            for mensaje in resultado.get('messages', []):
                flash(mensaje['text'], mensaje['category'])

            if resultado['success']:
                # Enviar notificación a todos los clientes
                notification_manager.broadcast({
                    'user': session['usuario']['nombre_completo'],
                    'page': 'Dashboard General'
                })
                return redirect(url_for('dashboard_general'))

        except Exception as e:
            flash(f'Error al procesar los archivos: {str(e)}', 'error')

    return render_template('cargar_datos.html')


# ==============================================================================
# --- 5. GESTIÓN DE LICITACIONES Y PDF ---
# ==============================================================================

@app.route('/licitaciones-vigentes')
@login_required
@permission_required('modificar_analisis')
def licitaciones_vigentes():
    manager = LicitacionesManager()
    licitaciones = manager.obtener_licitaciones_completas()
    return render_template('licitaciones_vigentes.html', licitaciones=licitaciones)

@app.route('/licitaciones/pdf/<int:licitacion_id>')
@login_required
@permission_required('generar_informes')
def generar_pdf_licitacion_ruta(licitacion_id):
    if not PDF_LICITACIONES_DISPONIBLE:
        flash("Módulo para generar PDFs no está instalado (falta 'reportlab').", "danger")
        return redirect(url_for('licitaciones_vigentes'))
    
    manager = LicitacionesManager()
    todas_las_licitaciones = manager.obtener_licitaciones_completas()
    
    licitacion_encontrada = None
    for lic in todas_las_licitaciones:
        if lic['id'] == licitacion_id:
            licitacion_encontrada = lic
            break

    if not licitacion_encontrada:
        flash("No se encontró la licitación.", "danger")
        return redirect(url_for('licitaciones_vigentes'))

    try:
        # Nombre de archivo seguro
        file_name = f"Ficha_{licitacion_encontrada['id_licitacion']}.pdf".replace('/', '_').replace(' ', '')
        pdf_folder = app.config['UPLOAD_FOLDER']
        pdf_path = os.path.join(pdf_folder, file_name)
        
        # Llamada a la función del generador
        generar_pdf_licitacion(licitacion_encontrada, pdf_path)
        
        if os.path.exists(pdf_path):
            return send_from_directory(directory=pdf_folder, path=file_name, as_attachment=True)
        else:
            flash("Error al generar el archivo PDF.", "danger")
    except Exception as e:
        flash(f"Ocurrió un error al crear el PDF: {e}", "danger")
        import traceback
        traceback.print_exc()
    
    return redirect(url_for('licitaciones_vigentes'))


## ==============================================================================
# --- 6. API ENDPOINTS ---
# ==============================================================================

@app.route('/api/licitacion', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_add_licitacion():
    manager = LicitacionesManager()
    success, message = manager.agregar_licitacion(
        request.form['id_licitacion'], 
        request.form['nombre_licitacion'], 
        request.form['requirente'],
        inspector_tecnico=request.form.get('inspector_tecnico'),
        decreto_adjudicacion=request.form.get('decreto_adjudicacion')
    )
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/licitacion/update', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_update_licitacion():
    manager = LicitacionesManager()
    licitacion_id = request.form.get('licitacion_id')
    datos_actualizados = {
        'id_licitacion': request.form.get('id_licitacion'),
        'nombre_licitacion': request.form.get('nombre_licitacion'),
        'requirente': request.form.get('requirente'),
        'inspector_tecnico': request.form.get('inspector_tecnico'),
        'decreto_adjudicacion': request.form.get('decreto_adjudicacion')
    }
    datos_filtrados = {k: v for k, v in datos_actualizados.items() if v is not None}
    success, message = manager.actualizar_licitacion(licitacion_id, **datos_filtrados)
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/licitacion/delete/<int:licitacion_id>', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_delete_licitacion(licitacion_id):
    manager = LicitacionesManager()
    success, message = manager.eliminar_licitacion(licitacion_id)
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/convenio', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_add_convenio():
    manager = LicitacionesManager()
    campos_adicionales = {
        'fecha_inicio': request.form.get('fecha_inicio'), 'fecha_termino': request.form.get('fecha_termino'),
        'meses': request.form.get('meses'), 'id_gestion_contratos': request.form.get('id_gestion_contratos'),
        'tiene_ipc': request.form.get('tiene_ipc'), 'garantia': request.form.get('garantia'),
        'decreto_aprueba_contrato': request.form.get('decreto_aprueba_contrato'), 'id_mercado_publico': request.form.get('id_mercado_publico'),
    }
    success, message = manager.agregar_convenio(
        request.form['licitacion_id'], request.form['proveedor'],
        request.form['rut_proveedor'], request.form['monto_adjudicado'],
        **campos_adicionales
    )
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/convenio/update', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_update_convenio():
    manager = LicitacionesManager()
    convenio_id = request.form.get('convenio_id')
    datos_actualizados = {
        'proveedor': request.form.get('proveedor'), 'rut_proveedor': request.form.get('rut_proveedor'),
        'monto_adjudicado': request.form.get('monto_adjudicado'), 'meses': request.form.get('meses'),
        'fecha_inicio': request.form.get('fecha_inicio'), 'fecha_termino': request.form.get('fecha_termino'),
        'id_gestion_contratos': request.form.get('id_gestion_contratos'), 'id_mercado_publico': request.form.get('id_mercado_publico'),
        'tiene_ipc': request.form.get('tiene_ipc'), 'garantia': request.form.get('garantia'),
        'decreto_aprueba_contrato': request.form.get('decreto_aprueba_contrato'),
    }
    datos_filtrados = {k: v for k, v in datos_actualizados.items() if v is not None}
    success, message = manager.actualizar_convenio(convenio_id, **datos_filtrados)
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/convenio/delete/<int:convenio_id>', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_delete_convenio(convenio_id):
    manager = LicitacionesManager()
    success, message = manager.eliminar_convenio(convenio_id)
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/oc', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_add_oc():
    manager = LicitacionesManager()
    success, message = manager.agregar_oc(request.form['convenio_id'], request.form['numero_oc'], request.form['monto'])
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/oc/update', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_update_oc():
    manager = LicitacionesManager()
    oc_id = request.form.get('oc_id')
    datos_actualizados = {
        'numero_oc': request.form.get('numero_oc'),
        'monto': request.form.get('monto'),
        'fecha_emision': request.form.get('fecha_emision'),
    }
    datos_filtrados = {k: v for k, v in datos_actualizados.items() if v is not None and v != ''}
    success, message = manager.actualizar_oc(oc_id, **datos_filtrados)
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})

@app.route('/api/oc/delete/<int:oc_id>', methods=['POST'])
@login_required
@permission_required('modificar_analisis')
def api_delete_oc(oc_id):
    manager = LicitacionesManager()
    success, message = manager.eliminar_oc(oc_id)
    flash(message, 'success' if success else 'error')
    return jsonify({'success': success, 'message': message})
# ==============================================================================
# --- 8. RUTAS DE CONFIGURACIÓN Y PLACEHOLDERS ---
# ==============================================================================

@app.route('/configuracion', methods=['GET', 'POST'])
@login_required
@permission_required('ver_menu_admin')
def configuracion():
    if request.method == 'POST':
        try:
            import sqlite3

            print("🔄 Vaciando base de datos...")

            # En lugar de eliminar el archivo, VACIAR todas las tablas EXCEPTO usuarios
            with sqlite3.connect('compras.db') as conn:
                cursor = conn.cursor()

                # Tablas a vaciar (excluir usuarios)
                tablas_a_vaciar = [
                    'sesiones', 'procesados', 'en_proceso',
                    'licitaciones', 'convenios', 'ordenes_compra',
                    'oc_financiero'
                ]

                # Vaciar cada tabla
                for nombre_tabla in tablas_a_vaciar:
                    try:
                        cursor.execute(f"DELETE FROM {nombre_tabla}")
                        print(f"  ✅ Tabla '{nombre_tabla}' vaciada")
                    except sqlite3.OperationalError:
                        print(f"  ⚠️ Tabla '{nombre_tabla}' no existe")

                conn.commit()
                print("✅ Base de datos vaciada correctamente (usuarios preservados)")

            flash("✅ ¡Base de datos vaciada! Todos los datos han sido eliminados. Puedes cargar nuevos datos.", "success")
            return redirect(url_for('cargar_datos'))
        except Exception as e:
            print(f"❌ Error al vaciar BD: {e}")
            import traceback
            traceback.print_exc()
            flash(f"❌ Ocurrió un error al vaciar la base de datos: {e}", "danger")
    return render_template('configuracion.html')

@app.route('/finanzas')
@login_required
def finanzas():
    return render_template('placeholder.html', titulo="💰 Finanzas")

@app.route('/recursos-humanos')
@login_required
def recursos_humanos():
    return render_template('placeholder.html', titulo="👥 Recursos Humanos")

@app.route('/operaciones')
@login_required
def operaciones():
    return render_template('placeholder.html', titulo="⚙️ Operaciones")

@app.route('/compras-agiles', methods=['GET', 'POST'])
@login_required
def compras_agiles():
    ultima_sesion, df_procesados, df_en_proceso = obtener_datos_sesion()
    kpis_iniciales, df_en_proceso_html, df_procesados_html, analisis_financiero = {}, None, None, None

    if ultima_sesion and df_procesados is not None and df_en_proceso is not None:
        df_procesados_ca = df_procesados[df_procesados['tipo_compra'] == 'Compra Ágil']
        df_en_proceso_ca = df_en_proceso[df_en_proceso['tipo_compra'] == 'Compra Ágil']
        total_ca, procesados_ca = len(df_procesados_ca) + len(df_en_proceso_ca), len(df_procesados_ca)
        kpis_iniciales = {
            'total_neto': total_ca, 'procesados': procesados_ca, 'en_proceso': len(df_en_proceso_ca),
            'eficiencia': f"{(procesados_ca / total_ca * 100) if total_ca > 0 else 0:.1f}%"
        }
        if not df_procesados_ca.empty:
            df_procesados_html = df_procesados_ca[[c for c in ['numero_req', 'titulo', 'unidad', 'comprador', 'orden_compra'] if c in df_procesados_ca.columns]].to_html(classes='table table-striped table-hover table-sm', index=False, border=0)
        if not df_en_proceso_ca.empty:
            df_en_proceso_html = df_en_proceso_ca[[c for c in ['numero_req', 'titulo', 'unidad', 'comprador', 'estado'] if c in df_en_proceso_ca.columns]].to_html(classes='table table-striped table-hover table-sm', index=False, border=0)

    if request.method == 'POST' and request.files.get('resultado_oc'):
        try:
            df_resultado = leer_archivo(request.files['resultado_oc'])
            df_experto_hist = leer_archivo(request.files['experto_historico'])
            if df_resultado is not None and df_experto_hist is not None:
                resultado_analisis = ComprasProcessor().procesar_analisis_financiero(df_resultado, df_experto_hist, sesion_id=ultima_sesion['id'] if ultima_sesion else None, tipo_filtro='Compra Ágil')
                for msg in resultado_analisis['messages']: flash(msg['text'], msg['category'])
                if resultado_analisis['success']:
                    analisis_completo = generar_graficos_financieros(resultado_analisis['df_validas'])
                    if analisis_completo:
                        analisis_financiero = {
                            'kpis_financieros': analisis_completo['kpis_financieros'],
                            'grafico_monto_json': analisis_completo['graficos'].get('top_unidades_monto'),
                            'tabla_resumen_unidades': analisis_completo.get('tabla_resumen_unidades')
                        }
            else: flash('Error al leer los archivos.', 'error')
        except Exception as e: flash(f'Error al procesar archivos: {e}', 'error')
    
    elif not analisis_financiero and ultima_sesion:
        try:
            with sqlite3.connect('compras.db') as conn:
                df_financiero_base = pd.read_sql_query("SELECT * FROM oc_financiero WHERE sesion_id = ?", conn, params=(ultima_sesion['id'],))
            if not df_financiero_base.empty:
                df_filtrado_ca = df_financiero_base[df_financiero_base['tipo_compra'] == 'Compra Ágil'].copy()
                if not df_filtrado_ca.empty:
                    analisis_completo = generar_graficos_financieros(df_filtrado_ca)
                    if analisis_completo:
                        analisis_financiero = {
                            'kpis_financieros': analisis_completo['kpis_financieros'],
                            'grafico_monto_json': analisis_completo['graficos'].get('top_unidades_monto'),
                            'tabla_resumen_unidades': analisis_completo.get('tabla_resumen_unidades')
                        }
        except Exception as e: print(f"Error al cargar análisis de CA: {e}")

    return render_template('compras_agiles.html', kpis_iniciales=kpis_iniciales, df_en_proceso=df_en_proceso_html, df_procesados=df_procesados_html, analisis_financiero=analisis_financiero)
def _generar_analisis_licitaciones_desde_df(df_merged):
    # ... (El contenido de esta función no cambia) ...
    if df_merged is None or df_merged.empty: return None
    try:
        required_clean_cols = ['id_adquisicion', 'nombre_adquisicion', 'estado_licitacion', 'responsable']
        if all(col in df_merged.columns for col in required_clean_cols):
            df_proc = df_merged.copy()
        else:
            def find_column_robust(columns, keywords):
                for keyword in keywords:
                    kw_cleaned = keyword.lower().replace(' ', '').replace('_', '').replace('.', '')
                    for col in columns:
                        if kw_cleaned in str(col).lower().replace(' ', '').replace('_', '').replace('.', ''):
                            return col
                return None
            col_map = {
                'id_adquisicion': find_column_robust(df_merged.columns, ['nro de la adquisición', 'id adquisición']),
                'nombre_adquisicion': find_column_robust(df_merged.columns, ['nombre de la adquisición']),
                'estado_licitacion': find_column_robust(df_merged.columns, ['estado licitación']),
                'monto': find_column_robust(df_merged.columns, ['monto total adjudicado', 'monto adjudicado', 'monto total estimado', 'monto']),
                'responsable': find_column_robust(df_merged.columns, ['responsable'])
            }
            missing_cols = [key for key, value in col_map.items() if value is None]
            if missing_cols: return None
            df_proc = df_merged.rename(columns={v: k for k, v in col_map.items()})
        if 'monto' not in df_proc.columns and 'monto_adjudicado' in df_proc.columns:
            df_proc['monto'] = df_proc['monto_adjudicado']
        df_proc['monto'] = pd.to_numeric(df_proc.get('monto', 0), errors='coerce').fillna(0)
        def clasificar_licitacion(id_adquisicion):
            if not isinstance(id_adquisicion, str): return 'ADQ'
            match = re.search(r'-([A-Z]{2,4})\d*$', id_adquisicion)
            if not match: return 'ADQ'
            return 'CSUM' if match.group(1).upper() in ['LR', 'LQ'] else 'ADQ'
        df_proc['tipo_lic'] = df_proc['id_adquisicion'].apply(clasificar_licitacion)
        adjudicadas_df = df_proc[df_proc['estado_licitacion'].str.contains('Adjudicad', case=False, na=False)]
        kpis = { 'total_licitaciones': len(df_proc), 'total_adjudicadas': len(adjudicadas_df), 'monto_adjudicado': f"${adjudicadas_df['monto'].sum():,.0f}".replace(',', '.') }
        kpis_por_tipo = {}
        for tipo in ['CSUM', 'ADQ']:
            df_tipo = df_proc[df_proc['tipo_lic'] == tipo]
            if not df_tipo.empty:
                adj_tipo_df = df_tipo[df_tipo['estado_licitacion'].str.contains('Adjudicad', case=False, na=False)]
                kpis_por_tipo[tipo] = { 'total': len(df_tipo), 'adjudicadas': len(adj_tipo_df), 'monto_estimado': f"${df_tipo['monto'].sum():,.0f}".replace(',', '.'), 'monto_adjudicado': f"${adj_tipo_df['monto'].sum():,.0f}".replace(',', '.') }
        responsables_df = df_proc.groupby('responsable').agg(Cantidad_Licitaciones=('id_adquisicion', 'count'), Monto_Total=('monto', 'sum')).sort_values(by='Monto_Total', ascending=False).reset_index()
        total_monto = responsables_df['Monto_Total'].sum()
        responsables_df['Monto_Total'] = responsables_df['Monto_Total'].apply(lambda x: f"${x:,.0f}".replace(',', '.'))
        total_row = pd.DataFrame([{'Responsable': '<strong>TOTAL GENERAL</strong>', 'Cantidad_Licitaciones': '', 'Monto_Total': f"<strong>${total_monto:,.0f}".replace(',', '.') + "</strong>"}])
        tabla_responsables_html = pd.concat([responsables_df.rename(columns={'responsable': 'Responsable'}), total_row], ignore_index=True).to_html(classes='table table-striped table-sm', index=False, border=0, escape=False, table_id='tabla-responsables')
        estados_finalizados = ['Adjudicada', 'Terminada', 'Desierta', 'Revocada', 'Cerrada']
        en_proceso_df = df_proc[~df_proc['estado_licitacion'].str.contains('|'.join(estados_finalizados), case=False, na=True)]
        df_en_proceso_html = en_proceso_df[['id_adquisicion', 'nombre_adquisicion', 'estado_licitacion', 'responsable']].rename(columns={'id_adquisicion': 'ID Adquisición', 'nombre_adquisicion': 'Nombre De La Adquisición', 'estado_licitacion': 'Estado Licitación', 'responsable': 'Responsable'}).to_html(classes='table table-striped table-sm', index=False, border=0, table_id='tabla-en-proceso') if not en_proceso_df.empty else None
        fig = px.pie(df_proc, names='estado_licitacion', title='Distribución por Estado', color='estado_licitacion', color_discrete_map={'Adjudicada': '#198754', 'Guardada': '#0d6efd'}, hole=0.4)
        fig.update_traces(textinfo='value+percent', texttemplate='%{value} (%{percent})', hovertemplate='<b>%{label}</b><br>Cantidad: %{value}<br>Porcentaje: %{percent}<extra></extra>')
        fig.update_layout(legend_title_text='Estados', margin=dict(t=40, b=20, l=20, r=20))
        grafico_json = fig.to_json()
        df_proc['monto_formateado'] = df_proc['monto'].apply(lambda x: f"${x:,.0f}".replace(',', '.'))
        detalle_por_tipo = {}
        for tipo in ['CSUM', 'ADQ']:
            df_tipo_filtrado = df_proc[df_proc['tipo_lic'] == tipo]
            if not df_tipo_filtrado.empty:
                cols_existentes = [col for col in ['id_adquisicion', 'nombre_adquisicion', 'estado_licitacion', 'monto_formateado'] if col in df_tipo_filtrado.columns]
                df_detalle = df_tipo_filtrado[cols_existentes].rename(columns={'id_adquisicion': 'ID Adquisición', 'nombre_adquisicion': 'Nombre', 'estado_licitacion': 'Estado', 'monto_formateado': 'Monto'})
                detalle_por_tipo[tipo] = df_detalle.to_dict('records')
        return { 'kpis': kpis, 'kpis_por_tipo': kpis_por_tipo, 'tabla_responsables_html': tabla_responsables_html, 'df_en_proceso_html': df_en_proceso_html, 'total_en_proceso': len(en_proceso_df), 'grafico_json': grafico_json, 'df_merged': df_proc, 'detalle_por_tipo': detalle_por_tipo }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return None

@app.route('/licitaciones-analisis', methods=['GET', 'POST'])
@login_required
def licitaciones_analisis():
    analisis = None
    kpis_requerimientos = {}
    last_sesion_id_df = pd.DataFrame()

    try:
        with sqlite3.connect('compras.db') as conn:
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn)
            if not last_sesion_id_df.empty:
                last_sesion_id = int(last_sesion_id_df.iloc[0, 0])
                df_procesados_lic = pd.read_sql_query("SELECT * FROM procesados WHERE sesion_id = ? AND tipo_compra = 'Licitación'", conn, params=(last_sesion_id,))
                df_en_proceso_lic = pd.read_sql_query("SELECT * FROM en_proceso WHERE sesion_id = ? AND tipo_compra = 'Licitación'", conn, params=(last_sesion_id,))
                total_procesados, total_en_proceso = len(df_procesados_lic), len(df_en_proceso_lic)
                total_neto = total_procesados + total_en_proceso
                kpis_requerimientos = {
                    'total_neto': total_neto, 'procesados': total_procesados,
                    'en_proceso': total_en_proceso, 'eficiencia': f"{(total_procesados / total_neto * 100) if total_neto > 0 else 0:.1f}%"
                }
    except Exception as e:
        print(f"Advertencia al cargar KPIs de requerimientos: {e}")

    if request.method == 'POST':
        try:
            df_resultados = leer_archivo(request.files.get('resultados_lic'))
            df_seguimiento = leer_archivo(request.files.get('seguimiento_lic'))

            if df_resultados is not None and df_seguimiento is not None:
                
                def find_flexible(cols, keywords):
                    for kw in keywords:
                        kw_cleaned = kw.lower().replace(' ', '').replace('_', '').replace('.', '')
                        for col in cols:
                            col_cleaned = str(col).lower().replace(' ', '').replace('_', '').replace('.', '')
                            if kw_cleaned in col_cleaned:
                                return col
                    return None

                col_map = {
                    'codigo_res': find_flexible(df_resultados.columns, ['nro de la adquisición', 'id adquisición']),
                    'codigo_seg': find_flexible(df_seguimiento.columns, ['id mercado público', 'id licitacion', 'id']),
                }
               
                if not all(col_map.values()):
                    missing = [k for k, v in col_map.items() if v is None]
                    flash(f"Error: No se encontraron las columnas de ID para la fusión: {', '.join(missing)}.", 'danger')
                    return render_template('licitaciones_analisis.html', kpis_requerimientos=kpis_requerimientos, analisis=None)

                df_merged = pd.merge(df_resultados, df_seguimiento, left_on=col_map['codigo_res'], right_on=col_map['codigo_seg'], how='left', suffixes=('_res', '_seg'))
                analisis = _generar_analisis_licitaciones_desde_df(df_merged)

                if analisis and not last_sesion_id_df.empty:
                    sesion_id = int(last_sesion_id_df.iloc[0, 0])
                    df_to_save = analisis['df_merged'].copy()
                    df_to_save['sesion_id'] = sesion_id
                    
                    cols_bd = {
                        'id_adquisicion': 'id_adquisicion', 
                        'nombre_adquisicion': 'nombre_adquisicion', 
                        'estado_licitacion': 'estado_licitacion', 
                        'monto': 'monto_adjudicado', 
                        'responsable': 'responsable', 
                        'tipo_lic': 'tipo_licitacion'
                    }
                    cols_a_guardar = {k: v for k, v in cols_bd.items() if k in df_to_save.columns}
                    df_to_save = df_to_save[list(cols_a_guardar.keys())].rename(columns=cols_a_guardar)

                    with sqlite3.connect('compras.db') as conn:
                        conn.execute("DELETE FROM analisis_licitaciones WHERE sesion_id = ?", (sesion_id,))
                        df_to_save.to_sql('analisis_licitaciones', conn, if_exists='append', index=False)
                    flash('✅ Análisis completado y guardado exitosamente', 'success')

                    # Enviar notificación a todos los clientes
                    notification_manager.broadcast({
                        'user': session['usuario']['nombre_completo'],
                        'page': 'Análisis Licitaciones'
                    })

                elif not analisis:
                     flash('❌ Error al procesar los datos después de la fusión.', 'danger')
            else:
                flash('❌ Error al leer los archivos', 'error')
        except Exception as e:
            flash(f'❌ Error al procesar archivos: {str(e)}', 'danger')
            import traceback
            traceback.print_exc()

    if not analisis and not last_sesion_id_df.empty:
        try:
            sesion_id = int(last_sesion_id_df.iloc[0, 0])
            with sqlite3.connect('compras.db') as conn:
                df_guardado = pd.read_sql_query("SELECT * FROM analisis_licitaciones WHERE sesion_id = ?", conn, params=(sesion_id,))
            
            if not df_guardado.empty:
                print("INFO: Cargando análisis de licitaciones guardado desde la base de datos.")
                # Renombramos solo las columnas necesarias para que coincidan con la lógica interna de la función
                df_guardado.rename(columns={'monto_adjudicado': 'monto', 'tipo_licitacion': 'tipo_lic'}, inplace=True)
                analisis = _generar_analisis_licitaciones_desde_df(df_guardado)
        except Exception as e:
            print(f"No se pudo cargar el análisis guardado: {e}")

    return render_template('licitaciones_analisis.html', kpis_requerimientos=kpis_requerimientos, analisis=analisis)

# ==============================================================================
# --- 7. RUTAS DE ESTADO Y NOTIFICACIONES ---
# ==============================================================================
@app.route('/stream')
def stream():
    def event_stream():
        messages = notification_manager.listen()
        while True:
            msg = messages.get()
            yield msg
    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/api/last-update')
@login_required
def last_update():
    """
    Devuelve la fecha y hora de la última sesión cargada.
    Esto permite al frontend comprobar si los datos han cambiado.
    """
    try:
        with sqlite3.connect('compras.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT fecha FROM sesiones ORDER BY id DESC LIMIT 1")
            ultima_fecha = cursor.fetchone()
            if ultima_fecha:
                return jsonify({'last_update': ultima_fecha[0]})
            else:
                # Si no hay sesiones, devuelve una fecha muy antigua
                return jsonify({'last_update': '2000-01-01 00:00:00'})
    except Exception as e:
        print(f"Error al obtener última actualización: {e}")
        return jsonify({'error': str(e)}), 500

# ==============================================================================
# --- 9. EJECUCIÓN DE LA APLICACIÓN ---
# ==============================================================================
if __name__ == '__main__':
    app.run(debug=True, port=5001)