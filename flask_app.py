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
from flask_mail import Mail, Message

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

# --- CONFIGURACIÓN DE CORREO ---
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USERNAME'] = 'tu-correo@gmail.com'  
app.config['MAIL_PASSWORD'] = 'tu-contraseña-de-app'
mail = Mail(app)

# --- Inicialización de Módulos ---
crear_tablas_iniciales()
# ¡Importante! Pasa la 'secret_key' de la app al AuthManager
auth_manager = AuthManager(secret_key=app.config['SECRET_KEY'])

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
        
        # --- LÓGICA DE CAMBIO FORZADO ---
        if session['usuario'].get('force_password_change'):
            # Lista de páginas permitidas si NO has cambiado la contraseña
            allowed_endpoints = [
                'cambiar_password_forzado', 
                'logout',
                'solicitar_recuperacion',
                'reset_password_with_token'
            ]
            
            # Si intentas ir a cualquier otra página, te redirige
            if request.endpoint not in allowed_endpoints:
                flash('Debes cambiar tu contraseña genérica para continuar.', 'warning')
                return redirect(url_for('cambiar_password_forzado'))
                
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
    if 'autenticado' in session: 
        return redirect(url_for('dashboard_general'))
    
    if request.method == 'POST':
        usuario = auth_manager.login(request.form.get('username'), request.form.get('password'))
        
        if usuario:
            session['autenticado'] = True
            session['usuario'] = usuario  # La sesión ahora incluye el flag
            
            # --- ¡NUEVA REDIRECCIÓN! ---
            if usuario.get('force_password_change'):
                flash('Tu contraseña es genérica. Debes cambiarla antes de continuar.', 'warning')
                return redirect(url_for('cambiar_password_forzado'))
            
            # Si es False, login normal.
            flash(f"¡Bienvenido, {usuario['nombre_completo']}!", 'success')
            return redirect(url_for('dashboard_general'))
        else:
            flash('Usuario o contraseña incorrectos.', 'error')
            
    return render_template('login.html')

@app.route('/cambiar-password-forzado', methods=['GET', 'POST'])
@login_required 
def cambiar_password_forzado():
    # Si el usuario ya no necesita cambiarla, lo sacamos de aquí
    if not session['usuario'].get('force_password_change'):
        return redirect(url_for('dashboard_general'))

    if request.method == 'POST':
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if not new_password or not confirm_password:
            flash('Debes completar ambos campos.', 'danger')
            return render_template('cambiar_password.html')

        if new_password != confirm_password:
            flash('Las contraseñas no coinciden. Inténtalo de nuevo.', 'danger')
            return render_template('cambiar_password.html')

        if len(new_password) < 8:
            flash('La contraseña debe tener al menos 8 caracteres.', 'danger')
            return render_template('cambiar_password.html')

        success, message = auth_manager.change_password(
            session['usuario']['username'], 
            new_password
        )

        if success:
            session['usuario']['force_password_change'] = False
            session.modified = True 
            
            flash('¡Contraseña actualizada exitosamente! Ya puedes usar el sistema.', 'success')
            return redirect(url_for('dashboard_general'))
        else:
            flash(f'Error al actualizar la contraseña: {message}', 'danger')

    return render_template('cambiar_password.html')


@app.route('/admin/reset-password', methods=['POST'])
@login_required
@permission_required('ver_menu_admin') # Solo admins
def admin_reset_password():
    user_to_reset = request.form.get('user_to_reset')
    new_generic_password = request.form.get('new_generic_password')
    current_admin = session['usuario']['username']

    if not user_to_reset or not new_generic_password:
        flash('Debes seleccionar un usuario y escribir una contraseña.', 'danger')
        return redirect(url_for('configuracion'))

    if user_to_reset == current_admin:
        flash('No puedes reiniciar tu propia contraseña desde este panel.', 'warning')
        return redirect(url_for('configuracion'))

    success, message = auth_manager.admin_reset_password(user_to_reset, new_generic_password)

    if success:
        flash(f"¡Éxito! {message}. El usuario deberá cambiarla al iniciar sesión.", 'success')
    else:
        flash(f"Error: {message}", 'danger')
    
    return redirect(url_for('configuracion'))

@app.route('/logout')
def logout():
    session.clear()
    flash('Has cerrado sesión exitosamente.', 'info')
    return redirect(url_for('login'))

# ==============================================================================
# --- 4. FUNCIONES AUXILIARES ---
# ==============================================================================

def generar_graficos_financieros(df_financiero_completo):
    """
    Genera todos los gráficos financieros a partir de un DataFrame con datos de oc_financiero.
    CORREGIDO: Filtro insensible a mayúsculas/minúsculas para incluir correctamente todos los estados.
    """
    try:
        graficos_financieros = {}

        # --- INICIO DE LA MODIFICACIÓN ---
        # Renombrar columnas al formato esperado por los gráficos
        columnas_a_renombrar = {}
        if 'total_oc' in df_financiero_completo.columns:
            columnas_a_renombrar['total_oc'] = 'Total OC'
        if 'estado_oc' in df_financiero_completo.columns:
            columnas_a_renombrar['estado_oc'] = 'Estado OC'
        
        df_renamed = df_financiero_completo.rename(columns=columnas_a_renombrar)

        print(f"DEBUG generar_graficos - Columnas después del rename: {df_renamed.columns.tolist()}")

        # 1. (NUEVO ORDEN) Generar Distribución por Estado (CON TODOS LOS DATOS)
        tabla_estados = None
        grafico_estados = None
        estados_disponibles = []

        if 'Estado OC' in df_renamed.columns and not df_renamed.empty:
            estados_df = df_renamed.groupby('Estado OC').agg({
                'Estado OC': 'count',
                'Total OC': 'sum'
            }).rename(columns={'Estado OC': 'Cantidad'})
            estados_df['Monto Total'] = estados_df['Total OC'].apply(lambda x: f"${x:,.0f}")
            estados_df = estados_df[['Cantidad', 'Monto Total']].sort_values('Cantidad', ascending=False)
            estados_df.reset_index(inplace=True) 
            
            tabla_estados = estados_df.to_html(
                classes='table table-striped table-hover', 
                border=0,
                index=False,
                table_id='tabla-resumen-estados'
            )

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

            estados_disponibles = ['Todos'] + sorted(df_renamed['Estado OC'].dropna().unique().tolist())
        
        # 2. (NUEVO) Aplicar el filtro para los KPIs y gráficos restantes
        
        # ----- ¡LÍNEA ACTUALIZADA! -----
        # Definimos la lista de estados que SÍ cuentan como "Válidos" (en minúsculas para normalizar)
        estados_validos_usuario_lower = [
            'recepción conforme', 
            'aceptada', 
            'guardada', 
            'enviada a proveedor', 
            'enviada a autorizar', # Ahora capturará "Enviada a Autorizar" independientemente del Match
            'en proceso'
        ]
        
        if 'Estado OC' in df_renamed.columns:
            # Creamos una copia temporal para normalizar el estado a minúsculas
            df_temp = df_renamed.copy()
            df_temp['estado_temp_lower'] = df_temp['Estado OC'].astype(str).str.strip().str.lower()
            
            # Filtramos el DF para que SOLO contenga los estados válidos (usando la columna normalizada)
            df_validas = df_temp[df_temp['estado_temp_lower'].isin(estados_validos_usuario_lower)].copy()
            
            # Eliminamos la columna temporal para no ensuciar
            df_validas.drop(columns=['estado_temp_lower'], inplace=True)
        else:
            # Si no hay columna de estado, no podemos filtrar.
            df_validas = df_renamed.copy()
        
        print(f"DEBUG generar_graficos - Total OCs: {len(df_renamed)}, OCs Válidas (filtradas POR INCLUSIÓN): {len(df_validas)}")

        
        # 3. (NUEVO) Calcular KPIs financieros
        
        # Monto Total Bruto (incluye todo, usa df_renamed)
        monto_total_bruto = df_renamed['Total OC'].sum()
        
        # Monto Total Válido (usa el df_validas filtrado correctamente)
        monto_total_valido = df_validas['Total OC'].sum()
        
        # Monto Conforme (solo 'Recepción Conforme' y 'Aceptada')
        estados_conformes_lower = ['recepción conforme', 'aceptada']
        
        if 'Estado OC' in df_validas.columns:
            # Usamos la misma lógica de normalización temporal para verificar "Conforme"
            mask_conforme = df_validas['Estado OC'].astype(str).str.strip().str.lower().isin(estados_conformes_lower)
            monto_conforme = df_validas[mask_conforme]['Total OC'].sum()
        else:
            monto_conforme = 0
        
        # Efectividad (Conforme vs. Válido)
        efectividad = (monto_conforme / monto_total_valido * 100) if monto_total_valido > 0 else 0

        print(f"DEBUG generar_graficos - Monto Bruto (total): ${monto_total_bruto:,.0f}")
        print(f"DEBUG generar_graficos - Monto Válido (filtrado): ${monto_total_valido:,.0f}")
        print(f"DEBUG generar_graficos - Monto Conforme: ${monto_conforme:,.0f}")

        kpis_financieros = {
            'monto_total_bruto': f"${monto_total_bruto:,.0f}",
            'monto_total_valido': f"${monto_total_valido:,.0f}",
            'monto_conforme': f"${monto_conforme:,.0f}",
            'efectividad': f"{efectividad:.1f}%"
        }
        
        # 4. (ANTIGUO ORDEN) Generar otros gráficos (USANDO DATOS FILTRADOS - df_validas)
        
        # 4.1. Distribución del Monto por Tipo de Compra (Pie Chart)
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

        # 4.2. Top 5 Unidades por Monto y Tipo (Gráfico Apilado con Totales)
        if 'unidad' in df_validas.columns and not df_validas.empty:
            # Calcular el total por unidad para encontrar las top 5
            total_por_unidad = df_validas.groupby('unidad')['Total OC'].sum()
            top_5_unidades_nombres = total_por_unidad.nlargest(5).index

           
            monto_por_unidad_tipo = df_validas.groupby(['unidad', 'tipo_compra'])['Total OC'].sum().reset_index()

           
            data_top_5 = monto_por_unidad_tipo[monto_por_unidad_tipo['unidad'].isin(top_5_unidades_nombres)].copy()

            data_top_5['total_unidad'] = data_top_5['unidad'].map(total_por_unidad)

          
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
                template="plotly_white",
                text='Total OC'  
            )

      
            fig_bar_monto.update_traces(
                texttemplate='$%{text:,.0f}', 
                textposition='inside',
                textfont=dict(color='white', size=10) 
            )


            fig_bar_monto.update_layout(
                height=400,
                xaxis_title="Monto Total ($)",
                yaxis_title="",
                margin=dict(t=30, b=40, l=250, r=20),
                yaxis=dict(automargin=True),
                legend_title_text='Tipo de Compra',
                barmode='stack')

            graficos_financieros['top_unidades_monto'] = fig_bar_monto.to_html(
                    full_html=False,
                    include_plotlyjs='cdn',
                    config={'displayModeBar': False}
                )
           
        # 4.3. Top 5 Unidades por Cantidad
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
            graficos_financieros['top_unidades_cantidad'] = fig_bar_cantidad.to_html(
                full_html=False,
                include_plotlyjs=False,
                config={'displayModeBar': False}
            )


        # 4.4. Resumen Detallado por Unidad
        tabla_resumen_unidades = None
        if 'unidad' in df_validas.columns and not df_validas.empty:
            resumen_unidades = df_validas.groupby('unidad').agg({
                'Total OC': ['sum', 'count']
            }).round(0)

            resumen_unidades.columns = ['Monto Total', 'Cantidad OCs']
            resumen_unidades['Monto Total'] = resumen_unidades['Monto Total'].apply(lambda x: f"${x:,.0f}")
            resumen_unidades['Cantidad OCs'] = resumen_unidades['Cantidad OCs'].astype(int)
            resumen_unidades = resumen_unidades.sort_values('Cantidad OCs', ascending=False)
            resumen_unidades.reset_index(inplace=True)
            resumen_unidades.rename(columns={'unidad': 'Unidad'}, inplace=True)

            tabla_resumen_unidades = resumen_unidades.to_html(
                classes='table table-striped table-hover',
                border=0,
                index=False,
                table_id='tabla-resumen-unidades'
         )

        # 5. (NUEVO) Ensamblar el resultado final
        resultado = {
            'kpis_financieros': kpis_financieros,
            'graficos': graficos_financieros,
            'tabla_resumen_unidades': tabla_resumen_unidades,
            'tabla_estados': tabla_estados, # <-- Viene de la data completa
            'grafico_estados': grafico_estados, # <-- Viene de la data completa
            'estados_disponibles': estados_disponibles, # <-- Viene de la data completa
            'df_analisis': df_validas # <-- Pasa el df filtrado al template (si es necesario)
        }
        # --- FIN DE LA MODIFICACIÓN ---

        print(f"DEBUG generar_graficos - Gráficos generados: {list(graficos_financieros.keys())}")
        print(f"DEBUG generar_graficos - KPIs (filtrados): {kpis_financieros}")
        print(f"DEBUG generar_graficos - Estados disponibles (completos): {estados_disponibles if estados_disponibles else 'None'}")
        print(f"DEBUG generar_graficos - tabla_resumen_unidades (filtrada) existe: {tabla_resumen_unidades is not None}")
        print(f"DEBUG generar_graficos - top_unidades_monto (filtrado) existe: {'top_unidades_monto' in graficos_financieros}")
        print(f"DEBUG generar_graficos - top_unidades_cantidad (filtrado) existe: {'top_unidades_cantidad' in graficos_financieros}")

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
                cols = ['numero_req', 'titulo', 'unidad', 'comprador', 'estado', 'tipo_financiamiento']
                df_en_proceso_html = df_en_proceso[[c for c in cols if c in df_en_proceso.columns]].to_html(classes='table table-striped table-hover table-sm', index=False, border=0)
            if not df_procesados.empty:
                counts = df_procesados['tipo_compra'].value_counts()
                fig_tipos = px.pie(values=counts.values, names=counts.index, hole=0.4, color_discrete_sequence=px.colors.sequential.Blues_r)
                fig_tipos.update_traces(textposition='inside', textinfo='percent+label')
                fig_tipos.update_layout(height=300, showlegend=True, margin=dict(t=0, b=0, l=0, r=0))
                graficos['distribucion_tipos'] = fig_tipos.to_html(full_html=False, include_plotlyjs='cdn')
            if not df_procesados.empty and 'unidad' in df_procesados.columns:
                top_unidades = df_procesados['unidad'].value_counts().head(10).sort_values(ascending=True)
                
                # --- INICIO DE LA CORRECCIÓN ---
                # Convertir la Serie a un DataFrame para que Plotly lo entienda
                top_unidades_df = top_unidades.reset_index()
                top_unidades_df.columns = ['Unidad', 'Cantidad'] # Renombrar columnas

                # Usar la sintaxis de DataFrame de Plotly (más robusta)
                fig_unidades = px.bar(
                    data_frame=top_unidades_df, 
                    x='Cantidad', 
                    y='Unidad', 
                    orientation='h', 
                    text='Cantidad'
                )
                # --- FIN DE LA CORRECCIÓN ---

                fig_unidades.update_traces(marker_color='#3B82F6', textposition='outside')

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
                    analisis_financiero = generar_graficos_financieros(resultado_analisis['df_analisis'])
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
def send_reset_email(user_email, token):
    """Función helper para enviar el correo."""
    try:
        # _external=True es VITAL para que genere la URL completa
        reset_url = url_for('reset_password_with_token', token=token, _external=True)
        
        msg = Message(
            subject="Restablecimiento de contraseña - Métrica Web",
            sender=app.config['MAIL_USERNAME'],
            recipients=[user_email]
        )
        msg.body = f"""Hola,

Para restablecer tu contraseña, haz clic en el siguiente enlace:
{reset_url}

Si no solicitaste esto, ignora este mensaje. El enlace expirará en 1 hora.

Saludos,
Equipo de Métrica Web
"""
        mail.send(msg)
        return True
    except Exception as e:
        print(f"Error al enviar email: {e}")
        return False


@app.route('/solicitar-recuperacion', methods=['GET', 'POST'])
def solicitar_recuperacion():
    # Si ya está logueado, lo mandamos al dashboard
    if 'autenticado' in session:
        return redirect(url_for('dashboard_general'))

    if request.method == 'POST':
        email = request.form.get('email')
        
        # Buscamos al usuario por email
        username, user_details = auth_manager.find_user_by_email(email)
        
        if user_details:
            # Si existe, generamos token y enviamos email
            token = auth_manager.generate_reset_token(email)
            send_reset_email(email, token)

        # ¡Importante! Mostramos el mismo mensaje aunque el email no exista.
        # Esto evita que alguien pueda adivinar qué correos están registrados.
        flash('Si tu correo está en nuestro sistema, recibirás un enlace para reiniciar tu contraseña.', 'info')
        return redirect(url_for('login'))

    return render_template('solicitar_recuperacion.html')


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password_with_token(token):
    if 'autenticado' in session:
        return redirect(url_for('dashboard_general'))

    # Primero, verificamos si el token es válido
    email = auth_manager.verify_reset_token(token)
    if not email:
        flash('El enlace de recuperación es inválido o ha expirado.', 'danger')
        return redirect(url_for('login'))

    if request.method == 'POST':
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if not new_password or not confirm_password:
            flash('Debes completar ambos campos.', 'danger')
            return render_template('reset_password.html', token=token)

        if new_password != confirm_password:
            flash('Las contraseñas no coinciden.', 'danger')
            return render_template('reset_password.html', token=token)
        
        if len(new_password) < 8:
            flash('La contraseña debe tener al menos 8 caracteres.', 'danger')
            return render_template('reset_password.html', token=token)

        # Buscamos el 'username' asociado a ese email
        username, user_details = auth_manager.find_user_by_email(email)
        if not username:
             flash('Error: No se pudo encontrar el usuario asociado al email.', 'danger')
             return redirect(url_for('login'))

        # Usamos la función que YA TENÍAMOS para cambiar la contraseña
        # Esta función ya hashea la contraseña y pone 'force_password_change' en false
        success, message = auth_manager.change_password(username, new_password)

        if success:
            flash('¡Contraseña actualizada exitosamente! Ya puedes iniciar sesión.', 'success')
            return redirect(url_for('login'))
        else:
            flash(f'Error al actualizar: {message}', 'danger')
            return render_template('reset_password.html', token=token)

    # Si es GET, solo mostramos la página de reseteo
    return render_template('reset_password.html', token=token)
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
        # Revisa si el POST es para borrar la BD
        if request.form.get('action') == 'delete_db':
            try:
                with sqlite3.connect('compras.db') as conn:
                    cursor = conn.cursor()
                    tablas_a_vaciar = [
                        'sesiones', 'procesados', 'en_proceso',
                        'licitaciones', 'convenios', 'ordenes_compra',
                        'oc_financiero', 'analisis_licitaciones'
                    ]
                    for nombre_tabla in tablas_a_vaciar:
                        try:
                            cursor.execute(f"DELETE FROM {nombre_tabla}")
                        except sqlite3.OperationalError:
                            pass 
                    conn.commit()
                
                flash("✅ ¡Base de datos vaciada! Todos los datos han sido eliminados.", "success")
                return redirect(url_for('cargar_datos'))
            except Exception as e:
                flash(f"❌ Ocurrió un error al vaciar la base de datos: {e}", "danger")
    
    # --- LÓGICA GET ---
    # Obtenemos los usuarios para llenar el dropdown en configuracion.html
    all_user_details = auth_manager.get_all_users_details()
    
    return render_template(
        'configuracion.html', 
        all_users_details=all_user_details
    )

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
            
            cols = ['numero_req', 'titulo', 'unidad', 'comprador', 'estado', 'tipo_financiamiento']
            df_en_proceso_html = df_en_proceso_ca[[c for c in cols if c in df_en_proceso_ca.columns]].to_html(classes='table table-striped table-hover table-sm', index=False, border=0)
    if request.method == 'POST' and request.files.get('resultado_oc'):
        try:
            df_resultado = leer_archivo(request.files['resultado_oc'])
            df_experto_hist = leer_archivo(request.files['experto_historico'])
            if df_resultado is not None and df_experto_hist is not None:
                resultado_analisis = ComprasProcessor().procesar_analisis_financiero(df_resultado, df_experto_hist, sesion_id=ultima_sesion['id'] if ultima_sesion else None, tipo_filtro='Compra Ágil')
                for msg in resultado_analisis['messages']: flash(msg['text'], msg['category'])
                if resultado_analisis['success']:
                    analisis_completo = generar_graficos_financieros(resultado_analisis['df_analisis']) 
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
    """
    Procesa datos de licitaciones de forma robusta.
    """
    if df_merged is None or df_merged.empty: 
        return None

    try:
        df_proc = df_merged.copy()
        
        # --- DEBUG: IMPRIMIR COLUMNAS DETECTADAS ---
        # Mira tu consola/terminal cuando subas el archivo. 
        # Si 'monto_estimado' no aparece o tiene nombre raro, agrégalo al mapa abajo.
        print("COLUMNAS EN EL DATAFRAME RECIBIDO:", df_proc.columns.tolist()) 
        # -------------------------------------------

        # 1. MAPEO INTELIGENTE DE COLUMNAS (Ampliado)
        columnas_map = {
            'id_adquisicion': ['id_adquisicion', 'nro de la adquisición', 'id adquisición', 'id_licitacion', 'id'],
            'nombre_adquisicion': ['nombre_adquisicion', 'nombre de la adquisición', 'nombre', 'descripcion'],
            'estado_licitacion': ['estado_licitacion', 'estado licitación', 'estado', 'status'],
            'responsable': ['responsable', 'comprador', 'encargado', 'analista'],
            'monto_adjudicado': ['monto_adjudicado', 'monto total adjudicado', '$ adjudicado', 'adjudicado', 'valor adjudicado'],
            # Agregamos más sinónimos aquí por si tus Excel usan otros términos:
            'monto_estimado': ['monto_estimado', 'monto total estimado', 'monto estimado', 'estimado', 'presupuesto', 'valor referencial', 'base'],
            'lineas': ['cantidad_lineas', 'lineas licitadas', 'n° lineas', 'lineas'],
            'ofertas': ['cantidad_ofertas', 'ofertas recibidas', 'n° ofertas', 'ofertas']
        }

        # Normalizar nombres de columnas
        for col_destino, posibles_nombres in columnas_map.items():
            if col_destino in df_proc.columns:
                continue
            
            col_encontrada = None
            for candidato in posibles_nombres:
                for col_actual in df_proc.columns:
                    # Busca coincidencias parciales (ej: "Monto Estimado (CLP)")
                    if candidato.lower() in str(col_actual).lower():
                        col_encontrada = col_actual
                        break
                if col_encontrada: break
            
            if col_encontrada:
                df_proc[col_destino] = df_proc[col_encontrada]
            else:
                # Si no existe, rellenamos con 0 o N/A según corresponda
                df_proc[col_destino] = 0 if 'monto' in col_destino or col_destino in ['lineas', 'ofertas'] else 'N/A'

        # 2. LIMPIEZA DE DATOS (Maneja $ , . y textos)
        def limpiar_numero(valor):
            if pd.isna(valor): return 0
            if isinstance(valor, (int, float)): return valor
            try:
                # Limpieza agresiva de caracteres monetarios
                limpio = str(valor).replace('$', '').replace('CLP', '').replace('.', '').replace(',', '.').strip()
                return pd.to_numeric(limpio, errors='coerce') or 0
            except: return 0

        for col in ['monto_adjudicado', 'monto_estimado', 'lineas', 'ofertas']:
            df_proc[col] = df_proc[col].apply(limpiar_numero).fillna(0)

        # 3. LOGICA DE NEGOCIO
        # Monto principal: Si no hay adjudicado, mostramos estimado (opcional, depende de tu regla)
        df_proc['monto'] = df_proc['monto_adjudicado']
        mask_zero = df_proc['monto'] == 0
        df_proc.loc[mask_zero, 'monto'] = df_proc.loc[mask_zero, 'monto_estimado']

        df_proc['monto_formateado'] = df_proc['monto'].apply(lambda x: f"${x:,.0f}".replace(',', '.'))

        # 4. CLASIFICACIÓN DE TIPO
        if 'tipo_licitacion' in df_proc.columns:
             df_proc['tipo_lic'] = df_proc['tipo_licitacion']
        else:
            def clasificar_licitacion(id_adq):
                if not isinstance(id_adq, str): return 'ADQ'
                match = re.search(r'-([A-Z]{2,4})\d*$', id_adq)
                if not match: return 'ADQ'
                return 'CSUM' if match.group(1).upper() in ['LR', 'LQ'] else 'ADQ'
            df_proc['tipo_lic'] = df_proc['id_adquisicion'].apply(clasificar_licitacion)

        # 5. CÁLCULO DE KPIs (AQUÍ ESTABA EL ERROR)
        adjudicadas_df = df_proc[df_proc['estado_licitacion'].astype(str).str.contains('Adjudicad', case=False, na=False)]
        
        kpis = { 
            'total_licitaciones': len(df_proc), 
            'total_adjudicadas': len(adjudicadas_df), 
            'monto_adjudicado': f"${adjudicadas_df['monto'].sum():,.0f}".replace(',', '.') 
        }
        
        kpis_por_tipo = {}
        for tipo in ['CSUM', 'ADQ']:
            df_tipo = df_proc[df_proc['tipo_lic'] == tipo]
            if not df_tipo.empty:
                adj_tipo_df = df_tipo[df_tipo['estado_licitacion'].astype(str).str.contains('Adjudicad', case=False, na=False)]
                
                # --- CORRECCIÓN: SUMA REAL EN LUGAR DE "N/A" ---
                suma_estimada = df_tipo['monto_estimado'].sum()
                
                kpis_por_tipo[tipo] = { 
                    'total': len(df_tipo), 
                    'adjudicadas': len(adj_tipo_df), 
                    'monto_estimado': f"${suma_estimada:,.0f}".replace(',', '.'), # Ahora sí calcula
                    'monto_adjudicado': f"${adj_tipo_df['monto'].sum():,.0f}".replace(',', '.') 
                }
        
        # 6. TABLA DE RESPONSABLES
        responsables_df = df_proc.groupby('responsable').agg(
            Cantidad_Licitaciones=('id_adquisicion', 'count'),
            Lineas_Licitadas=('lineas', 'sum'),
            Monto_Total=('monto', 'sum')
        ).sort_values(by='Monto_Total', ascending=False).reset_index()
        
        responsables_df['Monto_Total_Fmt'] = responsables_df['Monto_Total'].apply(lambda x: f"${x:,.0f}".replace(',', '.'))
        responsables_data = responsables_df.to_dict('records')

        responsables_total = {
            'Responsable': 'TOTAL GENERAL',
            'Cantidad_Licitaciones': responsables_df['Cantidad_Licitaciones'].sum(),
            'Lineas_Licitadas': responsables_df['Lineas_Licitadas'].sum(),
            'Monto_Total_Fmt': f"${responsables_df['Monto_Total'].sum():,.0f}".replace(',', '.')
        }

        # 7. DETALLE PARA MODALS
        detalle_por_responsable = {}
        for resp in responsables_df['responsable'].unique():
            df_resp = df_proc[df_proc['responsable'] == resp].copy()
            detalle = df_resp[['id_adquisicion', 'nombre_adquisicion', 'estado_licitacion', 'lineas', 'ofertas', 'monto_formateado']].rename(columns={
                'id_adquisicion': 'ID', 'nombre_adquisicion': 'Nombre', 
                'estado_licitacion': 'Estado', 'lineas': 'Líneas', 
                'ofertas': 'Ofertas', 'monto_formateado': 'Monto'
            }).to_dict('records')
            detalle_por_responsable[resp] = detalle

        # 8. VISUALIZACIONES
        estados_finalizados = ['Adjudicada', 'Terminada', 'Desierta', 'Revocada', 'Cerrada']
        en_proceso_df = df_proc[~df_proc['estado_licitacion'].str.strip().isin(estados_finalizados)]
        
        df_en_proceso_html = None
        if not en_proceso_df.empty:
             df_en_proceso_html = en_proceso_df[['id_adquisicion', 'nombre_adquisicion', 'estado_licitacion', 'responsable']].rename(columns={'id_adquisicion': 'ID Adquisición', 'nombre_adquisicion': 'Nombre De La Adquisición', 'estado_licitacion': 'Estado Licitación', 'responsable': 'Responsable'}).to_html(classes='table table-striped table-sm', index=False, border=0, table_id='tabla-en-proceso')

        fig = px.pie(df_proc, names='estado_licitacion', title='Distribución por Estado', color='estado_licitacion', hole=0.4)
        fig.update_traces(textinfo='value+percent', texttemplate='%{value} (%{percent})')
        grafico_json = fig.to_json()
        
        detalle_por_tipo = {}
        for tipo in ['CSUM', 'ADQ']:
            df_tipo_filtrado = df_proc[df_proc['tipo_lic'] == tipo]
            if not df_tipo_filtrado.empty:
                df_detalle = df_tipo_filtrado[['id_adquisicion', 'nombre_adquisicion', 'estado_licitacion', 'monto_formateado']].rename(columns={'id_adquisicion': 'ID Adquisición', 'nombre_adquisicion': 'Nombre', 'estado_licitacion': 'Estado', 'monto_formateado': 'Monto'})
                detalle_por_tipo[tipo] = df_detalle.to_dict('records')
        
        return { 
            'kpis': kpis, 'kpis_por_tipo': kpis_por_tipo, 'responsables_data': responsables_data,
            'responsables_total': responsables_total, 'detalle_por_responsable': detalle_por_responsable,
            'df_en_proceso_html': df_en_proceso_html, 'total_en_proceso': len(en_proceso_df), 
            'grafico_json': grafico_json, 'df_merged': df_proc, 'detalle_por_tipo': detalle_por_tipo 
        }

    except Exception as e:
        print(f"Error en generador unificado: {e}")
        traceback.print_exc()
        return None

@app.route('/licitaciones-analisis', methods=['GET', 'POST'])
@login_required
def licitaciones_analisis():
    analisis = None
    kpis_requerimientos = {}
    last_sesion_id = None
    
    # 1. OBTENER LA SESIÓN GENERAL (Para KPIs laterales)
    try:
        with sqlite3.connect('compras.db') as conn:
            last_sesion_id_df = pd.read_sql_query("SELECT id FROM sesiones ORDER BY fecha DESC LIMIT 1", conn) 
            if not last_sesion_id_df.empty:
                last_sesion_id = int(last_sesion_id_df.iloc[0, 0])
                
                # Cargar KPIs de requerimientos
                df_procesados_lic = pd.read_sql_query("SELECT * FROM procesados WHERE sesion_id = ? AND tipo_compra = 'Licitación'", conn, params=(last_sesion_id,)) 
                df_en_proceso_lic = pd.read_sql_query("SELECT * FROM en_proceso WHERE sesion_id = ? AND tipo_compra = 'Licitación'", conn, params=(last_sesion_id,)) 
                total_procesados, total_en_proceso = len(df_procesados_lic), len(df_en_proceso_lic)
                total_neto = total_procesados + total_en_proceso
                kpis_requerimientos = {
                    'total_neto': total_neto, 'procesados': total_procesados,
                    'en_proceso': total_en_proceso, 'eficiencia': f"{(total_procesados / total_neto * 100) if total_neto > 0 else 0:.1f}%"
                }
            else:
                last_sesion_id = None
    except Exception as e:
        print(f"Advertencia al cargar KPIs de requerimientos: {e}")
        last_sesion_id = None

    # 2. PROCESAR SI SE SUBEN ARCHIVOS NUEVOS (POST)
    if request.method == 'POST':
        try:
            df_resultados = leer_archivo(request.files.get('resultados_lic')) 
            df_seguimiento = leer_archivo(request.files.get('seguimiento_lic')) 

            if df_resultados is not None and df_seguimiento is not None:
                # Función de búsqueda flexible
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
                    flash(f"Error: No se encontraron las columnas de ID: {', '.join(missing)}.", 'danger')
                else:
                    # Merge
                    df_merged = pd.merge(df_resultados, df_seguimiento, left_on=col_map['codigo_res'], right_on=col_map['codigo_seg'], how='left', suffixes=('_res', '_seg')) 
                    
                    analisis = _generar_analisis_licitaciones_desde_df(df_merged) 

                    # --- AQUÍ ESTÁ LA CORRECCIÓN DEL GUARDADO ---
                    if analisis and last_sesion_id:
                        sesion_id = last_sesion_id
                        df_to_save = analisis['df_merged'].copy()
                        df_to_save['sesion_id'] = sesion_id 
                        
                        # Mapeo de columnas: DataFrame -> Base de Datos
                        cols_bd = {
                            'id_adquisicion': 'id_adquisicion', 
                            'nombre_adquisicion': 'nombre_adquisicion', 
                            'estado_licitacion': 'estado_licitacion', 
                            'monto': 'monto_adjudicado', 
                            'responsable': 'responsable', 
                            'tipo_lic': 'tipo_licitacion',
                            'lineas': 'cantidad_lineas',
                            'ofertas': 'cantidad_ofertas',
                            'monto_estimado': 'monto_estimado'  # <--- ¡ESTO FALTABA!
                        }
                        
                        df_renamed = df_to_save.rename(columns=cols_bd)
                        
                        # Lista estricta de columnas que existen en la BD
                        columnas_finales_bd = [
                            'sesion_id', 'id_adquisicion', 'nombre_adquisicion', 
                            'estado_licitacion', 'monto_adjudicado', 'responsable', 
                            'tipo_licitacion', 'cantidad_lineas', 'cantidad_ofertas',
                            'monto_estimado' # <--- ¡ESTO FALTABA!
                        ]
                        
                        # Filtramos para guardar solo lo que entra en la BD
                        df_final_para_guardar = df_renamed[[col for col in columnas_finales_bd if col in df_renamed.columns]]

                        with sqlite3.connect('compras.db') as conn:
                            try:
                                # Borrar datos viejos de esta sesión para no duplicar
                                conn.execute("DELETE FROM analisis_licitaciones WHERE sesion_id = ?", (sesion_id,)) 
                                df_final_para_guardar.to_sql('analisis_licitaciones', conn, if_exists='append', index=False)
                                flash('✅ Análisis completado y guardado exitosamente', 'success') 
                            except Exception as db_err:
                                print(f"Error BD: {db_err}")
                                flash(f'⚠️ Error al guardar en BD: {db_err}', 'warning')

                        notification_manager.broadcast({
                            'user': session['usuario']['nombre_completo'],
                            'page': 'Análisis Licitaciones'
                        }) 
                    elif not analisis:
                         flash('❌ Error al procesar los datos después de la fusión.', 'danger') 
            else:
                flash('❌ Error al leer los archivos.', 'error') 
        except Exception as e:
            flash(f'❌ Error procesando archivos: {str(e)}', 'danger') 
            import traceback
            traceback.print_exc()

    # 3. CARGAR DATOS GUARDADOS (PERSISTENCIA)
    if not analisis:
        try:
            with sqlite3.connect('compras.db') as conn:
                # Buscar la última sesión con análisis guardado
                query_last_session = "SELECT MAX(sesion_id) FROM analisis_licitaciones"
                cursor = conn.cursor()
                cursor.execute(query_last_session)
                row = cursor.fetchone()
                
                target_sesion_id = row[0] if row and row[0] else None

                if target_sesion_id:
                    print(f"INFO: Cargando análisis persistente (Sesión {target_sesion_id})")
                    # Aquí se recuperan todos los campos, incluido monto_estimado
                    df_guardado = pd.read_sql_query("SELECT * FROM analisis_licitaciones WHERE sesion_id = ?", conn, params=(target_sesion_id,))
                
                    if not df_guardado.empty:
                        analisis = _generar_analisis_licitaciones_desde_df(df_guardado)
                
        except Exception as e:
            print(f"Error al cargar el análisis guardado: {e}")

    return render_template(
        'licitaciones_analisis.html',
        kpis_requerimientos=kpis_requerimientos,
        analisis=analisis
    )
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