# data_processor.py

import pandas as pd
import sqlite3
from datetime import datetime
import time

def obtener_datos_sesion():
    """Obtiene los datos de la última sesión y los dataframes asociados."""
    try:
        with sqlite3.connect('compras.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sesiones'")
            if not cursor.fetchone(): return None, None, None

            ultima_sesion_df = pd.read_sql_query('SELECT * FROM sesiones ORDER BY fecha DESC LIMIT 1', conn)
            if ultima_sesion_df.empty: return None, None, None
            
            ultima_sesion = ultima_sesion_df.to_dict('records')[0]
            sesion_id = ultima_sesion['id']

            df_procesados = pd.read_sql_query("SELECT * FROM procesados WHERE sesion_id = ?", conn, params=(sesion_id,))
            df_en_proceso = pd.read_sql_query("SELECT * FROM en_proceso WHERE sesion_id = ?", conn, params=(sesion_id,))

            return ultima_sesion, df_procesados, df_en_proceso
    except Exception as e:
        print(f"Error al obtener datos de sesión: {e}")
        return None, None, None

def leer_archivo(uploaded_file):
    """Lee un archivo subido (Excel o CSV) y lo convierte en un DataFrame."""
    if not uploaded_file:
        return None
    try:
        filename = uploaded_file.filename
        file_extension = filename.lower().split('.')[-1]
        if file_extension in ['xlsx', 'xls']:
            return pd.read_excel(uploaded_file)
        elif file_extension == 'csv':
            return pd.read_csv(uploaded_file, sep=None, engine='python', encoding='utf-8-sig', on_bad_lines='skip')
        else:
            return None
    except Exception as e:
        print(f"Error leyendo archivo {uploaded_file.filename}: {e}")
        return None

class ComprasProcessor:
    def __init__(self):
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect('compras.db')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS sesiones (
                id INTEGER PRIMARY KEY, fecha DATETIME, total_brutos INTEGER,
                req_procesados INTEGER, req_en_proceso INTEGER, eficiencia REAL
            )''')
        cursor = conn.execute("PRAGMA table_info(sesiones)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'req_cancelados' not in columns:
            conn.execute('ALTER TABLE sesiones ADD COLUMN req_cancelados INTEGER DEFAULT 0')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS procesados (
                id INTEGER PRIMARY KEY, sesion_id INTEGER, numero_req TEXT, titulo TEXT,
                tipo_compra TEXT, unidad TEXT, comprador TEXT, orden_compra TEXT,
                tipo_financiamiento TEXT,
                FOREIGN KEY (sesion_id) REFERENCES sesiones (id) ON DELETE CASCADE
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS en_proceso (
                id INTEGER PRIMARY KEY, sesion_id INTEGER, numero_req TEXT, titulo TEXT,
                tipo_compra TEXT, unidad TEXT, comprador TEXT, estado TEXT,
                tipo_financiamiento TEXT,
                FOREIGN KEY (sesion_id) REFERENCES sesiones (id) ON DELETE CASCADE
            )''')
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
        columnas = df.columns.str.lower().str.strip()
        patrones_numero = ['número', 'numero', 'req', 'requerimiento', 'solicitud', 'id']
        patrones_tipo_compra = ['tipo', 'compra', 'modalidad', 'procedimiento']
        patrones_orden_compra = ['orden', 'oc', 'compra', 'estado']
        col_numero = None
        col_tipo_compra = None
        col_orden_compra = None
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(patron in col_lower for patron in patrones_numero):
                col_numero = col
                break
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
        
       # --- AÑADIDO ---
        patrones_financiamiento = ['financiamiento', 'fuente', 'aporte', 'gestion', 'gestión']
        col_financiamiento = None
        for col in df.columns:
            col_lower = col.lower().strip()
            # Aquí verifica si ALGUNA de las palabras clave está en el nombre de tu columna
            if any(patron in col_lower for patron in patrones_financiamiento):
                col_financiamiento = col
                break
        
        return col_numero, col_tipo_compra, col_orden_compra, col_financiamiento
        # --- FIN AÑADIDO ---


    def _normalizar_id(self, valor):
        try:
            valor_str = str(valor).strip()
            if valor_str.lower() == 'nan' or valor_str == '':
                return ''
            try:
                valor_float = float(valor_str)
                if valor_float.is_integer():
                    return str(int(valor_float))
                else:
                    return str(valor_float)
            except (ValueError, OverflowError):
                return valor_str
        except:
            return str(valor)

    def _expandir_ordenes_compra(self, oc_str):
        import re
        if pd.isna(oc_str) or str(oc_str).strip() == '' or str(oc_str).lower() == 'nan':
            return []
        oc_str = str(oc_str).strip()
        if not re.search(r'\d+-\d+-[A-Z0-9]+', oc_str):
            return [oc_str.upper()]
        ordenes = []
        partes_por_espacio = oc_str.split()
        for parte in partes_por_espacio:
            if '/' in parte:
                subpartes = parte.split('/')
                primera_oc = subpartes[0].strip()
                ordenes.append(primera_oc)
                match = re.match(r'^(\d+)-(\d+)-([A-Z0-9]+)$', primera_oc)
                if match:
                    prefijo = match.group(1)
                    sufijo = match.group(3)
                    for subparte in subpartes[1:]:
                        subparte = subparte.strip()
                        if not subparte:
                            continue
                        if re.match(r'^\d+$', subparte):
                            oc_expandida = f"{prefijo}-{subparte}-{sufijo}"
                            ordenes.append(oc_expandida)
                        elif re.match(r'^\d+-\d+-[A-Z0-9]+$', subparte):
                            ordenes.append(subparte)
                else:
                    for subparte in subpartes[1:]:
                        if subparte.strip():
                            ordenes.append(subparte.strip())
            else:
                if parte.strip():
                    ordenes.append(parte.strip())
        ordenes = [oc.upper() for oc in ordenes if oc]
        return list(set(ordenes))

    def _expandir_dataframe_ordenes(self, df, col_oc):
        if df.empty:
            return df.copy()
        filas_expandidas = []
        for idx, row in df.iterrows():
            oc_valor = row[col_oc]
            ordenes_expandidas = self._expandir_ordenes_compra(oc_valor)
            if len(ordenes_expandidas) == 0:
                filas_expandidas.append(row.copy())
            else:
                for oc in ordenes_expandidas:
                    row_copy = row.copy()
                    row_copy[col_oc] = oc
                    filas_expandidas.append(row_copy)
        df_expandido = pd.DataFrame(filas_expandidas, columns=df.columns)
        return df_expandido

    def procesar_datos(self, df_experto, df_cancelados, df_precompra=None):
        """
        Procesa los datos y devuelve un diccionario con el resultado y los mensajes.
        No usa funciones de Streamlit.
        """
        mensajes = []
        # --- MODIFICADO ---
        COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC, col_financiamiento = self._detectar_columnas(df_experto)
        # --- FIN MODIFICADO ---

        if not COL_NUM_REQ:
            mensajes.append({'text': "❌ No se pudo detectar la columna de número de requerimiento.", 'category': 'error'})
            return {'success': False, 'messages': mensajes}
        if not COL_TIPO_COMPRA:
            mensajes.append({'text': "❌ No se pudo detectar la columna de tipo de compra.", 'category': 'error'})
            return {'success': False, 'messages': mensajes}
        if not COL_ESTADO_OC:
            mensajes.append({'text': "❌ No se pudo detectar la columna de orden de compra.", 'category': 'error'})
            return {'success': False, 'messages': mensajes}

        mensajes.append({'text': f"✅ Columnas detectadas: Número='{COL_NUM_REQ}', Tipo='{COL_TIPO_COMPRA}', Estado OC='{COL_ESTADO_OC}'", 'category': 'info'})
        if col_financiamiento:
             mensajes.append({'text': f"✅ Columna de financiamiento detectada: '{col_financiamiento}'", 'category': 'info'})

        ids_cancelados = set(df_cancelados.iloc[:, 0].apply(self._normalizar_id))
        ids_cancelados.discard('')
        df_experto[COL_NUM_REQ + '_normalizado'] = df_experto[COL_NUM_REQ].apply(self._normalizar_id)
        df_filtrado = df_experto[~df_experto[COL_NUM_REQ + '_normalizado'].isin(ids_cancelados)].copy()
        dict_precompra = set(df_precompra.iloc[:, 0].apply(self._normalizar_id)) if df_precompra is not None and not df_precompra.empty else set()
        dict_precompra.discard('')
        cancelados_filtrados = len(df_experto) - len(df_filtrado)
        
        mensajes.append({'text': f"📊 Debug: Total brutos: {len(df_experto)}, Cancelados filtrados: {cancelados_filtrados}, Neto: {len(df_filtrado)}", 'category': 'info'})

        procesados, en_proceso, no_clasificados = [], [], []
        for _, row in df_filtrado.iterrows():
            numero_req, tipo_compra, estado_oc_raw = str(row[COL_NUM_REQ]), str(row[COL_TIPO_COMPRA]), row[COL_ESTADO_OC]
            estado_oc = str(estado_oc_raw) if pd.notna(estado_oc_raw) else ""
            if tipo_compra == "Convenio de Insumos": tipo_compra = "Convenio de Suministros Vigentes"
            elif tipo_compra == "Trato Directo con Cotizaciones": tipo_compra = "Trato Directo"
            es_proc = self._es_procesado(tipo_compra, estado_oc)
            es_en_proc = self._es_en_proceso(tipo_compra, estado_oc, numero_req, dict_precompra, es_proc)
            if es_proc: procesados.append(row)
            elif es_en_proc: en_proceso.append(row)
            else: no_clasificados.append(row)

        total_procesados, total_en_proceso = len(procesados), len(en_proceso)
        total_neto = len(df_filtrado)
        total_no_clasificados = len(no_clasificados)
        
        if total_no_clasificados > 0:
             mensajes.append({'text': f"⚠️ ATENCIÓN: {total_no_clasificados} requerimientos NO fueron clasificados.", 'category': 'warning'})

        eficiencia = (total_procesados / total_neto * 100) if total_neto > 0 else 0
        df_procesados = pd.DataFrame(procesados) if procesados else pd.DataFrame()
        df_en_proceso = pd.DataFrame(en_proceso) if en_proceso else pd.DataFrame()
        
        # --- MODIFICADO ---
        sesion_id = self._guardar_resultados(len(df_experto), total_procesados, total_en_proceso, eficiencia, cancelados_filtrados, df_procesados, df_en_proceso, COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC, col_financiamiento)
        # --- FIN MODIFICADO ---
        
        if sesion_id is not None:
            return {'success': True, 'messages': mensajes}
        else:
            mensajes.append({'text': '❌ Hubo un error al guardar los resultados en la base de datos.', 'category': 'error'})
            return {'success': False, 'messages': mensajes}

    def _detectar_columnas_adicionales(self, df, col_numero, col_tipo_compra, col_estado_oc):
        patrones_titulo = ['titulo', 'título', 'solicitud', 'descripcion', 'descripción', 'nombre']
        patrones_unidad = ['unidad', 'solicitante', 'area', 'área', 'departamento']
        patrones_comprador = ['comprador', 'asignado', 'responsable', 'buyer']
        col_titulo, col_unidad, col_comprador = None, None, None
        for col in df.columns:
            if any(patron in col.lower().strip() for patron in patrones_titulo):
                col_titulo = col
                break
        for col in df.columns:
            if any(patron in col.lower().strip() for patron in patrones_unidad):
                col_unidad = col
                break
        for col in df.columns:
            if any(patron in col.lower().strip() for patron in patrones_comprador):
                col_comprador = col
                break
        return col_titulo, col_unidad, col_comprador

    # --- FIRMA MODIFICADA ---
    def _guardar_resultados(self, total_brutos, total_procesados, total_en_proceso, eficiencia, total_cancelados, df_procesados, df_en_proceso, col_numero, col_tipo_compra, col_estado_oc, col_financiamiento=None):
        try:
            with sqlite3.connect('compras.db') as conn:
                cursor = conn.execute('INSERT INTO sesiones (fecha, total_brutos, req_procesados, req_en_proceso, eficiencia, req_cancelados) VALUES (?, ?, ?, ?, ?, ?)',
                                    (datetime.now(), total_brutos, total_procesados, total_en_proceso, eficiencia, total_cancelados))
                sesion_id = cursor.lastrowid
                if not df_procesados.empty:
                    col_titulo, col_unidad, col_comprador = self._detectar_columnas_adicionales(df_procesados, col_numero, col_tipo_compra, col_estado_oc)
                    # --- MAPA MODIFICADO ---
                    column_map = {col: new_name for col, new_name in {
                        col_numero: 'numero_req', col_titulo: 'titulo', col_tipo_compra: 'tipo_compra',
                        col_unidad: 'unidad', col_comprador: 'comprador', col_estado_oc: 'orden_compra',
                        col_financiamiento: 'tipo_financiamiento'
                    }.items() if col}
                    df_to_save = df_procesados[list(column_map.keys())].rename(columns=column_map)
                    df_to_save['sesion_id'] = sesion_id
                    df_to_save.to_sql('procesados', conn, if_exists='append', index=False)
                if not df_en_proceso.empty:
                    col_titulo, col_unidad, col_comprador = self._detectar_columnas_adicionales(df_en_proceso, col_numero, col_tipo_compra, col_estado_oc)
                    # --- MAPA MODIFICADO ---
                    column_map = {col: new_name for col, new_name in {
                        col_numero: 'numero_req', col_titulo: 'titulo', col_tipo_compra: 'tipo_compra',
                        col_unidad: 'unidad', col_comprador: 'comprador', col_estado_oc: 'estado',
                        col_financiamiento: 'tipo_financiamiento'
                    }.items() if col}
                    df_to_save = df_en_proceso[list(column_map.keys())].rename(columns=column_map)
                    df_to_save['sesion_id'] = sesion_id
                    df_to_save.to_sql('en_proceso', conn, if_exists='append', index=False)
                return sesion_id
        except Exception as e:
            print(f"Error guardando resultados: {e}")
            return None

    def _guardar_datos_financieros(self, sesion_id, df_analisis_financiero, df_experto_bruto=None):
        """
        Guarda los datos del análisis financiero en la base de datos.
        VERSIÓN CORREGIDA Y SIMPLIFICADA.
        """
        try:
            with sqlite3.connect('compras.db') as conn:
                if df_analisis_financiero.empty:
                    print("DataFrame vacío, no hay datos para guardar.")
                    return

                # 1. Crear una copia para trabajar de forma segura
                df_to_save = df_analisis_financiero.copy()
                df_to_save['sesion_id'] = sesion_id

                # 2. Mapa de Mapeo: Define las columnas que queremos y sus posibles nombres en el DataFrame
                # La clave es el nombre final en la BD. El valor es una lista de posibles nombres en el DF.
                mapa_columnas = {
                    'numero_oc': ['N° Orden', 'oc_normalizada', 'oc_normalizada_res'], # <-- LÍNEA CORREGIDA
                    'nombre_oc': ['Nombre de la OC'],
                    'tipo_compra': ['tipo_compra'],
                    'estado_oc': ['Estado OC'],
                    'unidad': ['unidad'], # <-- Clave: Buscamos la columna 'unidad' que ya fue preparada.
                    'nombre_proveedor': ['Nombre Proveedor'],
                    'rut_proveedor': ['Rut Proveedor'],
                    'fecha_creacion': ['Fecha de Creación'],
                    'fecha_envio': ['Fecha Envió'],
                    'monto_neto': ['Monto Neto'],
                    'descuentos': ['Descuento'],
                    'cargos': ['Cargo'],
                    'iva': ['I.V.A'],
                    'impuesto_especifico': ['Impuesto Específico'],
                    'total_oc': ['Total OC'],
                    'numero_req': ['numero_req'],
                    'titulo_req': ['titulo'],
                    'comprador': ['comprador']
                }

                columnas_renombrar = {}
                for db_col, df_cols_posibles in mapa_columnas.items():
                    for df_col in df_cols_posibles:
                        if df_col in df_to_save.columns:
                            columnas_renombrar[df_col] = db_col
                            break
                
                # 3. Renombrar las columnas encontradas al nombre de la BD
                df_to_save.rename(columns=columnas_renombrar, inplace=True)
                
                # 4. Obtener la lista de columnas que realmente existen en la tabla de la BD
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(oc_financiero)")
                columnas_bd = [info[1] for info in cursor.fetchall()]
                
                # 5. Filtrar el DataFrame para que solo contenga columnas que se pueden guardar
                columnas_a_guardar = [col for col in df_to_save.columns if col in columnas_bd]
                df_final = df_to_save[columnas_a_guardar]

                # 6. Convertir columnas monetarias a número
                for col in ['monto_neto', 'descuentos', 'cargos', 'iva', 'impuesto_especifico', 'total_oc']:
                    if col in df_final.columns:
                        df_final[col] = df_final[col].apply(self._convertir_a_numero)

                # 7. Guardar en la base de datos
                conn.execute('DELETE FROM oc_financiero WHERE sesion_id = ?', (sesion_id,))
                df_final.to_sql('oc_financiero', conn, if_exists='append', index=False)
                
                print(f"✅ {len(df_final)} registros guardados en oc_financiero para la sesión {sesion_id}.")
                if 'unidad' in df_final.columns:
                    print(f"✅ La columna 'unidad' se guardó correctamente con {df_final['unidad'].notna().sum()} valores no nulos.")

        except Exception as e:
            print(f"❌ Error al guardar datos financieros: {e}")
            import traceback
            traceback.print_exc()

    def _convertir_a_numero(self, valor):
        if pd.isna(valor): return 0
        try:
            if isinstance(valor, str): valor = valor.replace('$', '').replace(',', '').replace('.', '').strip()
            return float(valor)
        except: return 0

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
            print(f"Error en la base de datos: {e}")
            return None

    def procesar_analisis_financiero(self, df_resultado_oc, df_experto_historico, sesion_id=None, tipo_filtro=None):
        """
        Procesa el análisis financiero completo: merge de archivos, cálculo de KPIs y generación de gráficos.

        Args:
            df_resultado_oc: DataFrame con ListadoResultadoOC
            df_experto_historico: DataFrame con Experto Histórico
            sesion_id: ID de sesión para guardar en BD (opcional)
            tipo_filtro: Filtrar por tipo de compra específico (ej: "Compra Ágil")

        Returns:
            dict con resultados del análisis
        """
        import re

        resultado = {'success': False, 'messages': [], 'kpis': {}, 'df_analisis': None, 'unmatched_ocs': []}

        try:
            # 1. Detectar columna de OC en ListadoResultadoOC
            col_oc_resultado = None
            patrones_oc = ['orden', 'oc', 'n°', 'numero', 'número', 'compra']
            for col in df_resultado_oc.columns:
                col_lower = str(col).lower().strip()
                if any(patron in col_lower for patron in patrones_oc):
                    col_oc_resultado = col
                    break

            if not col_oc_resultado:
                resultado['messages'].append({'text': f"❌ No se detectó columna de OC. Columnas: {', '.join(df_resultado_oc.columns)}", 'category': 'error'})
                return resultado

            # 2. Detectar columnas en Experto Histórico
            # --- MODIFICADO ---
            COL_NUM_REQ, COL_TIPO_COMPRA, COL_ESTADO_OC, col_financiamiento = self._detectar_columnas(df_experto_historico)
            # --- FIN MODIFICADO ---

            if not COL_ESTADO_OC:
                resultado['messages'].append({'text': "❌ No se detectó columna de estado/OC en Experto", 'category': 'error'})
                return resultado

            # 3. EXPANDIR órdenes múltiples en experto
            df_experto_expandido = self._expandir_dataframe_ordenes(df_experto_historico, COL_ESTADO_OC)

            # 4. Normalizar columnas de OC
            df_resultado_oc['oc_normalizada_res'] = df_resultado_oc[col_oc_resultado].astype(str).str.strip().str.upper()
            df_experto_expandido['oc_normalizada_exp'] = df_experto_expandido[COL_ESTADO_OC].astype(str).str.strip().str.upper()

            # 5. Merge: LEFT JOIN desde resultado hacia experto
            df_analisis = pd.merge(
                df_resultado_oc,
                df_experto_expandido,
                left_on='oc_normalizada_res',
                right_on='oc_normalizada_exp',
                how='left',
                suffixes=('', '_experto')
            ).copy()

            # --- 🔥 INICIO DE LA CORRECCIÓN ---
            # Se eliminan duplicados basados en la OC del archivo de resultados (el de la izquierda).
            # Esto asegura que cada OC y su monto se cuenten UNA SOLA VEZ.
            df_analisis.drop_duplicates(subset=['oc_normalizada_res'], keep='first', inplace=True)
            # --- FIN DE LA CORRECCIÓN ---
            
            # Identificar OCs sin match
            col_req_despues_merge = f"{COL_NUM_REQ}_experto" if f"{COL_NUM_REQ}_experto" in df_analisis.columns else COL_NUM_REQ
            unmatched_df = df_analisis[df_analisis[col_req_despues_merge].isnull()]
            if not unmatched_df.empty:
                resultado['unmatched_ocs'] = unmatched_df[col_oc_resultado].tolist()
            
            matches = len(df_analisis) - len(unmatched_df)
            if len(resultado['unmatched_ocs']) > 0:
                resultado['messages'].append({'text': f"✅ Merge completado. {matches} de {len(df_analisis)} OCs con información combinada", 'category': 'success'})
                resultado['messages'].append({'text': f"⚠️ {len(resultado['unmatched_ocs'])} OCs sin match", 'category': 'warning'})
            else:
                resultado['messages'].append({'text': f"✅ Merge completado. Todas las {len(df_analisis)} OCs encontraron correspondencia.", 'category': 'success'})

            # 6. Continuar con el resto del procesamiento...
            col_unidad = None
            patrones_unidad = ['unidad', 'solicitante', 'area', 'área']
            for col in df_experto_expandido.columns:
                if any(patron in str(col).lower().strip() for patron in patrones_unidad):
                    col_unidad = col
                    break

            if 'unidad_experto' in df_analisis.columns:
                df_analisis['unidad'] = df_analisis['unidad_experto']
            elif col_unidad and col_unidad in df_analisis.columns:
                if col_unidad != 'unidad':
                    df_analisis['unidad'] = df_analisis[col_unidad]

            if COL_TIPO_COMPRA and COL_TIPO_COMPRA != 'tipo_compra':
                df_analisis['tipo_compra'] = df_analisis[COL_TIPO_COMPRA]

            # --- AÑADIDO: Mapear Columna Financiamiento ---
            if col_financiamiento and col_financiamiento != 'tipo_financiamiento':
                df_analisis['tipo_financiamiento'] = df_analisis[col_financiamiento]

            if 'Total OC' not in df_analisis.columns:
                for col in df_analisis.columns:
                    if 'total' in str(col).lower():
                        df_analisis['Total OC'] = df_analisis[col]
                        break
            if 'Total OC' in df_analisis.columns:
                df_analisis['Total OC'] = df_analisis['Total OC'].astype(str).str.replace(',', '').str.replace('$', '').str.strip()
                df_analisis['Total OC'] = pd.to_numeric(df_analisis['Total OC'], errors='coerce').fillna(0)
            else:
                df_analisis['Total OC'] = 0
            # --- DEBUG: VERIFICAR CRUCE DE FINANCIAMIENTO ---
            print("\n" + "="*50)
            print("🧐 DIAGNÓSTICO DE MERGE (data_processor.py)")
            print("="*50)
            
            # 1. ¿Existe la columna en el DataFrame final?
            if 'tipo_financiamiento' in df_analisis.columns:
                nulos = df_analisis['tipo_financiamiento'].isnull().sum()
                llenos = df_analisis['tipo_financiamiento'].notnull().sum()
                ejemplos = df_analisis['tipo_financiamiento'].dropna().unique()[:5]
                
                print(f"✅ Columna 'tipo_financiamiento' ENCONTRADA.")
                print(f"📊 Registros con dato: {llenos}")
                print(f"⚠️ Registros vacíos (NaN): {nulos}")
                print(f"🔍 Primeros 5 valores encontrados: {ejemplos}")
            else:
                print("❌ ERROR: La columna 'tipo_financiamiento' NO EXISTE en df_analisis tras el merge.")
                print("Columnas disponibles:", df_analisis.columns.tolist())
            print("="*50 + "\n")
            # ------------------------------------------------
            if sesion_id:
                self._guardar_datos_financieros(sesion_id, df_analisis)
                resultado['messages'].append({'text': f"💾 {len(df_analisis)} registros financieros guardados en la base de datos", 'category': 'info'})

            df_analisis_filtrado = df_analisis.copy()
            if tipo_filtro and 'tipo_compra' in df_analisis_filtrado.columns:
                df_analisis_filtrado = df_analisis_filtrado[df_analisis_filtrado['tipo_compra'] == tipo_filtro].copy()
                resultado['messages'].append({'text': f"🔍 Filtrado por: {tipo_filtro} ({len(df_analisis_filtrado)} registros)", 'category': 'info'})
            
            
            # ----- INICIO BLOQUE KPI MODIFICADO -----
            
            # Lista de exclusión actualizada
            terminos_exclusion = ['cancelad', 'rechazad', 'no aceptad', 'eliminad']
            
            def es_estado_rechazado(estado):
                if pd.isna(estado): return False
                return any(termino in str(estado).lower() for termino in terminos_exclusion)
            
            if 'Estado OC' in df_analisis_filtrado.columns:
                df_validas = df_analisis_filtrado[~df_analisis_filtrado['Estado OC'].apply(es_estado_rechazado)].copy()
            else:
                df_validas = df_analisis_filtrado.copy()

            # Monto Total Bruto (usa el df_analisis_filtrado, que es el "bruto" para esta sección)
            monto_total_bruto = df_analisis_filtrado['Total OC'].sum()

            # Monto Total Válido (usa el df_validas filtrado)
            monto_total_valido = df_validas['Total OC'].sum()
            
            # Monto Conforme (solo 'Recepción Conforme' y 'Aceptada', usa df_validas)
            estados_conformes = ['Recepción Conforme', 'Aceptada']
            if 'Estado OC' in df_validas.columns:
                monto_conforme = df_validas[df_validas['Estado OC'].isin(estados_conformes)]['Total OC'].sum()
            else:
                monto_conforme = 0
            
            # Efectividad (Conforme vs. Válido)
            efectividad_recepcion = (monto_conforme / monto_total_valido * 100) if monto_total_valido > 0 else 0
            
            resultado['kpis'] = {
                'monto_total_bruto': f"${monto_total_bruto:,.0f}",
                'monto_total_valido': f"${monto_total_valido:,.0f}",
                'monto_conforme': f"${monto_conforme:,.0f}",
                'efectividad': f"{efectividad_recepcion:.1f}%"
            }
            # ----- FIN BLOQUE KPI MODIFICADO -----

            resultado['df_analisis'] = df_analisis_filtrado
            resultado['df_validas'] = df_validas
            resultado['success'] = True
            return resultado
        except Exception as e:
            resultado['messages'].append({'text': f"❌ Error: {str(e)}", 'category': 'error'})
            return resultado