# licitaciones_manager.py
import sqlite3
import pandas as pd
from datetime import datetime

class LicitacionesManager:
    def __init__(self, db_path='compras.db'):
        self.db_path = db_path

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def obtener_licitaciones_completas(self):
        with self._get_conn() as conn:
            try:
                df_licitaciones = pd.read_sql_query("SELECT * FROM licitaciones ORDER BY id DESC", conn)
                df_convenios = pd.read_sql_query("SELECT * FROM convenios", conn)
                df_ocs = pd.read_sql_query("SELECT * FROM ordenes_compra", conn)

                if not df_convenios.empty:
                    df_convenios['monto_adjudicado'] = pd.to_numeric(df_convenios['monto_adjudicado'], errors='coerce').fillna(0)
                if not df_ocs.empty:
                    df_ocs['monto'] = pd.to_numeric(df_ocs['monto'], errors='coerce').fillna(0)

                licitaciones_dict = df_licitaciones.to_dict('records')
                convenios_dict = df_convenios.to_dict('records')
                ocs_dict = df_ocs.to_dict('records')

                for lic in licitaciones_dict:
                    lic['convenios'] = [conv for conv in convenios_dict if conv['licitacion_id'] == lic['id']]
                    for conv in lic['convenios']:
                        conv['ocs'] = [oc for oc in ocs_dict if oc['convenio_id'] == conv['id']]
                        conv['total_ocs'] = sum(oc['monto'] for oc in conv['ocs'])

                        fecha_termino_str = conv.get('fecha_termino')
                        tiempo_restante_str = "No definido"
                        if fecha_termino_str:
                            try:
                                fecha_termino = datetime.strptime(fecha_termino_str, '%Y-%m-%d')
                                hoy = datetime.now()
                                if fecha_termino < hoy:
                                    tiempo_restante_str = "Vencido"
                                else:
                                    dias = (fecha_termino - hoy).days
                                    if dias > 365: tiempo_restante_str = f"~ {dias // 365} año(s)"
                                    elif dias > 30: tiempo_restante_str = f"~ {dias // 30} mes(es)"
                                    else: tiempo_restante_str = f"{dias} día(s)"
                            except (ValueError, TypeError):
                                tiempo_restante_str = "Fecha inválida"
                        conv['tiempo_restante'] = tiempo_restante_str
                return licitaciones_dict
            except Exception as e:
                print(f"Error al obtener licitaciones: {e}")
                return []
    
    def agregar_licitacion(self, id_licitacion, nombre, requirente, **kwargs):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO licitaciones (id_licitacion, nombre_licitacion, requirente, inspector_tecnico, decreto_adjudicacion) VALUES (?, ?, ?, ?, ?)",
                    (id_licitacion, nombre, requirente, kwargs.get('inspector_tecnico'), kwargs.get('decreto_adjudicacion'))
                )
                conn.commit()
                return True, "Licitación agregada exitosamente."
            except sqlite3.IntegrityError:
                return False, f"Error: El ID de licitación '{id_licitacion}' ya existe."

    def actualizar_licitacion(self, licitacion_id, **kwargs):
        """Actualiza los datos de una licitación existente."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            set_clause = ", ".join([f"{key} = ?" for key in kwargs])
            valores = list(kwargs.values())
            valores.append(licitacion_id)
            if not set_clause:
                return False, "No se proporcionaron datos para actualizar."
            sql = f"UPDATE licitaciones SET {set_clause} WHERE id = ?"
            try:
                cursor.execute(sql, valores)
                conn.commit()
                return True, "Licitación actualizada exitosamente."
            except Exception as e:
                return False, f"Error al actualizar la licitación: {e}"

    def agregar_convenio(self, licitacion_id, proveedor, rut, monto, **kwargs):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            campos_posibles = ['fecha_inicio', 'fecha_termino', 'meses', 'id_gestion_contratos', 'tiene_ipc', 'garantia', 'decreto_aprueba_contrato', 'id_mercado_publico']
            columnas = ['licitacion_id', 'proveedor', 'rut_proveedor', 'monto_adjudicado']
            valores = [licitacion_id, proveedor, rut, monto]
            for campo in campos_posibles:
                if campo in kwargs and kwargs[campo]:
                    columnas.append(campo)
                    valores.append(kwargs[campo])
            placeholders = ', '.join(['?'] * len(columnas))
            sql = f"INSERT INTO convenios ({', '.join(columnas)}) VALUES ({placeholders})"
            cursor.execute(sql, valores)
            conn.commit()
            return True, "Proveedor agregado exitosamente."

    def actualizar_convenio(self, convenio_id, **kwargs):
        """Actualiza los datos de un convenio existente en la base de datos."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            set_clause = ", ".join([f"{key} = ?" for key in kwargs])
            valores = list(kwargs.values())
            valores.append(convenio_id)
            if not set_clause:
                return False, "No se proporcionaron datos para actualizar."
            sql = f"UPDATE convenios SET {set_clause} WHERE id = ?"
            try:
                cursor.execute(sql, valores)
                conn.commit()
                return True, "Convenio actualizado exitosamente."
            except Exception as e:
                return False, f"Error al actualizar el convenio: {e}"

    def agregar_oc(self, convenio_id, numero_oc, monto):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            # Asume que la fecha de emisión se establece automáticamente o se pasa como argumento
            fecha_emision = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("INSERT INTO ordenes_compra (convenio_id, numero_oc, monto, fecha_emision) VALUES (?, ?, ?, ?)", (convenio_id, numero_oc, monto, fecha_emision))
            conn.commit()
            return True, "Orden de compra agregada."

    def actualizar_oc(self, oc_id, **kwargs):
        """Actualiza los datos de una orden de compra existente."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            valid_kwargs = {k: v for k, v in kwargs.items() if k in ['numero_oc', 'monto', 'fecha_emision']}
            if not valid_kwargs:
                return False, "No se proporcionaron datos válidos para actualizar."
            
            set_clause = ", ".join([f"{key} = ?" for key in valid_kwargs])
            valores = list(valid_kwargs.values())
            valores.append(oc_id)
            
            sql = f"UPDATE ordenes_compra SET {set_clause} WHERE id = ?"
            try:
                cursor.execute(sql, valores)
                conn.commit()
                return True, "Orden de compra actualizada exitosamente."
            except Exception as e:
                return False, f"Error al actualizar la orden de compra: {e}"

    def eliminar_licitacion(self, licitacion_id):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM licitaciones WHERE id = ?", (licitacion_id,))
            conn.commit()
            return True, "Licitación eliminada."

    def eliminar_convenio(self, convenio_id):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM convenios WHERE id = ?", (convenio_id,))
            conn.commit()
            return True, "Convenio eliminado."

    def eliminar_oc(self, oc_id):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ordenes_compra WHERE id = ?", (oc_id,))
            conn.commit()
            return True, "Orden de compra eliminada."