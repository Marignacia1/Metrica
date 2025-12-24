import queue
import sqlite3

class NotificationManager:
    """Maneja notificaciones en tiempo real (SSE)."""
    def __init__(self):
        self.listeners = []

    def listen(self):
        q = queue.Queue(maxsize=5)
        self.listeners.append(q)
        return q

    def broadcast(self, msg):
        for i in reversed(range(len(self.listeners))):
            try:
                self.listeners[i].put_nowait(msg)
            except queue.Full:
                del self.listeners[i]

class LicitacionesManager:
    """Gestor de base de datos para licitaciones."""
    def obtener_licitaciones_completas(self):
        # Retorna lista vacía si no hay datos para evitar errores
        return []
    
    def agregar_licitacion(self, *args, **kwargs):
        return True, "Licitación agregada (Simulación)"
    
    def actualizar_licitacion(self, id, **kwargs):
        return True, "Actualizado (Simulación)"

    def eliminar_licitacion(self, id):
        return True, "Eliminado"

    # Stubs para convenios/OCs para que la API no falle
    def agregar_convenio(self, *args, **kwargs): return True, "Ok"
    def actualizar_convenio(self, *args, **kwargs): return True, "Ok"
    def eliminar_convenio(self, *args, **kwargs): return True, "Ok"
    def agregar_oc(self, *args, **kwargs): return True, "Ok"
    def actualizar_oc(self, *args, **kwargs): return True, "Ok"
    def eliminar_oc(self, *args, **kwargs): return True, "Ok"

class ComprasProcessor:
    """Procesador de archivos Excel."""
    def procesar_datos(self, df_experto, df_cancelados, df_precompra=None):
        return {
            'success': True,
            'messages': [{'text': 'Archivos procesados correctamente (Simulación)', 'category': 'success'}]
        }
    
    def procesar_analisis_financiero(self, df_resultado, df_historico, sesion_id=None, tipo_filtro=None):
        return {
            'success': True,
            'messages': [{'text': 'Análisis financiero completado', 'category': 'success'}],
            'df_analisis': df_resultado, # Devuelve el mismo DF para graficar
            'unmatched_ocs': []
        }