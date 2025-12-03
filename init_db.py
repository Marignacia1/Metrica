# init_db.py

import sqlite3

def crear_tablas_iniciales():
    """
    Crea la estructura de 3 tablas (licitaciones, convenios, ordenes_compra)
    necesaria para la gestión manual de licitaciones vigentes.
    """
    try:
        with sqlite3.connect('compras.db') as conn:
            cursor = conn.cursor()

            # --- TABLAS DE PROCESAMIENTO GENERAL (de data_processor) ---
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sesiones (
                    id INTEGER PRIMARY KEY, fecha DATETIME, total_brutos INTEGER,
                    req_procesados INTEGER, req_en_proceso INTEGER, eficiencia REAL,
                    req_cancelados INTEGER DEFAULT 0
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS procesados (
                    id INTEGER PRIMARY KEY, sesion_id INTEGER, numero_req TEXT, titulo TEXT,
                    tipo_compra TEXT, unidad TEXT, comprador TEXT, orden_compra TEXT,
                    FOREIGN KEY (sesion_id) REFERENCES sesiones (id) ON DELETE CASCADE
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS en_proceso (
                    id INTEGER PRIMARY KEY, sesion_id INTEGER, numero_req TEXT, titulo TEXT,
                    tipo_compra TEXT, unidad TEXT, comprador TEXT, estado TEXT,
                    tipo_financiamiento TEXT,
                    FOREIGN KEY (sesion_id) REFERENCES sesiones (id) ON DELETE CASCADE
                )''')

            # --- TABLAS PARA GESTIÓN MANUAL DE LICITACIONES VIGENTES ---
            print("🔧 Verificando Tabla Nivel 1: licitaciones...")
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS licitaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_licitacion TEXT UNIQUE NOT NULL,
                nombre_licitacion TEXT,
                requirente TEXT,
                estado_general TEXT DEFAULT 'VIGENTE',
                decreto_adjudicacion TEXT,
                inspector_tecnico TEXT
            )
            ''')

            print("🔧 Verificando Tabla Nivel 2: convenios (Proveedores)...")
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS convenios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                licitacion_id INTEGER NOT NULL,
                proveedor TEXT NOT NULL,
                rut_proveedor TEXT,
                monto_adjudicado REAL,
                fecha_inicio TEXT,
                fecha_termino TEXT,
                meses INTEGER,
                id_gestion_contratos TEXT,
                tiene_ipc TEXT,
                garantia TEXT,
                decreto_aprueba_contrato TEXT,
                id_mercado_publico TEXT,
                direccion_proveedor TEXT,
                telefono_proveedor TEXT,
                correo_proveedor TEXT,
                inicio_contrato TEXT,
                FOREIGN KEY (licitacion_id) REFERENCES licitaciones(id) ON DELETE CASCADE
            )
            ''')

            print("🔧 Verificando Tabla Nivel 3: ordenes_compra...")
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS ordenes_compra (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                convenio_id INTEGER NOT NULL,
                numero_oc TEXT NOT NULL,
                monto REAL,
                fecha_emision TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (convenio_id) REFERENCES convenios(id) ON DELETE CASCADE
            )
            ''')

            # --- TABLA PARA ANÁLISIS FINANCIERO DE OCs ---
            print("🔧 Verificando Tabla de Análisis Financiero: oc_financiero...")
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS oc_financiero (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sesion_id INTEGER,
                numero_oc TEXT,
                nombre_oc TEXT,
                tipo_compra TEXT,
                estado_oc TEXT,
                unidad TEXT,
                nombre_proveedor TEXT,
                rut_proveedor TEXT,
                fecha_creacion TEXT,
                fecha_envio TEXT,
                monto_neto REAL,
                descuentos REAL,
                cargos REAL,
                iva REAL,
                impuesto_especifico REAL,
                total_oc REAL,
                numero_req TEXT,
                titulo_req TEXT,
                comprador TEXT,
                FOREIGN KEY (sesion_id) REFERENCES sesiones(id) ON DELETE CASCADE
            )
            ''')
            print("🔧 Verificando Tabla de Análisis de Licitaciones...")
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisis_licitaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sesion_id INTEGER,
                id_adquisicion TEXT,
                nombre_adquisicion TEXT,
                estado_licitacion TEXT,
                monto_adjudicado REAL,
                responsable TEXT,
                tipo_licitacion TEXT,
                FOREIGN KEY (sesion_id) REFERENCES sesiones(id) ON DELETE CASCADE
            )
            ''')
            conn.commit()
            print("\n✅ Base de datos verificada y lista.")

    except Exception as e:
        print(f"❌ Ocurrió un error al preparar la base de datos: {e}")