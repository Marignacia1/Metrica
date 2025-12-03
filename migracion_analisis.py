import sqlite3

def migrar_tabla_analisis():
    print("🔄 Iniciando migración de tabla 'analisis_licitaciones'...")
    
    conn = sqlite3.connect('compras.db')
    cursor = conn.cursor()
    
    try:
        # Verificar columnas actuales
        cursor.execute("PRAGMA table_info(analisis_licitaciones)")
        columnas = [col[1] for col in cursor.fetchall()]
        print(f"   📋 Columnas actuales: {columnas}")
        
        # 1. Agregar columna cantidad_lineas
        if 'cantidad_lineas' not in columnas:
            print("   ➕ Agregando columna 'cantidad_lineas'...")
            cursor.execute("ALTER TABLE analisis_licitaciones ADD COLUMN cantidad_lineas INTEGER DEFAULT 0")
        else:
            print("   ✅ Columna 'cantidad_lineas' ya existe.")
            
        # 2. Agregar columna cantidad_ofertas
        if 'cantidad_ofertas' not in columnas:
            print("   ➕ Agregando columna 'cantidad_ofertas'...")
            cursor.execute("ALTER TABLE analisis_licitaciones ADD COLUMN cantidad_ofertas INTEGER DEFAULT 0")
        else:
            print("   ✅ Columna 'cantidad_ofertas' ya existe.")

        # 3. Agregar columna monto_estimado (¡ESTA ES LA QUE FALTABA!)
        if 'monto_estimado' not in columnas:
            print("   ➕ Agregando columna 'monto_estimado'...")
            cursor.execute("ALTER TABLE analisis_licitaciones ADD COLUMN monto_estimado REAL DEFAULT 0")
        else:
            print("   ✅ Columna 'monto_estimado' ya existe.")

        conn.commit()
        print("\n✅ Migración completada exitosamente.")
        
    except Exception as e:
        print(f"\n❌ Error durante la migración: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrar_tabla_analisis()  