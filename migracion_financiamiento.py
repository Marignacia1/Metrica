import sqlite3

def agregar_columna(tabla, columna):
    """Añade una columna a una tabla si no existe."""
    conn = sqlite3.connect('compras.db')
    cursor = conn.cursor()

    try:
        # Revisa las columnas existentes
        cursor.execute(f"PRAGMA table_info({tabla})")
        columnas = [col[1] for col in cursor.fetchall()]

        if columna in columnas:
            print(f"✅ La columna '{columna}' ya existe en la tabla '{tabla}'. No se necesita migración.")
        else:
            # Si no existe, la añade
            print(f"🔄 Añadiendo columna '{columna}' a la tabla '{tabla}'...")
            cursor.execute(f"ALTER TABLE {tabla} ADD COLUMN {columna} TEXT")
            conn.commit()
            print(f"¡Éxito! Columna '{columna}' añadida a '{tabla}'.")

    except Exception as e:
        print(f"❌ Error al migrar la tabla '{tabla}': {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("🚀 Iniciando migración de base de datos...")
    agregar_columna('en_proceso', 'tipo_financiamiento')
    agregar_columna('procesados', 'tipo_financiamiento')
    print("✅ Migración completada.")