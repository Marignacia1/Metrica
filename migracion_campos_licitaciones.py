"""
Script de migración para agregar campos faltantes a las tablas de licitaciones.
Este script es seguro de ejecutar múltiples veces.
"""
import sqlite3
from datetime import datetime

def migrar_campos_licitaciones():
    """Agrega todos los campos faltantes a las tablas de licitaciones"""
    conn = sqlite3.connect('compras.db')
    cursor = conn.cursor()

    try:
        print("🔄 Iniciando migración de campos de licitaciones...")
        print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        # --- TABLA LICITACIONES ---
        print("📋 Migrando tabla 'licitaciones'...")
        cursor.execute("PRAGMA table_info(licitaciones)")
        columnas_existentes = {col[1] for col in cursor.fetchall()}
        campos_licitaciones = [
            ("estado_general", "TEXT DEFAULT 'VIGENTE'"),
            ("decreto_adjudicacion", "TEXT"),
            ("inspector_tecnico", "TEXT"),
        ]
        for campo, tipo in campos_licitaciones:
            if campo not in columnas_existentes:
                cursor.execute(f"ALTER TABLE licitaciones ADD COLUMN {campo} {tipo}")
                print(f"  ✅ Agregado campo '{campo}' a 'licitaciones'")
            else:
                print(f"  ℹ️  Campo '{campo}' ya existe en 'licitaciones'")

        # --- TABLA CONVENIOS ---
        print("\n📋 Migrando tabla 'convenios'...")
        cursor.execute("PRAGMA table_info(convenios)")
        columnas_existentes = {col[1] for col in cursor.fetchall()}
        campos_convenios = [
            ("fecha_inicio", "TEXT"),
            ("fecha_termino", "TEXT"),
            ("meses", "INTEGER"),
            ("id_gestion_contratos", "TEXT"),
            ("tiene_ipc", "TEXT DEFAULT 'NO'"),
            ("garantia", "TEXT"),
            ("decreto_aprueba_contrato", "TEXT"),
            ("id_mercado_publico", "TEXT"),
            ("direccion_proveedor", "TEXT"),
            ("telefono_proveedor", "TEXT"),
            ("correo_proveedor", "TEXT"),
            ("inicio_contrato", "TEXT"),
        ]
        for campo, tipo in campos_convenios:
            if campo not in columnas_existentes:
                cursor.execute(f"ALTER TABLE convenios ADD COLUMN {campo} {tipo}")
                print(f"  ✅ Agregado campo '{campo}' a 'convenios'")
            else:
                print(f"  ℹ️  Campo '{campo}' ya existe en 'convenios'")

        # --- TABLA ORDENES_COMPRA ---
        print("\n📋 Migrando tabla 'ordenes_compra'...")
        cursor.execute("PRAGMA table_info(ordenes_compra)")
        columnas_existentes = {col[1] for col in cursor.fetchall()}
        campos_ocs = [
            ("fecha_emision", "TEXT"),
            ("estado", "TEXT DEFAULT 'ACTIVA'"),
        ]
        for campo, tipo in campos_ocs:
            if campo not in columnas_existentes:
                cursor.execute(f"ALTER TABLE ordenes_compra ADD COLUMN {campo} {tipo}")
                print(f"  ✅ Agregado campo '{campo}' a 'ordenes_compra'")
            else:
                print(f"  ℹ️  Campo '{campo}' ya existe en 'ordenes_compra'")

        conn.commit()
        print("\n✅ Migración completada exitosamente.")

    except Exception as e:
        print(f"\n❌ Error durante la migración: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrar_campos_licitaciones()