import sqlite3

# Conectar a la base de datos
conn = sqlite3.connect('compras.db')
cursor = conn.cursor()

# Ver todas las tablas
print("=== TABLAS EN LA BASE DE DATOS ===")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
for table in tables:
    print(f"  - {table[0]}")
print()

# Ver licitaciones
print("=== LICITACIONES ===")
try:
    cursor.execute("SELECT * FROM licitaciones")
    licitaciones = cursor.fetchall()
    print(f"Total licitaciones: {len(licitaciones)}")
    if licitaciones:
        cursor.execute("PRAGMA table_info(licitaciones)")
        columnas = [col[1] for col in cursor.fetchall()]
        print(f"Columnas: {', '.join(columnas)}")
        for lic in licitaciones:
            print(f"  {lic}")
    else:
        print("  (vacía)")
except Exception as e:
    print(f"Error: {e}")
print()

# Ver convenios
print("=== CONVENIOS ===")
try:
    cursor.execute("SELECT * FROM convenios")
    convenios = cursor.fetchall()
    print(f"Total convenios: {len(convenios)}")
    if convenios:
        cursor.execute("PRAGMA table_info(convenios)")
        columnas = [col[1] for col in cursor.fetchall()]
        print(f"Columnas: {', '.join(columnas)}")
        for conv in convenios:
            print(f"  {conv}")
    else:
        print("  (vacía)")
except Exception as e:
    print(f"Error: {e}")
print()

# Ver ordenes de compra
print("=== ORDENES DE COMPRA ===")
try:
    cursor.execute("SELECT * FROM ordenes_compra")
    ocs = cursor.fetchall()
    print(f"Total OCs: {len(ocs)}")
    if ocs:
        cursor.execute("PRAGMA table_info(ordenes_compra)")
        columnas = [col[1] for col in cursor.fetchall()]
        print(f"Columnas: {', '.join(columnas)}")
        for oc in ocs:
            print(f"  {oc}")
    else:
        print("  (vacía)")
except Exception as e:
    print(f"Error: {e}")
print()

# Ver estructura de las tablas
print("=== ESTRUCTURA DETALLADA ===")
for tabla in ['licitaciones', 'convenios', 'ordenes_compra']:
    print(f"\n{tabla.upper()}:")
    try:
        cursor.execute(f"PRAGMA table_info({tabla})")
        columnas = cursor.fetchall()
        for col in columnas:
            print(f"  {col[1]:30} {col[2]:15} {'NOT NULL' if col[3] else ''}")
    except Exception as e:
        print(f"  Error: {e}")

conn.close()
print("\n✅ Revisión completada")