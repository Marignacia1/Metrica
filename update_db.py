import sqlite3
with sqlite3.connect('compras.db') as conn:
    cursor = conn.cursor()
    # Añadimos columnas para control de moneda
    cursor.execute("ALTER TABLE oc_financiero ADD COLUMN moneda_origen TEXT DEFAULT 'CLP'")
    cursor.execute("ALTER TABLE oc_financiero ADD COLUMN validado_manual INTEGER DEFAULT 0")
    cursor.execute("ALTER TABLE oc_financiero ADD COLUMN valor_nominal REAL")
