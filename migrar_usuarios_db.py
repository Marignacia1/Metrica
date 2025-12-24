import sqlite3
import json

def migrar_usuarios():
    # 1. Conectar a la BD
    conn = sqlite3.connect('compras.db')
    cursor = conn.cursor()

    # 2. Crear tabla usuarios
    print("🔨 Creando tabla usuarios...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            nombre_completo TEXT,
            rol TEXT DEFAULT 'operador',
            fecha_cambio_password DATETIME DEFAULT CURRENT_TIMESTAMP,
            force_password_change BOOLEAN DEFAULT 0
        )
    ''')

    # 3. Leer usuarios del JSON actual
    try:
        with open('usuarios.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            usuarios_json = data.get('usuarios', {})
            
            print(f"📂 Encontrados {len(usuarios_json)} usuarios en JSON.")

            for username, details in usuarios_json.items():
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO usuarios 
                        (username, email, password_hash, nombre_completo, rol, force_password_change)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        username, 
                        details.get('email'), 
                        details.get('password_hash'),
                        details.get('nombre_completo'),
                        details.get('rol'),
                        details.get('force_password_change', 0)
                    ))
                    print(f"  ✅ Usuario migrado: {username}")
                except Exception as e:
                    print(f"  ❌ Error migrando {username}: {e}")

            conn.commit()
            print("\n✨ Migración completada exitosamente.")

    except FileNotFoundError:
        print("⚠️ No se encontró usuarios.json, se creó la tabla vacía.")
    finally:
        conn.close()

if __name__ == "__main__":
    migrar_usuarios()