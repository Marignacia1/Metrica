import sqlite3

# Cambia 'admin' por el nombre de usuario exacto que estás usando
USUARIO_A_ASCENDER = 'admin' 

try:
    with sqlite3.connect('compras.db') as conn:
        cursor = conn.cursor()
        
        # 1. Verificamos qué rol tiene actualmente
        cursor.execute("SELECT rol FROM usuarios WHERE username = ?", (USUARIO_A_ASCENDER,))
        resultado = cursor.fetchone()
        
        if resultado:
            print(f"El usuario '{USUARIO_A_ASCENDER}' tiene actualmente el rol: {resultado[0]}")
            
            # 2. Lo actualizamos a admin
            cursor.execute("UPDATE usuarios SET rol = 'admin' WHERE username = ?", (USUARIO_A_ASCENDER,))
            conn.commit()
            print(f"✅ ¡ÉXITO! El usuario '{USUARIO_A_ASCENDER}' ahora es 'admin'.")
            print("Cierra sesión y vuelve a entrar para ver los cambios.")
        else:
            print(f"❌ Error: El usuario '{USUARIO_A_ASCENDER}' no existe en la base de datos.")
            
except Exception as e:
    print(f"Error conectando a la BD: {e}")