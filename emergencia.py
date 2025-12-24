import json
from werkzeug.security import generate_password_hash

# 1. Definimos la nueva clave simple: 1234
nueva_clave = "1234"
hash_nuevo = generate_password_hash(nueva_clave)

print(f"Generando hash para: {nueva_clave}...")

# 2. Intentamos leer el archivo actual
try:
    with open('usuarios.json', 'r') as f:
        data = json.load(f)
except Exception as e:
    print("No se pudo leer el archivo, creando uno nuevo.")
    data = {"usuarios": {}, "roles_permisos": {}}

# 3. FORZAMOS al usuario admin
data['usuarios']['admin'] = {
    "password_hash": hash_nuevo,
    "rol": "operador",
    "nombre_completo": "Admin de Rescate",
    "force_password_change": True,  # Te pedirá cambiarla al entrar
    "email": "admin@rescate.com"
}

# 4. Guardamos el archivo arreglado
with open('usuarios.json', 'w') as f:
    json.dump(data, f, indent=2)

print("\n✅ ¡ÉXITO! Base de datos de usuarios actualizada.")
print(f"👉 Usuario: admin")
print(f"👉 Contraseña: {nueva_clave}")
print("\n⚠️  AHORA DEBES REINICIAR EL SERVIDOR (flask_app.py) PARA QUE FUNCIONE.")