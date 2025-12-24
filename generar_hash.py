from werkzeug.security import generate_password_hash

# --- Define tu contraseña genérica aquí ---
contrasena_generica = "Desam.2025"

# --- Ejecuta este script con: python generar_hash.py ---
print("--- Hash Genérico para usuarios.json ---")
print("Copia la siguiente línea completa (incluidas las comillas):")
print("")
print(f'"{generate_password_hash(contrasena_generica)}"')