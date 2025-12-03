import json
import os
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature

class AuthManager:
    """
    Gestor de autenticación y permisos para Flask.
    
    Maneja:
    - Carga de usuarios desde JSON.
    - Validación de contraseñas con hash (werkzeug.security).
    - Cambio de contraseña forzado.
    - Reseteo de contraseña por administrador.
    - Generación y verificación de tokens para auto-recuperación (itsdangerous).
    """

    def __init__(self, usuarios_file='usuarios.json', secret_key=None):
        """
        Inicializa el gestor.
        
        Args:
            usuarios_file (str): Ruta al archivo JSON de usuarios.
            secret_key (str): La SECRET_KEY de la app Flask, necesaria para
                              generar tokens seguros.
        """
        self.usuarios_file = usuarios_file
        self.usuarios_data = self._cargar_usuarios()
        
        # Fallback por si no se pasa una secret_key (aunque siempre debería pasarse)
        if not secret_key:
            print("ADVERTENCIA: No se proporcionó una SECRET_KEY a AuthManager. Usando una clave temporal no segura.")
            secret_key = os.urandom(24)
            
        # Inicializa el serializador para crear tokens
        self.serializer = URLSafeTimedSerializer(secret_key)

    def _cargar_usuarios(self):
        """Carga los usuarios desde el archivo JSON."""
        try:
            with open(self.usuarios_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error cargando {self.usuarios_file}: {e}")
            return {"usuarios": {}, "roles_permisos": {}}
    
    def _guardar_usuarios(self, data):
        """
        Guarda de forma segura (atómica) el diccionario de usuarios en el archivo JSON.
        """
        try:
            temp_file = self.usuarios_file + ".tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Si la escritura fue exitosa, renombra el temporal al archivo real
            os.replace(temp_file, self.usuarios_file)
            
            # Actualiza los datos en memoria
            self.usuarios_data = data
            return True, "Datos guardados."
        except Exception as e:
            print(f"Error fatal al guardar {self.usuarios_file}: {e}")
            return False, f"Error al guardar datos: {e}"

    def login(self, username, password_attempt):
        """
        Autentica un usuario usando hashes seguros.
        Devuelve los datos del usuario si es exitoso, incluyendo el flag 
        'force_password_change'.
        """
        if not self.usuarios_data:
            return None

        usuario_info = self.usuarios_data['usuarios'].get(username.lower())

        # Verifica que el usuario exista y que el hash de la contraseña coincida
        if usuario_info and check_password_hash(usuario_info.get('password_hash', ''), password_attempt):
            rol = usuario_info.get('rol')
            permisos = self.usuarios_data['roles_permisos'].get(rol, {})
            
            return {
                'username': username.lower(),
                'nombre_completo': usuario_info.get('nombre_completo'),
                'rol': rol,
                'permisos': permisos,
                'force_password_change': usuario_info.get('force_password_change', False)
            }
        
        return None # Falla el login

    def change_password(self, username, new_password):
        """
        Actualiza la contraseña de un usuario (hasheada) y desactiva el flag 
        'force_password_change'.
        """
        username = username.lower()
        
        # Carga los datos más actuales antes de modificar
        current_data = self._cargar_usuarios()
        
        if username not in current_data['usuarios']:
            return False, "Usuario no encontrado."

        try:
            new_hash = generate_password_hash(new_password)
            
            current_data['usuarios'][username]['password_hash'] = new_hash
            current_data['usuarios'][username]['force_password_change'] = False
            
            # Usa el método seguro de guardado
            success, message = self._guardar_usuarios(current_data)
            return success, message
            
        except Exception as e:
            print(f"Error en change_password: {e}")
            return False, f"Error al procesar la contraseña: {e}"

    def get_all_users_details(self):
        """Devuelve el diccionario completo de usuarios."""
        return self.usuarios_data.get('usuarios', {})

    def admin_reset_password(self, username_to_reset, new_generic_password):
        """
        (Admin) Reinicia la contraseña de un usuario a una genérica y
        fuerza el cambio en el próximo inicio de sesión.
        """
        username_to_reset = username_to_reset.lower()
        
        current_data = self._cargar_usuarios()
        
        if username_to_reset not in current_data['usuarios']:
            return False, "Usuario no encontrado."

        try:
            new_hash = generate_password_hash(new_generic_password)
            
            current_data['usuarios'][username_to_reset]['password_hash'] = new_hash
            current_data['usuarios'][username_to_reset]['force_password_change'] = True
            
            success, message = self._guardar_usuarios(current_data)
            if success:
                return True, f"Contraseña de '{username_to_reset}' reiniciada."
            else:
                return False, message
            
        except Exception as e:
            print(f"Error en admin_reset_password: {e}")
            return False, f"Error al guardar la contraseña: {e}"

    # --- Funciones para Auto-Recuperación ---

    def find_user_by_email(self, email):
        """
        Busca un usuario por su email y devuelve su username (key) y datos.
        """
        if not self.usuarios_data.get('usuarios'):
            return None, None
            
        for username, details in self.usuarios_data['usuarios'].items():
            if details.get('email', '').lower() == email.lower():
                return username, details
        
        return None, None # No se encontró

    def generate_reset_token(self, email):
        """
        Genera un token de reseteo firmado con el email.
        """
        # "password-reset-salt" es una "sal" criptográfica específica para esta operación
        return self.serializer.dumps(email, salt='password-reset-salt')

    def verify_reset_token(self, token, max_age_sec=3600):
        """
        Verifica el token. Devuelve el email si es válido y no ha expirado.
        max_age_sec = 3600 (1 hora)
        """
        try:
            # Verifica el token usando la misma "sal"
            email = self.serializer.loads(
                token, 
                salt='password-reset-salt', 
                max_age=max_age_sec
            )
            return email
        except (SignatureExpired):
            print("Token de reseteo expiró.")
            return None
        except (BadTimeSignature, Exception) as e:
            print(f"Token de reseteo inválido: {e}")
            return None