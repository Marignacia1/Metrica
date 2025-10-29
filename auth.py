import json

class AuthManager:
    """
    Gestor de autenticación y permisos para Flask.
    Lee el archivo de usuarios y valida las credenciales.
    """

    def __init__(self, usuarios_file='usuarios.json'):
        self.usuarios_file = usuarios_file
        self.usuarios_data = self._cargar_usuarios()

    def _cargar_usuarios(self):
        """Carga los usuarios desde el archivo JSON."""
        try:
            with open(self.usuarios_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            # En caso de error, retorna una estructura vacía para evitar que la app se caiga.
            return {"usuarios": {}, "roles_permisos": {}}

    def login(self, username, password):
        """
        Autentica un usuario y devuelve sus datos si la validación es exitosa.

        Returns:
            dict: Datos del usuario (incluyendo permisos) si es exitoso, None si falla.
        """
        if not self.usuarios_data:
            return None

        # Busca el usuario (insensible a mayúsculas/minúsculas)
        usuario_info = self.usuarios_data['usuarios'].get(username.lower())

        # Verifica que el usuario exista y la contraseña coincida
        if usuario_info and usuario_info['password'] == password:
            rol = usuario_info['rol']
            permisos = self.usuarios_data['roles_permisos'].get(rol, {})
            
            return {
                'username': username.lower(),
                'nombre_completo': usuario_info['nombre_completo'],
                'rol': rol,
                'permisos': permisos  # Añadimos los permisos directamente al diccionario del usuario
            }

        return None