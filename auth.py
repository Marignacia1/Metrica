import sqlite3
import os
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer

class AuthManager:
    def __init__(self, db_path='compras.db', secret_key=None):
        self.db_path = db_path
        if not secret_key:
            secret_key = os.environ.get('SECRET_KEY', 'una_clave_muy_secreta_123')
        self.serializer = URLSafeTimedSerializer(secret_key)
        
        # --- PERMISOS DEFINITIVOS ---
        self.roles_permisos = {
            'admin': {
                'ver_menu_admin': True, 
                'cargar_archivos': True, 
                'modificar_analisis': True, 
                'generar_informes': True,
                'gestionar_usuarios': True # SOLO ADMIN
            },
            'operador': {
                'ver_menu_admin': True,  # Puede ver menú config para cargar archivos
                'cargar_archivos': True, 
                'modificar_analisis': True, 
                'generar_informes': True,
                'gestionar_usuarios': False # NO PUEDE ver perfiles
            },
            'jefe': {
                'ver_menu_admin': False, # NO VE el menú de configuración
                'cargar_archivos': False, 
                'modificar_analisis': True, 
                'generar_informes': True,
                'gestionar_usuarios': False # NO PUEDE ver perfiles
            }
        }

        self._inicializar_base_de_datos()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _inicializar_base_de_datos(self):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS usuarios (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    rol TEXT NOT NULL,
                    nombre_completo TEXT
                )
            ''')
            
            columnas = [
                ('email', 'TEXT'), ('phone', 'TEXT'), ('bio', 'TEXT'),
                ('habilidades', 'TEXT'), ('fecha_nacimiento', 'TEXT'),
                ('frase_motivacional', 'TEXT'), ('contacto_emergencia', 'TEXT'),
                ('foto', 'TEXT'), ('theme_mode', "TEXT DEFAULT 'light'"),
                ('primary_color', "TEXT DEFAULT '#2563EB'"),
                ('font_family', "TEXT DEFAULT 'Inter'"),
                ('force_password_change', 'INTEGER DEFAULT 1')
            ]
            
            for nombre, tipo in columnas:
                try:
                    cursor.execute(f"ALTER TABLE usuarios ADD COLUMN {nombre} {tipo}")
                except sqlite3.OperationalError: pass
            conn.commit()

    def login(self, username, password):
        with self._get_conn() as conn:
            user = conn.execute("SELECT * FROM usuarios WHERE username = ?", (username,)).fetchone()
            if user and check_password_hash(user['password_hash'], password):
                user_dict = dict(user)
                user_dict['permisos'] = self.roles_permisos.get(user['rol'], self.roles_permisos['jefe'])
                return user_dict
        return None

    def change_password(self, username, new_password):
        pw_hash = generate_password_hash(new_password)
        with self._get_conn() as conn:
            conn.execute("UPDATE usuarios SET password_hash = ?, force_password_change = 0 WHERE username = ?", (pw_hash, username))
            conn.commit()
        return True

    def update_user_profile(self, username, email, phone, **kwargs):
        with self._get_conn() as conn:
            campos = ["email = ?", "phone = ?"]
            valores = [email, phone]
            for key, value in kwargs.items():
                campos.append(f"{key} = ?")
                valores.append(value)
            valores.append(username)
            conn.execute(f"UPDATE usuarios SET {', '.join(campos)} WHERE username = ?", valores)
            conn.commit()
        return True, "Perfil actualizado"

    def update_user_settings(self, username, theme_mode, primary_color, font_family='Inter'):
        with self._get_conn() as conn:
            conn.execute("UPDATE usuarios SET theme_mode = ?, primary_color = ?, font_family = ? WHERE username = ?", 
                         (theme_mode, primary_color, font_family, username))
            conn.commit()
        return True

    def get_all_users_details(self):
        with self._get_conn() as conn:
            users = conn.execute("SELECT username, nombre_completo, email, rol, phone FROM usuarios").fetchall()
            return [dict(u) for u in users]

    def crear_usuario(self, username, email, password, nombre, rol='operador'):
        pw_hash = generate_password_hash(password)
        try:
            with self._get_conn() as conn:
                conn.execute('INSERT INTO usuarios (username, email, password_hash, nombre_completo, rol, force_password_change) VALUES (?, ?, ?, ?, ?, 1)', 
                             (username, email, pw_hash, nombre, rol))
                conn.commit()
            return True, "Usuario creado"
        except sqlite3.IntegrityError:
            return False, "El usuario ya existe"

    def admin_reset_password(self, username, new_password):
        pw_hash = generate_password_hash(new_password)
        try:
            with self._get_conn() as conn:
                conn.execute("UPDATE usuarios SET password_hash = ?, force_password_change = 1 WHERE username = ?", (pw_hash, username))
                conn.commit()
            return True, "Contraseña actualizada"
        except Exception as e:
            return False, str(e)

    def find_user_by_email(self, email):
        with self._get_conn() as conn:
            user = conn.execute("SELECT * FROM usuarios WHERE email = ?", (email,)).fetchone()
            return (user['username'], dict(user)) if user else (None, None)

    def generate_reset_token(self, email):
        return self.serializer.dumps(email, salt='password-reset-salt')

    def verify_reset_token(self, token, max_age_sec=3600):
        try:
            return self.serializer.loads(token, salt='password-reset-salt', max_age=max_age_sec)
        except: return None
