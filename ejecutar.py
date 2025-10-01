import subprocess
import sys
import os

def instalar_dependencias():
    """Instala las dependencias necesarias desde una lista."""
    dependencias = [
        "streamlit",
        "pandas", 
        "plotly",
        "openpyxl"
    ]
    
    print("📦 Verificando e instalando dependencias...")
    for dep in dependencias:
        print(f"--- Instalando {dep}... ---")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
            print(f"✅ {dep} instalado correctamente.")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ ¡Error! No se pudo instalar '{dep}'. La aplicación podría no funcionar.")
            print(f"   Por favor, intenta instalarlo manualmente con: pip install {dep}")

def ejecutar_app():
    """Ejecuta la aplicación principal de Streamlit."""
    app_file = "app.py"
    if not os.path.exists(app_file):
        print(f"❌ Error: El archivo principal '{app_file}' no se encuentra en esta carpeta.")
        return

    print("\n" + "="*50)
    print("🚀 Ejecutando Sistema KPI Compras...")
    print(f"🖥️  Tu navegador debería abrirse en la siguiente dirección:")
    print(f"   > http://localhost:8501")
    print("⏹️  Para detener la aplicación, presiona Ctrl+C en esta ventana.")
    print("="*50 + "\n")
    
    try:
        subprocess.run([sys.executable, "-m", "streamlit", "run", app_file], check=True)
    except KeyboardInterrupt:
        print("\n👋 Aplicación detenida por el usuario.")
    except Exception as e:
        print(f"❌ Ocurrió un error al ejecutar la aplicación: {e}")

if __name__ == "__main__":
    instalar_dependencias()
    ejecutar_app()