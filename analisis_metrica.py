import pandas as pd
import requests
import tkinter as tk
from tkinter import messagebox

class AnalisisMetrica:
    def __init__(self):
        self.indicadores = self.obtener_indicadores()

    def obtener_indicadores(self):
        """Obtiene valores actualizados de UF, Dólar y UTM desde mindicador.cl"""
        try:
            url = 'https://mindicador.cl/api'
            response = requests.get(url).json()
            return {
                'UF': response['uf']['valor'],
                'USD': response['dolar']['valor'],
                'UTM': response['utm']['valor'],
                'CLP': 1
            }
        except Exception as e:
            print(f"Aviso: No se pudo conectar a la API ({e}). Usando valores base.")
            return {'UF': 38500, 'USD': 980, 'UTM': 67500, 'CLP': 1}

    def cargar_datos(self, ruta_archivo):
        """Lee el archivo CSV o Excel."""
        if ruta_archivo.endswith('.csv'):
            return pd.read_csv(ruta_archivo)
        else:
            return pd.read_excel(ruta_archivo)

    def pedir_confirmacion_moneda(self, index, fila):
        """
        Crea una ventana emergente para que el usuario elija 
        la moneda de una orden específica.
        """
        ventana = tk.Toplevel()
        ventana.title(f"Validar Moneda - OC {fila['N° orden de compra']}")
        ventana.geometry("450x250")
        ventana.configure(padx=20, pady=20)
        
        # Bloquear interacción con la ventana principal hasta responder
        ventana.grab_set()

        tk.Label(ventana, text="Monto sospechosamente bajo detectado", font=('Arial', 10, 'bold'), fg="red").pack()
        tk.Label(ventana, text=f"Orden: {fila['N° orden de compra']}", wraplength=400).pack(pady=5)
        tk.Label(ventana, text=f"Nombre: {fila['Nombre de la OC'][:60]}...", wraplength=400, fg="gray").pack()
        tk.Label(ventana, text=f"Monto original: {fila['Total OC']}", font=('Arial', 10, 'bold')).pack(pady=10)

        seleccion = tk.StringVar(value="CLP")

        # Opciones de moneda
        opciones_frame = tk.Frame(ventana)
        opciones_frame.pack()
        
        opciones = [("Peso (CLP)", "CLP"), ("UF", "UF"), ("Dólar (USD)", "USD"), ("UTM", "UTM")]
        for texto, modo in opciones:
            tk.Radiobutton(opciones_frame, text=texto, variable=seleccion, value=modo).pack(side=tk.LEFT, padx=10)

        resultado = {"moneda": "CLP"}

        def confirmar():
            resultado["moneda"] = seleccion.get()
            ventana.destroy()

        tk.Button(ventana, text="Confirmar y Convertir", command=confirmar, bg="#2ecc71", fg="white", font=('Arial', 10, 'bold')).pack(pady=15)

        # Esperar a que se cierre la ventana
        ventana.wait_window()
        return resultado["moneda"]

    def procesar_metrica(self, df):
        """
        Procesa el DataFrame, identifica montos bajos y 
        solicita intervención humana si es necesario.
        """
        # Umbral: Si es menor a 10.000, sospechamos que no es CLP
        UMBRAL_CLP = 10000
        
        montos_finales = []
        conteo_corregidas = 0

        for index, fila in df.iterrows():
            monto_actual = fila['Total OC']
            
            # Si el monto es bajo, preguntamos al usuario
            if monto_actual < UMBRAL_CLP:
                moneda_elegida = self.pedir_confirmacion_moneda(index, fila)
                valor_conversion = self.indicadores.get(moneda_elegida, 1)
                
                nuevo_monto = monto_actual * valor_conversion
                montos_finales.append(nuevo_monto)
                
                if moneda_elegida != "CLP":
                    conteo_corregidas += 1
            else:
                # Si es un monto normal, asumimos CLP
                montos_finales.append(monto_actual)

        # Añadimos la columna procesada al DataFrame
        df['Total_OC_Convertido'] = montos_finales
        
        if conteo_corregidas > 0:
            messagebox.showinfo("Proceso Finalizado", f"Se corrigieron {conteo_corregidas} órdenes de compra.")
        
        return df

    def generar_resumen(self, df):
        """Cálculos estadísticos básicos para tus métricas."""
        resumen = {
            'Total General (CLP)': df['Total_OC_Convertido'].sum(),
            'Promedio OC': df['Total_OC_Convertido'].mean(),
            'Cantidad OC': len(df)
        }
        return resumen
