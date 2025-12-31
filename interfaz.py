import tkinter as tk
from tkinter import simpledialog, messagebox
import pandas as pd
import requests

def obtener_valores_conversion():
    """Trae los valores del día para UF, Dólar y UTM."""
    try:
        r = requests.get("https://mindicador.cl/api").json()
        return {'UF': r['uf']['valor'], 'USD': r['dolar']['valor'], 'UTM': r['utm']['valor'], 'CLP': 1}
    except:
        # Valores por defecto si no hay internet
        return {'UF': 38000, 'USD': 950, 'UTM': 66000, 'CLP': 1}

def validar_ordenes_sospechosas(df):
    indicadores = obtener_valores_conversion()
    umbral = 10000 # Cualquier orden menor a 10.000 pesos es "sospechosa"
    
    # Buscamos las órdenes que cumplen el criterio
    sospechosas = df[df['Total OC'] < umbral]
    
    if sospechosas.empty:
        df['Total_Final_CLP'] = df['Total OC']
        return df

    messagebox.showwarning("Atención", f"Se detectaron {len(sospechosas)} órdenes con montos muy bajos. Debes asignarles la moneda correcta.")

    # Lista para guardar los nuevos montos
    montos_corregidos = []

    for index, row in df.iterrows():
        monto_original = row['Total OC']
        nombre_oc = row['Nombre de la OC']
        id_oc = row['N° orden de compra']
        
        if monto_original < umbral:
            # Crear una ventana de selección
            ventana_opcion = tk.Toplevel()
            ventana_opcion.title("Seleccionar Moneda")
            ventana_opcion.geometry("400x200")
            
            seleccion = tk.StringVar(value="CLP")
            
            tk.Label(ventana_opcion, text=f"Orden: {id_oc}", font=('Arial', 10, 'bold')).pack(pady=5)
            tk.Label(ventana_opcion, text=f"Monto: {monto_original} | Nombre: {nombre_oc[:50]}...", wraplength=350).pack(pady=5)
            
            opciones = [("Peso (CLP)", "CLP"), ("UF", "UF"), ("Dólar (USD)", "USD"), ("UTM", "UTM")]
            for texto, modo in opciones:
                tk.Radiobutton(ventana_opcion, text=texto, variable=seleccion, value=modo).pack(anchor='w', padx=50)
            
            def confirmar():
                ventana_opcion.destroy()

            tk.Button(ventana_opcion, text="Confirmar", command=confirmar, bg="green", fg="white").pack(pady=10)
            
            # Esperar a que el usuario responda
            ventana_opcion.grab_set()
            ventana_opcion.wait_window()
            
            # Aplicar conversión
            moneda_elegida = seleccion.get()
            valor_moneda = indicadores.get(moneda_elegida, 1)
            montos_corregidos.append(monto_original * valor_moneda)
        else:
            # Si es normal, se queda igual
            montos_corregidos.append(monto_original)

    df['Total_Final_CLP'] = montos_corregidos
    messagebox.showinfo("Proceso Completo", "Todas las órdenes han sido normalizadas a CLP.")
    return df
