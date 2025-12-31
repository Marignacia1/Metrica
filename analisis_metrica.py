import pandas as pd
import requests

def obtener_valores_actuales():
    """Obtiene los valores de UF, Dólar y UTM del día."""
    try:
        # Usamos la API de mindicador.cl (muy confiable en Chile)
        data = requests.get("https://mindicador.cl/api").json()
        return {
            'UF': data['uf']['valor'],
            'USD': data['dolar']['valor'],
            'UTM': data['utm']['valor'],
            'CLP': 1
        }
    except:
        # Valores de respaldo por si falla el internet (ajustar según fecha)
        return {'UF': 38500, 'USD': 980, 'UTM': 67000, 'CLP': 1}

def procesar_financiero_monedas(df):
    valores = obtener_valores_actuales()
    
    # 1. Identificamos la columna de moneda. 
    # Si no existe, la buscamos por palabras clave en el nombre
    if 'Moneda' not in df.columns:
        df['Moneda'] = 'CLP' # Por defecto
        if 'Nombre' in df.columns:
            df.loc[df['Nombre'].str.contains('UF', case=False, na=False), 'Moneda'] = 'UF'
            df.loc[df['Nombre'].str.contains('USD|DOLAR', case=False, na=False), 'Moneda'] = 'USD'
            df.loc[df['Nombre'].str.contains('UTM', case=False, na=False), 'Moneda'] = 'UTM'

    # 2. Creamos la columna "Total CLP" multiplicando el Total OC por el valor de la moneda
    def calcular_clp(fila):
        moneda = str(fila['Moneda']).upper()
        monto = fila['Total OC']
        
        # Si el monto es menor a 5000 y dice CLP, es MUY probable que sea un error de moneda
        # y que en realidad sea UF o USD. Aquí es donde atrapamos las "5 órdenes raras".
        if monto < 5000 and moneda == 'CLP':
            # Si el monto es muy bajo, lo tratamos como UF por precaución o podrías marcarlo
            return monto * valores['UF'] 
            
        return monto * valores.get(moneda, 1)

    df['Total_Final_CLP'] = df.apply(calcular_clp, axis=1)
    return df
