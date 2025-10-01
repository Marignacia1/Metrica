📊 SISTEMA KPI COMPRAS
=====================

🚀 INSTALACIÓN Y USO
-------------------

1. Abrir PowerShell en esta carpeta
2. Ejecutar: python ejecutar.py
3. Ir a: http://localhost:8501

📋 ARCHIVOS NECESARIOS
----------------------

1. Experto Bruto (Excel/CSV) - REQUERIDO
   - Archivo principal con todos los requerimientos

2. Cancelados-Experto (Excel/CSV) - REQUERIDO  
   - Archivo con requerimientos cancelados

3. Precompra (Excel/CSV) - OPCIONAL
   - Para filtros adicionales de Compra Ágil

🎯 FUNCIONALIDADES
-----------------

✅ Procesamiento automático (replica tu lógica Excel)
✅ Dashboard interactivo con métricas
✅ Gráficos de tendencias y estados
✅ Base de datos SQLite integrada
✅ Vista de registros procesados y en proceso
✅ KPI de eficiencia automático

💡 ESTRUCTURA DE ARCHIVOS
------------------------

app.py - Aplicación principal
ejecutar.py - Script de instalación y ejecución
compras.db - Base de datos (se crea automáticamente)

🔧 SOLUCIÓN DE PROBLEMAS
-----------------------

Si "python ejecutar.py" no funciona:

1. python -m pip install streamlit pandas plotly openpyxl
2. python -m streamlit run app.py

📞 SOPORTE
----------

Si tienes problemas, verifica:
- Python instalado correctamente
- Conexión a internet para instalar dependencias
- Archivos Excel/CSV con formato correcto