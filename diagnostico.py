# diagnostico.py
import pandas as pd

EXCEL_FILE = 'calendario de turnos.xlsx'

try:
    df = pd.read_excel(EXCEL_FILE)
    print("--- Nombres de las Columnas Encontradas ---")
    print(df.columns.tolist())
    print("-----------------------------------------")
    print("\nPor favor, copia la lista de arriba y pégala en nuestra conversación.")
except FileNotFoundError:
    print(f"Error: No se encontró el archivo '{EXCEL_FILE}'. Asegúrate de que está en la carpeta del proyecto.")
except Exception as e:
    print(f"Ocurrió un error al leer el archivo: {e}")