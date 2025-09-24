import sqlite3
import pandas as pd

DB_FILE = 'asistencia.db'
EXCEL_FILE = 'calendario de turnos.xlsx'

print(f"Iniciando la importación desde '{EXCEL_FILE}' hacia '{DB_FILE}'...")
conn = sqlite3.connect(DB_FILE)
conn.row_factory = sqlite3.Row

try:
    print("✅ Conexión a la base de datos exitosa.")
    
    # --- ¡ESTA ES LA LÓGICA MEJORADA! ---
    # 1. Leemos todo el archivo Excel sin asumir ninguna fila de encabezado.
    df = pd.read_excel(EXCEL_FILE, header=None)
    
    # --- AJUSTE CRÍTICO ---
    # 2. Asumimos que los nombres de los turnos están en la TERCERA fila del Excel.
    #    En programación, la tercera fila tiene el índice 2 (se empieza a contar desde 0).
    #    Si tus encabezados estuvieran en la cuarta fila, solo tendrías que cambiar el 2 por un 3.
    header_row_index = 2
    
    # 3. Establecemos esa fila como los nuevos encabezados de nuestra tabla.
    df.columns = df.iloc[header_row_index]
    
    # 4. Eliminamos todas las filas de título que estaban antes, quedándonos solo con los datos.
    df = df.iloc[header_row_index + 1:]
    
    # 5. Renombramos la primera columna (la de fechas) a un nombre estándar ('Fecha').
    #    Tomamos el nombre que tenga ahora la primera columna y lo reemplazamos.
    date_column_name = df.columns[0]
    df.rename(columns={date_column_name: 'Fecha'}, inplace=True)
    
    print("✅ Excel leído y reestructurado correctamente.")

    # 6. Obtener un mapa de los nombres de turno a sus IDs desde la BD
    turnos_map = {row['nombre']: row['id'] for row in conn.execute('SELECT id, nombre FROM turnos').fetchall()}
    
    # 7. Transformar la tabla de formato "ancho" a "largo"
    df_largo = df.melt(id_vars=['Fecha'], var_name='turno_nombre', value_name='codigo')
    
    # 8. Reemplazar los nombres de turno por sus IDs
    df_largo['turno_id'] = df_largo['turno_nombre'].map(turnos_map)
    
    # 9. Formatear la fecha y seleccionar las columnas finales
    df_largo['fecha'] = pd.to_datetime(df_largo['Fecha']).dt.strftime('%Y-%m-%d')
    df_final = df_largo[['turno_id', 'fecha', 'codigo']].dropna(subset=['turno_id', 'codigo'])
    
    print(f"Procesando {len(df_final)} registros para la base de datos...")

    # 10. Insertar los datos en la nueva tabla
    cursor = conn.cursor()
    print("Limpiando datos antiguos del calendario...")
    cursor.execute("DELETE FROM calendario_turnos")
    conn.commit()

    print("Insertando nuevos datos del calendario...")
    df_final.to_sql('calendario_turnos', conn, if_exists='append', index=False)
    
    print("✅ ¡Datos importados exitosamente!")

except Exception as e:
    print(f"❌ Ocurrió un error durante la importación: {e}")
finally:
    conn.close()
    print("🎉 Proceso finalizado.")