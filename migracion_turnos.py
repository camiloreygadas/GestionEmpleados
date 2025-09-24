# migracion_turnos.py
import sqlite3

DB_FILE = 'asistencia.db'
print(f"🔌 Conectando a '{DB_FILE}' para mejorar la tabla de turnos...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

try:
    print("✨ Añadiendo la columna 'fecha_referencia' a la tabla 'turnos'...")
    cursor.execute('ALTER TABLE turnos ADD COLUMN fecha_referencia TEXT')
    print("✅ Columna añadida exitosamente.")
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("🟡 La columna 'fecha_referencia' ya existe. No se realizaron cambios.")
    else:
        raise e

connection.commit()
connection.close()
print("🎉 Migración de turnos completada.")