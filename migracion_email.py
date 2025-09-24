# migracion_email.py
import sqlite3

DB_FILE = 'asistencia.db'
print(f"🔌 Conectando a la base de datos '{DB_FILE}'...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

try:
    print("✨ Añadiendo la columna 'correo_electronico' a la tabla 'empleados'...")
    # Este comando modifica la tabla sin borrar los datos existentes
    cursor.execute('ALTER TABLE empleados ADD COLUMN correo_electronico TEXT')
    print("✅ Columna añadida exitosamente.")
except sqlite3.OperationalError as e:
    # Esto previene un error si ejecutas el script más de una vez
    if "duplicate column name" in str(e):
        print("🟡 La columna 'correo_electronico' ya existe. No se realizaron cambios.")
    else:
        raise e

connection.commit()
connection.close()
print("🎉 Migración completada.")