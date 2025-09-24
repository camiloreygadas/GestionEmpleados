# migracion_calendario.py
import sqlite3

DB_FILE = 'asistencia.db'
print(f"🔌 Conectando a '{DB_FILE}' para crear la tabla de calendario...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

try:
    print("✨ Creando la tabla 'calendario_turnos'...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS calendario_turnos (
            turno_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            codigo TEXT NOT NULL,
            PRIMARY KEY (turno_id, fecha),
            FOREIGN KEY (turno_id) REFERENCES turnos (id)
        )
    ''')
    print("✅ Tabla 'calendario_turnos' creada o ya existente.")
except Exception as e:
    print(f"❌ Ocurrió un error: {e}")

connection.commit()
connection.close()
print("🎉 Migración de calendario completada.")