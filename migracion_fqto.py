# migracion_fqto.py
import sqlite3

DB_FILE = 'asistencia.db'
print(f"🔌 Conectando a '{DB_FILE}' para añadir el código de finiquito...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

try:
    print("✨ Añadiendo el código 'FQTO' a la tabla 'codigos_asistencia'...")
    # Usamos INSERT OR IGNORE para no causar un error si ya existe
    cursor.execute("INSERT OR IGNORE INTO codigos_asistencia (codigo, descripcion) VALUES ('FQTO', 'FINIQUITADO')")
    print("✅ Código 'FQTO' añadido o ya existente.")
except Exception as e:
    print(f"❌ Ocurrió un error: {e}")

connection.commit()
connection.close()
print("🎉 Migración de FQTO completada.")