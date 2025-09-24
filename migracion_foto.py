# migracion_foto.py
import sqlite3

DB_FILE = 'asistencia.db'
print(f"🔌 Conectando a '{DB_FILE}' para añadir la columna de foto de perfil...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

try:
    print("✨ Añadiendo la columna 'foto_url' a la tabla 'empleados'...")
    
    # Este comando añade la nueva columna.
    # Es seguro ejecutarlo varias veces, dará un error si la columna ya existe,
    # pero no dañará tus datos.
    cursor.execute('ALTER TABLE empleados ADD COLUMN foto_url TEXT;')

    print("✅ Columna 'foto_url' añadida exitosamente.")

except sqlite3.OperationalError as e:
    # Este error es normal si la columna ya existe.
    if "duplicate column name" in str(e):
        print("⚠️ La columna 'foto_url' ya existe en la tabla.")
    else:
        print(f"❌ Ocurrió un error de SQL: {e}")
except Exception as e:
    print(f"❌ Ocurrió un error inesperado: {e}")

connection.commit()
connection.close()
print("🎉 Migración de foto de perfil completada.")