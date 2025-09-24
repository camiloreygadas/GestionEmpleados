import sqlite3
import os

# Nombre del archivo de la base de datos
DB_FILE = 'asistencia.db'

# --- Borra la base de datos anterior si existe ---
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print(f"Base de datos '{DB_FILE}' anterior eliminada.")

print(f"🔌 Creando nueva base de datos '{DB_FILE}'...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()
print("✅ Conexión exitosa.")

try:
    # --- 1. Crear la estructura de las tablas ---
    print("📂 Leyendo el archivo schema.sql...")
    with open('schema.sql', 'r', encoding='utf-8') as f:
        cursor.executescript(f.read())
    print("🏗️ Tablas creadas exitosamente.")

    # --- 2. Poblar las tablas con datos iniciales ---
    print("📂 Leyendo el archivo poblar_datos.sql...")
    with open('poblar_datos.sql', 'r', encoding='utf-8') as f:
        cursor.executescript(f.read())
    print("🌱 Datos iniciales insertados exitosamente.")

except Exception as e:
    print(f"❌ Ocurrió un error: {e}")

finally:
    # --- Guardar cambios y cerrar conexión ---
    connection.commit()
    connection.close()
    print("🎉 ¡Base de datos creada y poblada correctamente!")