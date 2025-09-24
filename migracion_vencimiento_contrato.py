# migracion_vencimiento_contrato.py
import sqlite3

DB_FILE = 'asistencia.db'
print(f"🔌 Conectando a '{DB_FILE}' para añadir la columna de vencimiento de contrato...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

try:
    print("✨ Añadiendo la columna 'fecha_vencimiento_contrato' a la tabla 'empleados'...")

    # Este comando añade la nueva columna.
    # Es seguro ejecutarlo varias veces, dará un error si la columna ya existe,
    # pero no dañará tus datos.
    cursor.execute('ALTER TABLE empleados ADD COLUMN fecha_vencimiento_contrato DATE;')

    print("✅ Columna 'fecha_vencimiento_contrato' añadida exitosamente.")

except sqlite3.OperationalError as e:
    # Este error es normal si la columna ya existe.
    if "duplicate column name" in str(e):
        print("⚠️ La columna 'fecha_vencimiento_contrato' ya existe en la tabla.")
    else:
        print(f"❌ Ocurrió un error de SQL: {e}")
except Exception as e:
    print(f"❌ Ocurrió un error inesperado: {e}")

connection.commit()
connection.close()
print("🎉 Migración de vencimiento de contrato completada.")