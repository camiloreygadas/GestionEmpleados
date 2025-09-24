# migracion_asistencia.py
import sqlite3

DB_FILE = 'asistencia.db'
print(f"🔌 Conectando a '{DB_FILE}' para añadir restricción de unicidad...")
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

try:
    print("✨ Recreando la tabla 'asistencia' con la nueva restricción UNIQUE...")
    # SQLite no permite añadir UNIQUE a una tabla existente, así que la recreamos
    cursor.executescript('''
        BEGIN TRANSACTION;
        CREATE TABLE IF NOT EXISTS asistencia_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            empleado_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            codigo_asistencia_id TEXT,
            FOREIGN KEY (empleado_id) REFERENCES empleados (id),
            FOREIGN KEY (codigo_asistencia_id) REFERENCES codigos_asistencia (codigo),
            UNIQUE(empleado_id, fecha) -- ¡LA NUEVA REGLA!
        );
        INSERT INTO asistencia_new (id, empleado_id, fecha, codigo_asistencia_id)
        SELECT id, empleado_id, fecha, codigo_asistencia_id FROM asistencia;
        DROP TABLE asistencia;
        ALTER TABLE asistencia_new RENAME TO asistencia;
        COMMIT;
    ''')
    print("✅ Tabla 'asistencia' actualizada exitosamente.")
except Exception as e:
    print(f"❌ Ocurrió un error: {e}")
    cursor.execute('ROLLBACK;')

connection.close()
print("🎉 Migración de asistencia completada.")