-- schema.sql (Versión 5 - Profesional)

-- TABLAS DE CATÁLOGO
CREATE TABLE IF NOT EXISTS regiones (id INTEGER PRIMARY KEY, region TEXT NOT NULL, abreviatura TEXT NOT NULL, capital TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS provincias (id INTEGER PRIMARY KEY, provincia TEXT NOT NULL, region_id INTEGER NOT NULL, FOREIGN KEY (region_id) REFERENCES regiones (id));
CREATE TABLE IF NOT EXISTS comunas (id INTEGER PRIMARY KEY, comuna TEXT NOT NULL, provincia_id INTEGER NOT NULL, FOREIGN KEY (provincia_id) REFERENCES provincias (id));
CREATE TABLE IF NOT EXISTS cargos (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS turnos (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS codigos_asistencia (codigo TEXT PRIMARY KEY, descripcion TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS nacionalidades (id INTEGER PRIMARY KEY AUTOINCREMENT, pais TEXT NOT NULL, gentilicio TEXT NOT NULL, iso_code TEXT);
-- NUEVAS TABLAS DE CATÁLOGO
CREATE TABLE IF NOT EXISTS tipos_contrato (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS nominas (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS relaciones_laborales (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS acreditaciones (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS fases (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS distribucion_categorias (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS generos (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS tipos_pasaje (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS supervisiones (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS causales_despido (id INTEGER PRIMARY KEY, articulo_codigo TEXT, nombre_causal TEXT NOT NULL, descripcion TEXT, tipo_causal TEXT);
CREATE TABLE IF NOT EXISTS status_empleado (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);

-- TABLA PRINCIPAL DE EMPLEADOS (MUY MEJORADA)
CREATE TABLE IF NOT EXISTS empleados (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rut TEXT NOT NULL UNIQUE,
    nombre_completo TEXT NOT NULL,
    fecha_nacimiento TEXT,
    telefono TEXT,
    direccion TEXT,
    id_sap_global TEXT,
    id_sap_local TEXT,
    fecha_ingreso TEXT,
    fecha_egreso TEXT, -- Se llena manualmente cuando el empleado es finiquitado
    
    -- Conexiones (Llaves Foráneas) a las tablas de catálogo
    genero_id INTEGER,
    nacionalidad_id INTEGER,
    cargo_id INTEGER,
    turno_id INTEGER,
    comuna_id INTEGER,
    region_id INTEGER,
    tipo_contrato_id INTEGER,
    nomina_id INTEGER,
    relacion_laboral_id INTEGER,
    acreditacion_id INTEGER,
    area_id INTEGER,
    fase_id INTEGER,
    distribucion_categoria_id INTEGER,
    tipo_pasaje_id INTEGER,
    supervision_id INTEGER,
    causal_despido_id INTEGER,
    status_id INTEGER,
    
    FOREIGN KEY (genero_id) REFERENCES generos(id),
    FOREIGN KEY (nacionalidad_id) REFERENCES nacionalidades(id),
    FOREIGN KEY (cargo_id) REFERENCES cargos (id),
    FOREIGN KEY (turno_id) REFERENCES turnos (id),
    FOREIGN KEY (comuna_id) REFERENCES comunas (id),
    FOREIGN KEY (region_id) REFERENCES regiones (id),
    FOREIGN KEY (tipo_contrato_id) REFERENCES tipos_contrato(id),
    FOREIGN KEY (nomina_id) REFERENCES nominas(id),
    FOREIGN KEY (relacion_laboral_id) REFERENCES relaciones_laborales(id),
    FOREIGN KEY (acreditacion_id) REFERENCES acreditaciones(id),
    FOREIGN KEY (area_id) REFERENCES areas(id),
    FOREIGN KEY (fase_id) REFERENCES fases(id),
    FOREIGN KEY (distribucion_categoria_id) REFERENCES distribucion_categorias(id),
    FOREIGN KEY (tipo_pasaje_id) REFERENCES tipos_pasaje(id),
    FOREIGN KEY (supervision_id) REFERENCES supervisiones(id),
    FOREIGN KEY (causal_despido_id) REFERENCES causales_despido(id),
    FOREIGN KEY (status_id) REFERENCES status_empleado(id)
);

CREATE TABLE IF NOT EXISTS asistencia (id INTEGER PRIMARY KEY AUTOINCREMENT, empleado_id INTEGER NOT NULL, fecha TEXT NOT NULL, codigo_asistencia_id TEXT, FOREIGN KEY (empleado_id) REFERENCES empleados (id), FOREIGN KEY (codigo_asistencia_id) REFERENCES codigos_asistencia (codigo));

-- Ejecuta esto en tu base de datos SQLite
CREATE TABLE IF NOT EXISTS calendario_turnos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    turno_id INTEGER NOT NULL,
    fecha DATE NOT NULL,
    codigo VARCHAR(10) NOT NULL,
    FOREIGN KEY (turno_id) REFERENCES turnos (id),
    UNIQUE(turno_id, fecha)
);
