------------------------------------
-- Tabla de conocimiento de clientes
------------------------------------
--
-- Este script carga los datos de conocimeniento de clientes (de Exe, obviamente).
-- De esta tabla se obtiene el CP de origen del cliente y el modelo de la propiedad
--
-- Se excluyen algunas de las columnas que no son de interés al contener un único valor o datos}
-- muy sucios.
--
-- Originalmente, la columna producto es un buen canditado como ID de las filas aunque tiene un duplicado (que se filtra.)
--
-- La edad que se toma es la columna 'edad_hito' y al ser la columna que se utilza en el cruce con la
-- tabla de registro se filtran las columnas para los cuales falta el valor (WHERE edad_hito IS NOT NULL).
--
CREATE OR REPLACE TABLE clientes AS WITH loaded_excel AS (
SELECT "Producto" AS producto,
    -- "Fuente de posible cliente" AS fuente_cliente,  -- no es de interés
    '1899-12-30'::DATE + CAST("Fecha hito" AS INT) AS fecha_hito,
    CAST("Edad en el hito" AS INT) AS edad_hito,
    '1899-12-30'::DATE + TRY_CAST("Fecha de nacimiento" AS INT) AS fecha_nacimiento,
    CAST("Edad" AS INT) AS edad,
    "Ocupación" AS ocupacion,
    -- "Nombre de empresa" AS nombre_empresa,  -- mala calidad
    "Privada" AS privada,
    "Modelo" AS modelo,
    CAST("Mts construcción" AS FLOAT) AS mts_contruccion,
    CAST("Mts lote" AS FLOAT) AS mts_superficie,
    CAST("Total general" AS FLOAT) AS total_general,
    upper("Tipo de crédito") AS tipo_credito,
    -- "Hito" AS hito, -- 'Firmado' para todas las columnas
    "Estado civil" AS edo_civil,
    "Nombre de colonia" AS colonia_origen,
    "Ciudad" AS ciudad_origen,
    "Código postal" AS cp_origen,
    "Estado" AS estado_origen,
    "Municipio" AS municipio_origen -- "País" AS pais_origen -- MX para todas las columnas
FROM read_xlsx(
        'data/raw/conocimiento-cliente_vivienda-interes-social_2020-2024.xlsx',
        all_varchar = true,
        header = true
    )
)
SELECT DISTINCT ON (producto)
    /* DISTINCT ON (): DuckDB's way to get rid of duplicated row */
    producto,
    modelo,
    UPPER(privada) AS privada,
    tipo_credito,
    mts_superficie,
    mts_contruccion,
    total_general,
    fecha_hito,
    -- edad,  -- la edad en la fecha de hito es la que se utiliza para el cruce de datos
    edad_hito,
    IF(UPPER(municipio_origen) = 'MEXICALI', 1, 0) AS residia_mexicali,
    colonia_origen,
    municipio_origen,
    ciudad_origen,
    estado_origen,
    cp_origen
FROM loaded_excel
WHERE edad_hito IS NOT NULL -- drops less than 20 rows
;
--

-- Algunas cuentas para verificar
--
-- SELECT
--     (SELECT COUNT(*) FROM clientes_min) AS total,                                       -- 4639 (was 4655 without edad not null condition)
--     (SELECT COUNT(*) FROM clientes_min WHERE modelo IS NOT NULL) AS has_modelo,         -- 4639
--     (SELECT COUNT(*) FROM clientes_min WHERE privada IS NOT NULL) AS has_pvda,          -- 4639
--     (SELECT COUNT(*) FROM clientes_min WHERE total_general IS NOT NULL) AS has_total,   -- 4639
--     (SELECT COUNT(*) FROM clientes_min WHERE fecha_hito IS NOT NULL) AS has_fecha,      -- 4639
--     (SELECT COUNT(*) FROM clientes_min WHERE edad_hito IS NOT NULL) AS has_edad,        -- 4639
--     (SELECT COUNT(*) FROM clientes_min WHERE colonia_origen IS NOT NULL) AS has_col,    -- 4639
--     (SELECT COUNT(*) FROM clientes_min WHERE ciudad_origen IS NOT NULL) AS has_ciudad,  -- 4637 !
--     (SELECT COUNT(*) FROM clientes_min WHERE estado_origen IS NOT NULL) AS has_edo,     -- 4639
--     (SELECT COUNT(*) FROM clientes_min WHERE cp_origen IS NOT NULL) AS has_cp;          -- 4639
--

--
-- Cuentas de los modelos
-- En el Excel matriz estudios de vivienda se tienen los modelos Sofia, Eugenia, Carlota A y Carlota E
-- pero no tenemos el model correspondiente a Carlota. 
--
-- SELECT
--     modelo,
--     COUNT(*) AS num_records
-- FROM clientes
-- GROUP BY 1
-- ORDER BY 2 DESC;
-- │       modelo        │ num_records │
-- │       varchar       │    int64    │
-- ├─────────────────────┼─────────────┤
-- │ 01 Sofia            │        1917 │
-- │ 02 Eugenia          │        1414 │
-- │ 03 Carlota Austera  │         533 │
-- │ 03 Carlota Equipada │         474 │
-- │ 03 Carlota          │         301 │
--

--
-- Precio promedio de los diferentes modelos
-- Parece curioso que Carlota Equipada sea en promedio más económica que Carlota Austera,
-- y que Carlota sea el modelo más caro.
--
-- SELECT 
--     modelo,
--     ROUND(AVG(total_general)) AS precio_promedio
-- FROM clientes
-- GROUP BY 1
-- ORDER BY 1;
-- 
-- │       modelo        │ precio_promedio │
-- │       varchar       │     double      │
-- ├─────────────────────┼─────────────────┤
-- │ 01 Sofia            │        579026.0 │
-- │ 02 Eugenia          │        729022.0 │
-- │ 03 Carlota          │        883566.0 │
-- │ 03 Carlota Austera  │        852015.0 │
-- │ 03 Carlota Equipada │        797977.0 │