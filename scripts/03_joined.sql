----------------------------------------------------------------------------------
-- Unión de las tablas de registro público de propiedad y conocimiento de clientes
----------------------------------------------------------------------------------
--
-- La unión toma los campos de fecha de operaracion, edad y privada, y además se utiliza
-- como heurística que se busca minimizar la diferencia en valor absoluto entre los metros
-- y el valor de la operación.
--
-- Como ID del registro se utiliza la partida y del ID de conocimiento de clientes se utiliza
-- el producto.
--
CREATE OR REPLACE TABLE joined_records AS WITH joined AS (
SELECT l.partida AS id_registro,
    r.producto AS id_cliente,
    l.fecha_operacion AS fecha,
    l.edad,
    l.privada,
    l.valor_operacion AS valor_registro,
    r.total_general AS valor_cliente,
    l.mts_superficie AS mts_sup_registro,
    r.mts_superficie AS mts_sup_cliente,
    r.mts_contruccion,
    r.modelo,
    l.direccion,
    l.lote,
    l.manzana,
    l.fraccionamiento,
    l.latitud,
    l.longitud,
    r.residia_mexicali,
    r.cp_origen,
    r.colonia_origen,
    r.municipio_origen,
    r.ciudad_origen,
    r.estado_origen,
    ROW_NUMBER() OVER (
        PARTITION BY l.partida
        ORDER BY ABS(l.mts_superficie - r.mts_superficie),
            ABS(l.valor_operacion - r.total_general),
            r.producto -- to break ties
    ) AS rank_r,
    ROW_NUMBER() OVER (
        PARTITION BY r.producto
        ORDER BY ABS(l.mts_superficie - r.mts_superficie),
            ABS(l.valor_operacion - r.total_general),
            r.producto -- to break ties
    ) AS rank_l
FROM registro l
    INNER JOIN clientes r ON l.fecha_operacion = r.fecha_hito
    AND l.edad = r.edad_hito
    AND l.privada = r.privada
WHERE l.inmobiliaria = 'Exe Inmobiliaria'
)
SELECT * EXCLUDE(rank_r, rank_l)
FROM joined
WHERE rank_r = 1
    AND rank_l = 1
ORDER BY id_registro;
--

--
-- Algunas cuentas que verifican que tenemos IDs únicas
--
-- SELECT
--     (SELECT COUNT(*) FROM joined_records),                      -- 1217
--     (SELECT COUNT(DISTINCT id_registro) FROM joined_records),   -- 1217
--     (SELECT COUNT(DISTINCT id_cliente) FROM joined_records);    -- 1217
--

-- Exporta exe_records.parquet
--
COPY joined_records TO 'data/processed/exe_records.parquet' (FORMAT PARQUET);
--
--
-- Para exportar a CSV se utiliza
-- TO 'data/processed/exe_records.csv' (FORMAT CSV, HEADER);
--
--
--
-- Cuentas que nos muestran que en general el valor de operación casi no coincide, y
-- la superficie del terreno no lo hace en 106 casos.
--
-- SELECT
--     COUNT(*),                                                               -- 1217
--     SUM(CASE WHEN valor_registro = valor_cliente THEN 1 ELSE 0 END),        --  104
--     SUM(CASE WHEN mts_sup_registro = mts_sup_cliente THEN 1 ELSE 0 END)     -- 1111
-- FROM joined_records;