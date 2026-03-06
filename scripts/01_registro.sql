-----------------------------------------------------------------
-- Registro público de la propiedad y construcción del choice set
-----------------------------------------------------------------
--
-- Este script carga los datos del registro público de la propiedad, limpia el campo de fraccionamiento
-- y llena las coordenadas faltantes (estas provienend de la hoja de calculo matriz_estudio_de_vivienda
-- y en caso de no econtrarse ahí se buscaron manualmente en google maps).
--
-- Al correr el script completo, se crean las tablas 'registro' y 'choice_set', y posteriormente
-- se exporta la segunda tabla como 'choiceset.parquet'.
--
-- La hoja de cálculo original tiene dos posibles columnas de ID --- 'folio' y 'partida' --- pero 'folio'
-- contiene duplicados y por lo tanto las filas se indentifican con 'partida'.
--
-- El choice set cruza la base de datos del registro consigo misma, y cada operación se agrupa con
-- el resto de las operaciones que tuvieron lugar en una ventana de tiempo cercana, y que no difieren
-- de manera significativa con los metros de superficie de la propiedad original.
--
-- Esto resulta en dos parámetros con los que se puede experimentar:
-- 1. el tamaño de la ventana de tiempo: en la línea 147-148 esta se define como 15 días de cada lado de la fecha de operación
-- 2. la diferencia máxima en metros de superficie que se tolera, se define como 50 m en la línea 149.
--
-- La nueva tabla 'choice_set' contiene las columnas de la tabla original así como dos columnas nuevas 
-- 'partida_main' que indica la ID de la observación original, y 'is_main' que identifica con el valor 1 la
-- observación original (dicho de otro modo, esta es la variable indicadora).
--

--
-- Carga de Excel y limpia los datos para crear la tabla 'registro'
CREATE OR REPLACE TABLE registro AS WITH loaded_excel AS (
        SELECT "Inmobiliaria" AS inmobiliaria,
            CAST("Folio real" AS INT) AS folio,
            '1899-12-30'::DATE + CAST("Fecha de operacion" AS INT) AS fecha_operacion,
            -- "Municipio" AS municipio,  -- MEXICALI en todos los casos
            "Lote" AS lote,
            "Manzana" AS manzana,
            "Fraccionamiento" AS fraccionamiento,
            "Direccion" AS direccion,
            "Razón social" AS razon_social,
            "Comprador" AS comprador,
            IF(lower("Competencia actual") == 'sí', 1, 0) AS competencia_actual,
            CAST("Valor de operacion" AS FLOAT) AS valor_operacion,
            CAST("Monto de crédito" AS FLOAT) AS monto_credito,
            "Acreedor" AS acreedor,
            '1899-12-30'::DATE + CAST("Fecha de partida" AS INT) AS fecha_partida,
            CAST("Partida" AS INT) AS partida,
            CAST("Superficie" AS FLOAT) AS mts_superficie,
            "Acreedores" AS acreedores,
            "Tipo de vivienda" AS tipo_vivienda,
            CAST("Edad" AS INT) AS edad,
            CAST("Latitud" AS FLOAT) AS latitud,
            CAST("Longitud" AS FLOAT) AS longitud,
            (lower("Mercado Exe") == 'sí') AS mercado_exe,
            "Categoría" AS categoria
        FROM read_xlsx(
                '/Users/rodolfofigueroa/Library/CloudStorage/OneDrive-InstitutoTecnologicoydeEstudiosSuperioresdeMonterrey/mexicali_data/processing/RPPC_vivienda-interes-social-nueva_2020-2024.xlsx',
                all_varchar = true,
                header = true
            )
    ),
    clean_frac AS (
        SELECT partida,
            folio,
            inmobiliaria,
            fecha_operacion,
            fecha_partida,
            valor_operacion,
            edad,
            lote,
            manzana,
            CASE
                WHEN fraccionamiento LIKE 'FRACCIONAMIENTO VILLANOVA%' THEN 'VILLANOVA'
                WHEN fraccionamiento LIKE '%PORTICOS DEL VALLE%' THEN 'PORTICOS DEL VALLE' -- FRACCIONAMIENTO/FRACTO
                WHEN starts_with(fraccionamiento, 'FRAC') THEN fraccionamiento.REPLACE('FRACC ', '').REPLACE('FRACCIONAMIENTO ', '').REPLACE('FRACCTO ', '').REPLACE('FRACTO ', '')
                WHEN fraccionamiento LIKE 'COLONIA BALBUENA%' THEN fraccionamiento.REPLACE('DE MEXICALI BC DENTRO DE LA', '')
                WHEN fraccionamiento LIKE 'COLONIA GRANJAS AGRICOLAS%' THEN fraccionamiento.REPLACE('FOLIO REAL 1598810 DE MEXICALI BC', '')
                ELSE fraccionamiento
            END AS fraccionamiento,
            CASE
                WHEN starts_with(direccion, 'DESARROLLO URBANO LA CONDESA') THEN direccion.REPLACE('DESARROLLO URBANO LA CONDESA SECCION ', '').REPLACE('DESARROLLO URBANO LA CONDESA SECCIÓN ', '')
                WHEN starts_with(
                    direccion,
                    'DESARROLLO URBANO VICTORIA RESIDENCIAL SEGUNDA SECCION'
                ) THEN direccion.REPLACE(
                    'DESARROLLO URBANO VICTORIA RESIDENCIAL SEGUNDA SECCION',
                    'VICTORIA'
                )
                ELSE NULL
            END AS privada,
            REPLACE(direccion, 'LOCALIZACION: ', '') AS direccion,
            acreedor,
            comprador,
            mts_superficie,
            latitud,
            longitud
        FROM loaded_excel
        WHERE fraccionamiento NOT LIKE '%PUERTO DE SAN FELIPE%'
    )
SELECT * EXCLUDE (fraccionamiento),
    rtrim(fraccionamiento) AS fraccionamiento
FROM clean_frac;
--

-- Llena la coordenadas de los fracionamientos faltantes
UPDATE registro r
SET latitud = v.lat,
    longitud = v.lon
FROM (
        VALUES ('Angeles De Puebla', 32.564770, -115.339935),
            ('COLONIA BALBUENA', 32.630970, -115.472679),
            ('Corceles Residencial', 32.567668, -115.469794),
            ('Gran Foresta', 32.563446, -115.434414),
            ('Huertas del Colorado', 32.563662, -115.411109),
            (
                'LA RIOJA SECCION CASTILLAUNA',
                32.656198,
                -115.364974
            ),
            ('PORTICOS DEL VALLE', 32.595718, -115.438633),
            ('Parajes De Puebla', 32.557625, -115.346180),
            ('Quinta Granada', 32.572234, -115.469416),
            ('Quinta Granada 3', 32.567764, -115.473000),
            ('San Andres', 32.577850, -115.424811),
            ('Valle Oriente', 32.571884, -115.361330),
            ('Privadas Condesa', 32.595546, -115.344834)
    ) AS v(frac, lat, lon)
WHERE r.fraccionamiento = v.frac
    AND r.latitud IS NULL;
--

-- Abajo un par de cuentas para verificar
--
-- SELECT COUNT(*) FROM registro;                               -- 7137
-- SELECT COUNT(DISTINCT partida) FROM registro;                -- 7137
-- SELECT COUNT(*) FROM registro WHERE longitud IS NOT NULL;    -- 7137
--

--
-- Crea la tabla choice set
CREATE OR REPLACE TABLE choice_set AS WITH muestra AS (
        SELECT partida,
            fecha_operacion,
            mts_superficie
        FROM registro
        ORDER BY partida -- LIMIT 1  -- < -------- para hacer pruebas / debugging se puede descomentar
    )
SELECT IF(l.partida = r.partida, 1, 0) AS is_main,
    l.partida AS partida_main,
    r.*
FROM muestra l
    LEFT JOIN registro r ON r.fecha_operacion BETWEEN l.fecha_operacion - INTERVAL 15 DAYS
    AND l.fecha_operacion + INTERVAL 15 DAYS
    AND ABS(l.mts_superficie - r.mts_superficie) < 50
ORDER BY r.fecha_operacion,
    is_main DESC;
--

--
-- Cuentas para verificar.
-- Nótese que se pierden 119 partidas al no tener observaciones que satisfagan
-- las condiciones de la fecha y los metros de superficie.
--
-- SELECT COUNT(*) FROM choice_set; -- 834,197
-- SELECT SUM(is_main) FROM choice_set;                -- 7018
-- SELECT COUNT(DISTINCT partida) FROM choice_set;     -- 7018
--

-- Exporta choiceset.parquet
--
COPY choice_set TO 'data/processed/choiceset.parquet' (FORMAT PARQUET);
--
--
-- Para exportar a CSV se utiliza
-- TO 'data/processed/choiceset.csv' (FORMAT CSV, HEADER);