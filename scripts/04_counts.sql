-----------------------------------------------------------------
-- Cuentas varias
-----------------------------------------------------------------
--
-- Este archivo contiene algunas de las cuentas que iba desarrollando para
-- explorar diferentes aspectos de los datos, entre ellas:
--
-- 1. El número de entradas en la tabla del registro que tienen edades (indispensable
-- para hacer match con la matriz de conocimiento de clientes)
--
-- 2. El número de entradas en la tabla de registro que pertenecen a Exe
-- y en la tabla de clientes, y la comparación por la privada. El interés de este comparativo
-- es que hay aproximadamente números similares, sin embargo, utilizando la fecha de hito y la edad
-- tenemos muy pocos hits exactos al hacer la unión (en 03_joined.sql)
--
-- 3. Justo el punto anterior es el que se refleja en la última expresión, que cuenta los hits (matches)
-- al hacer join.
--
WITH total_counts AS (
    SELECT
        inmobiliaria,
        COUNT(*) AS num_records
    FROM registro
    GROUP BY 1
)
,counts_with_age AS (
    SELECT
        inmobiliaria,
        COUNT(*) AS num_w_age
    FROM registro
    WHERE edad IS NOT NULL
    GROUP BY 1
)
SELECT
    inmobiliaria,
    num_records,
    COALESCE(num_w_age, 0) AS num_w_age
FROM
    total_counts LEFT JOIN counts_with_age USING (inmobiliaria)
ORDER BY 2 DESC;
-- │      inmobiliaria      │ num_records │ num_w_age │
-- │        varchar         │    int64    │   int64   │
-- ├────────────────────────┼─────────────┼───────────┤
-- │ Exe Inmobiliaria       │        4447 │      3899 │
-- │ Ruba                   │        1814 │      1469 │
-- │ Casas Cadena           │         677 │       600 │
-- │ Institución financiera │         112 │        19 │
-- │ Provive                │          58 │        38 │
-- │ IDU                    │          20 │         2 │
-- │ Grupo Acxsa            │           4 │         4 │
-- │ Brasa                  │           2 │         1 │
-- │ Grupo VEQ              │           1 │         1 │
-- │ Novipolis              │           1 │         1 │
-- │ Grupo Vica             │           1 │         1 │
--
--
--
WITH counts_registro AS (
    SELECT
        privada,
        COUNT(*) AS num_registro
    FROM registro
    WHERE inmobiliaria = 'Exe Inmobiliaria'
    GROUP BY 1
)
,counts_clientes AS (
    SELECT
        privada,
        COUNT(*) AS num_clientes
    FROM clientes
    GROUP BY 1
)
SELECT
    COALESCE(privada, '---') AS privada,
    COALESCE(num_registro, 0) AS num_registro,
    COALESCE(num_clientes, 0) AS num_clientes
FROM
    counts_registro
    FULL JOIN counts_clientes USING (privada)
ORDER BY privada IS NULL, 2 DESC;
--
-- │  privada  │ num_registro │ num_clientes │
-- │  varchar  │    int64     │    int64     │
-- ├───────────┼──────────────┼──────────────┤
-- │ BENAVENTE │          603 │          610 │
-- │ MAYORGA   │          526 │          598 │
-- │ GANTE     │          505 │          510 │
-- │ OLIVETO   │          477 │          480 │
-- │ GERONA    │          442 │          446 │
-- │ GALVEZ    │          360 │          361 │
-- │ BAENA     │          348 │          355 │
-- │ LEGANÉS   │          296 │          298 │
-- │ OLEAGA    │          273 │          349 │
-- │ VELAYOS   │          219 │          222 │
-- │ FONTALBA  │           13 │            0 │
-- │ VICTORIA  │            7 │           25 │
-- │ VILLAR    │            0 │          385 │
-- │ ---       │          378 │            0 │

--
WITH joined AS (
    SELECT
        l.partida AS id_registro,
        r.producto AS id_cliente,
        l.fecha_operacion AS operacion,
        l.edad,
        l.privada
    FROM
        registro l
        INNER JOIN clientes r ON l.fecha_operacion = r.fecha_hito
        AND l.edad = r.edad_hito
        AND l.privada = r.privada
    WHERE l.inmobiliaria = 'Exe Inmobiliaria'
)
,count_all_hits AS (
    SELECT
        privada,
        id_registro,
        COUNT(*) AS num_matching
    FROM joined
    GROUP BY 1, 2
)
,counts_by_privada AS (
    SELECT
        privada,
        COUNT(DISTINCT id_registro) AS num_hits,
        SUM(CASE WHEN num_matching = 1 THEN 1 ELSE 0 END) AS uniq_hits
    FROM count_all_hits
    GROUP BY 1
)
,counts_joined_records AS (
    SELECT
        privada,
        COUNT(*) AS uniq_hits
    FROM joined_records
    GROUP BY 1
)
-- SELECT
--     SUM(num_hits), -- 1370
--     SUM(uniq_hits) -- 998
-- FROM counts_by_privada;
SELECT 
    l.*,
    COALESCE(r.num_hits, 0) AS num_hits,
    -- COALESCE(r.uniq_hits, 0) AS uniq_hits
    COALESCE(r2.uniq_hits, 0) AS uniq_hits
FROM
    counts_basic l
    LEFT JOIN counts_by_privada r USING (privada)
    LEFT JOIN counts_joined_records r2 USING (privada)
ORDER BY privada = '---', num_registro DESC;
-- │  privada  │ num_registro │ num_clientes │ num_hits │ uniq_hits │
-- │  varchar  │    int64     │    int64     │  int64   │  int128   │
-- ├───────────┼──────────────┼──────────────┼──────────┼───────────┤
-- │ BENAVENTE │          589 │          610 │      195 │       177 │
-- │ MAYORGA   │          515 │          598 │      214 │       193 │
-- │ GERONA    │          434 │          446 │      209 │       182 │
-- │ OLIVETO   │          410 │          480 │      157 │       138 │
-- │ GALVEZ    │          349 │          361 │      120 │       100 │
-- │ BAENA     │          343 │          355 │      161 │       143 │
-- │ LEGANÉS   │          291 │          298 │       89 │        80 │
-- │ OLEAGA    │          264 │          349 │      102 │        92 │
-- │ VELAYOS   │          217 │          222 │      107 │        87 │
-- │ GANTE     │           77 │          510 │       24 │        24 │
-- │ FONTALBA  │           12 │            0 │        0 │         0 │
-- │ VICTORIA  │            7 │           25 │        1 │         1 │
-- │ VILLAR    │            0 │          385 │        0 │         0 │
-- │ ---       │          431 │            0 │        0 │         0 │
-- ├───────────┼──────────────┼──────────────┼──────────┼───────────┤
-- │ total     │         3939 │         4639 │     1379 │      1217 │
