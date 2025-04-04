WITH main_query AS (
    SELECT
        placa,
        DATETIME(datahora, "America/Sao_Paulo") AS datahora,
        velocidade,
        TRUE AS found,
        'current' AS origin
    FROM `rj-cetrio.ocr_radar.readings_*`
    WHERE
        DATETIME(datahora, "America/Sao_Paulo") >= DATE_SUB(DATETIME('2024-08-01 03:49:34'), INTERVAL 3 HOUR)
        AND DATETIME(datahora, "America/Sao_Paulo") <= DATE_SUB(DATETIME('2024-08-01 03:49:34'), INTERVAL 3 HOUR)
        AND camera_numero = '637 - 1'
),
last_measurement_before AS (
    SELECT
        CAST(NULL AS STRING) AS placa,
        DATETIME(datahora, "America/Sao_Paulo") AS datahora,
        CAST(NULL AS INT64) AS velocidade,
        FALSE AS found,
        'before' AS origin
    FROM `rj-cetrio.ocr_radar.readings_*`
    WHERE
        DATETIME(datahora, "America/Sao_Paulo") < DATE_SUB(DATETIME('2024-08-01 03:49:34'), INTERVAL 3 HOUR)
        AND camera_numero = '637 - 1'
    ORDER BY datahora DESC
    LIMIT 1
),
last_measurement_after AS (
    SELECT
        CAST(NULL AS STRING) AS placa,
        DATETIME(datahora, "America/Sao_Paulo") AS datahora,
        CAST(NULL AS INT64) AS velocidade,
        FALSE AS found,
        'after' AS origin
    FROM `rj-cetrio.ocr_radar.readings_*`
    WHERE
        DATETIME(datahora, "America/Sao_Paulo") > DATE_SUB(DATETIME('2024-08-01 03:49:34'), INTERVAL 3 HOUR)
        AND camera_numero = '637 - 1'
    ORDER BY datahora ASC
    LIMIT 1
)

SELECT * FROM main_query
UNION ALL
SELECT * FROM last_measurement_before
WHERE NOT EXISTS (SELECT 1 FROM main_query)
UNION ALL
SELECT * FROM last_measurement_after
WHERE NOT EXISTS (SELECT 1 FROM main_query)