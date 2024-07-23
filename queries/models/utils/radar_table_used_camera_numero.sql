WITH radares AS (
    SELECT
        t2.camera_numero,
        t1.*
    FROM `rj-cetrio.ocr_radar.equipamento` t1
    FULL OUTER JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
        ON t1.codcet = t2.codcet
),

used_cameras AS (
    SELECT
        DISTINCT
        camera_numero,
        camera_latitude AS latitude,
        camera_longitude AS longitude
    FROM `rj-cetrio.ocr_radar.readings_*`
),

merged_data AS (
    SELECT
        t2.camera_numero,
        t1.codcet,
        t1.locequip,
        t1.bairro,
        COALESCE(t1.latitude, t2.latitude) AS latitude,
        COALESCE(t1.longitude, t2.longitude) AS longitude
    FROM radares t1
    RIGHT JOIN used_cameras t2
        ON t1.camera_numero = t2.camera_numero
),

buffered_data AS (
    SELECT
        *,
        ST_BUFFER(ST_GEOGPOINT(longitude, latitude), 1000) AS buffer_1km,
        ST_BUFFER(ST_GEOGPOINT(longitude, latitude), 2000) AS buffer_2km,
        ST_BUFFER(ST_GEOGPOINT(longitude, latitude), 3000) AS buffer_3km,
        ST_BUFFER(ST_GEOGPOINT(longitude, latitude), 4000) AS buffer_4km,
        ST_BUFFER(ST_GEOGPOINT(longitude, latitude), 5000) AS buffer_5km,
    FROM merged_data
)

SELECT *
FROM buffered_data