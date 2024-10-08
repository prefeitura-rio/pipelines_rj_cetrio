WITH loc AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude,
    ST_GEOGPOINT(CAST(t1.longitude AS FLOAT64), CAST(t1.latitude AS FLOAT64)) AS location,
    ST_DISTANCE(
      ST_GEOGPOINT(longitude, latitude), -- target position
      ST_GEOGPOINT(CAST(t1.latitude AS FLOAT64), CAST(t1.longitude AS FLOAT64))
    ) AS distance,
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
  WHERE ST_DWithin(
    ST_GEOGPOINT(longitude, latitude), -- target position
    ST_GEOGPOINT(CAST(t1.longitude AS FLOAT64), CAST(t1.latitude AS FLOAT64)),
    300 -- target distance (radars less than X meters)
    )
    AND t1.locequip LIKE "AVENIDA BRASIL%"
)

SELECT
  t2.camera_numero,
  t2.locequip,
  t2.bairro,
  t2.distance as distancia,
  t1.placa,
  DATETIME(datahora, "America/Sao_Paulo") AS datahora,
  COALESCE(t2.latitude,t1.camera_latitude) AS latitude,
  COALESCE(t2.longitude,t1.camera_longitude) AS longitude,
FROM `rj-cetrio.ocr_radar.readings_*` t1
JOIN loc t2
  ON t1.camera_numero = t2.camera_numero
WHERE DATETIME(datahora, "America/Sao_Paulo") BETWEEN "2024-07-01T00:00:00" AND "2024-07-01T00:00:00"
ORDER BY distance, t2.camera_numero, datahora