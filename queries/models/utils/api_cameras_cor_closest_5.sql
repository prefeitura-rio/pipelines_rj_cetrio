WITH selected_radar AS (
  SELECT
    camera_numero,
    latitude,
    longitude,
    ST_GEOGPOINT(CAST(t1.longitude AS FLOAT64), CAST(t1.latitude AS FLOAT64)) AS position
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  FULL OUTER JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
),

cameras AS (
  SELECT
    id_camera,
    nome,
    rtsp,
    latitude,
    longitude,
    ST_GEOGPOINT(longitude, latitude) AS position
  FROM `rj-cetrio.ocr_radar.cameras`
),

ranked_cameras AS (
  SELECT
    t2.camera_numero,
    t2.latitude AS latitude_radar,
    t2.longitude AS longitude_radar,
    t1.id_camera,
    t1.nome,
    t1.rtsp,
    t1.latitude,
    t1.longitude,
    ST_DISTANCE(t1.position, t2.position) AS distance,
    ROW_NUMBER() OVER (PARTITION BY t2.camera_numero ORDER BY ST_DISTANCE(t1.position, t2.position)) AS rank
  FROM cameras t1
  JOIN selected_radar t2
  ON ST_DWithin(t1.position, t2.position, 100000) -- Large enough distance to include all relevant cameras
)

SELECT
  camera_numero,
  latitude_radar,
  longitude_radar,
  id_camera,
  nome,
  rtsp,
  latitude,
  longitude,
  distance,
  rank
FROM ranked_cameras
WHERE rank <= 5
ORDER BY camera_numero, distance