WITH radars AS (
  SELECT
    COALESCE(t1.codcet, t2.codcet) AS codcet,
    t2.camera_numero,
    t1.latitude,
    t1.longitude,
    t1.locequip,
    t1.bairro,
    t1.logradouro,
    t1.sentido
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
),

used_radars AS (
  SELECT
    camera_numero,
    camera_latitude,
    camera_longitude,
    empresa,
    MAX(DATETIME(datahora, "America/Sao_Paulo")) AS last_detection_time,
    'active' AS status
  FROM `rj-cetrio.ocr_radar.readings_*`
  GROUP BY camera_numero, camera_latitude, camera_longitude, empresa
),

selected_radar AS (
  SELECT
    t1.codcet,
    COALESCE(t1.camera_numero, t2.camera_numero) AS camera_numero,
    COALESCE(t2.empresa, NULL) AS empresa,
    COALESCE(t1.latitude, t2.camera_latitude) AS latitude,
    COALESCE(t1.longitude, t2.camera_longitude) AS longitude,
    t1.locequip,
    t1.bairro,
    t1.logradouro,
    t1.sentido,
    COALESCE(t2.status, 'inactive') AS status,
    COALESCE(t2.last_detection_time, NULL) AS last_detection_time,
    CASE
      WHEN t2.last_detection_time IS NULL THEN NULL
      WHEN TIMESTAMP(t2.last_detection_time) >= TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATETIME("America/Sao_Paulo")), INTERVAL 24 HOUR) THEN 'yes'
      ELSE 'no'
    END AS active_in_last_24_hours
  FROM radars t1
  FULL OUTER JOIN used_radars t2
    ON t1.camera_numero = t2.camera_numero
)

SELECT * FROM selected_radar
ORDER BY last_detection_time DESC