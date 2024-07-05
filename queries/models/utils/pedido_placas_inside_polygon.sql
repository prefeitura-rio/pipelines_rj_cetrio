WITH loc AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude,
    ST_GEOGPOINT(CAST(t1.longitude AS FLOAT64), CAST(t1.latitude AS FLOAT64)) AS location,
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
  WHERE ST_WITHIN(
    ST_GEOGPOINT(CAST(t1.longitude AS FLOAT64), CAST(t1.latitude AS FLOAT64)),
    ST_GEOGFROMTEXT('POLYGON((-43.351199422135096 -22.826481599522367,-43.34755181206776 -22.83300036381351,-43.29269719282209 -22.81763133085856,-43.260182765280035 -22.838401207849614,-43.256147872926846 -22.8356131483648,-43.284885416297016 -22.8084810143404,-43.351199422135096 -22.826481599522367))')
    )
    AND t1.locequip LIKE "AVENIDA BRASIL%"
)

SELECT
  t2.camera_numero,
  t2.locequip,
  t2.bairro,
  t1.placa,
  DATETIME(datahora, "America/Sao_Paulo") AS datahora,
  COALESCE(t2.latitude,t1.camera_latitude) AS latitude,
  COALESCE(t2.longitude,t1.camera_longitude) AS longitude,
FROM `rj-cetrio.ocr_radar.readings_*` t1
JOIN loc t2
  ON t1.camera_numero = t2.camera_numero
WHERE DATETIME(datahora, "America/Sao_Paulo") BETWEEN "2024-01-01T00:00:00" AND "2024-01-01T00:00:00"
ORDER BY t2.camera_numero, datahora