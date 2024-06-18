SELECT
  empresa,
  SUBSTR(SHA256(
        CONCAT(
            '',
            SAFE_CAST(REGEXP_REPLACE(placa, r'\.0$', '')  AS STRING)
        )
    ), 2,17) as  id,
  tipoveiculo,
  velocidade,
  camera_numero,
  CONCAT(camera_latitude, camera_longitude) AS position,
  DATETIME(datahora, "America/Sao_Paulo") AS datahora,
  COUNT(*) AS duplicates,
FROM `rj-cetrio.ocr_radar.readings_*`
GROUP BY 1,2,3,4,5,6,7