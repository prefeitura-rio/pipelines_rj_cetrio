
SELECT
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP(), "America/Sao_Paulo") as updated_at,
    CAST(codcet AS STRING) as codcet,
    CAST(locequip AS STRING) as locequip,
    CAST(bairro AS STRING) as bairro,
    CAST(latitude AS FLOAT64) as latitude,
    CAST(longitude AS FLOAT64) as longitude,
    CAST(logradouro AS STRING) as logradouro,
    CAST(sentido AS STRING) as sentido,
    CAST(REGEXP_REPLACE(velofisc, r'\.0$', '') AS INT64) as velofisc
FROM `rj-cetrio.ocr_radar_staging.equipamento`