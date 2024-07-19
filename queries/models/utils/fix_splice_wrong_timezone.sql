-- Declare the year and month variable
DECLARE year_month STRING DEFAULT '2024_07';

-- Create the table with partitioning and clustering
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `rj-cetrio.ocr_radar.readings_%s`
PARTITION BY
  TIMESTAMP_TRUNC(
    datahora
    , HOUR)
CLUSTER BY placa
OPTIONS (
  require_partition_filter = FALSE
)
AS (
  SELECT
    datahora_captura,
    placa,
    tipoveiculo,
    velocidade,
    CASE
      WHEN datahora <= "2024-07-19 16:02:41 UTC" AND empresa = "SPLICE"
        THEN TIMESTAMP_ADD(datahora, INTERVAL 3 HOUR)
      ELSE datahora
    END AS datahora,
    camera_numero,
    camera_latitude,
    camera_longitude,
    empresa
  FROM `rj-cetrio.ocr_radar.readings_%s`
)
""", year_month, year_month);