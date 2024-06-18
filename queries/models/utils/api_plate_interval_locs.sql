        WITH ordered_positions AS (
            SELECT
                DATETIME(datahora, "America/Sao_Paulo") AS datahora,
                placa,
                camera_numero,
                camera_latitude,
                camera_longitude
            FROM `rj-cetrio.ocr_radar.readings_*`
            WHERE
                placa IN ("{{placa}}")
                AND (camera_latitude != 0 AND camera_longitude != 0)
                AND DATETIME_TRUNC(DATETIME(datahora, "America/Sao_Paulo"), HOUR) >= DATETIME_TRUNC(DATETIME("{{min_datetime}}"), HOUR)
                AND DATETIME_TRUNC(DATETIME(datahora, "America/Sao_Paulo"), HOUR) <= DATETIME_TRUNC(DATETIME("{{max_datetime}}"), HOUR)
            ORDER BY datahora ASC, placa ASC
        ),

        loc AS (
            SELECT
                t2.camera_numero,
                t1.bairro,
                t1.locequip AS localidade,
                CAST(t1.latitude AS FLOAT64) AS latitude,
                CAST(t1.longitude AS FLOAT64) AS longitude,
            FROM `rj-cetrio.ocr_radar_staging.equipamento` t1
            JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
                ON t1.codcet = t2.codcet
        )

        SELECT
            p.datahora,
            p.camera_numero,
            COALECE(l.latitude, p.camera_latitude) AS latitude,
            COALECE(l.longitude, p.camera_longitude) AS longitude,
            l.bairro,
            l.localidade
        FROM ordered_positions p
        JOIN loc l ON p.camera_numero = l.camera_numero
        ORDER BY p.datahora ASC