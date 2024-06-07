# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple
from prefeitura_rio.pipelines_utils.prefect import generate_dump_db_schedules

from pipelines.constants import constants

#####################################
#
# ocr_radar Schedules
#
#####################################

ocr_radar_queries = {
    "equipamento": {
        "materialize_after_dump": True,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                *
            FROM [DBOCR_2024].[dbo].[Equipamento]
        """,
    },
}

ocr_radar_clocks = generate_dump_db_schedules(
    interval=timedelta(days=100),
    start_date=datetime(2022, 11, 9, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_CETRIO_AGENT_LABEL.value,
    ],
    db_database="DBOCR_2024",
    db_host="10.39.64.50",
    db_port="1433",
    db_type="sql_server",
    dataset_id="ocr_radar",
    infisical_secret_path="/db-ocr-radar",
    table_parameters=ocr_radar_queries,
)

ocr_radar_monthly_update_schedule = Schedule(clocks=untuple(ocr_radar_clocks))
