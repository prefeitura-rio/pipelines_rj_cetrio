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

# automatic generated using https://jupyter.dados.rio/lab/tree/bases/rj-cetrio/ocr_radar/generate_queries.ipynb
ocr_radar_equipamento_queries = {
    "equipamento": {
        "materialize_after_dump": True,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[Equipamento]",
        "interval": timedelta(days=7),
    },
}

ocr_radar_equipamento_clocks = generate_dump_db_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2024, 7, 9, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_CETRIO_AGENT_LABEL.value,
    ],
    db_database="DBOCR_2024",
    db_host="10.39.64.50",
    db_port="1433",
    db_type="sql_server",
    dataset_id="ocr_radar",
    infisical_secret_path="/db-ocr-radar",
    table_parameters=ocr_radar_equipamento_queries,
)

ocr_radar_queries = {
    "readings_2024_01": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_012024]",
    },
    "readings_2024_02": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_022024]",
    },
    "readings_2024_03": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_032024]",
    },
    "readings_2024_04": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_042024]",
    },
    "readings_2024_05": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_052024]",
    },
    "readings_2024_06": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_062024]",
    },
    "readings_2024_07": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_072024]",
        "start_date": datetime(2024, 8, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    "readings_2024_08": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_082024]",
        "start_date": datetime(2024, 9, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    "readings_2024_09": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_092024]",
        "start_date": datetime(2024, 10, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    "readings_2024_10": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_102024]",
        "start_date": datetime(2024, 11, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    "readings_2024_11": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_112024]",
        "start_date": datetime(2024, 12, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    "readings_2024_12": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": "SELECT * FROM [DBOCR_2024].[dbo].[OCR_122024]",
        "start_date": datetime(2025, 1, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
}

ocr_radar_clocks = generate_dump_db_schedules(
    interval=timedelta(days=365 * 5),
    start_date=datetime(2024, 7, 9, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    runs_interval_minutes=7 * 60 * 24,
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


ocr_radar_schedule = Schedule(clocks=untuple(ocr_radar_clocks + ocr_radar_equipamento_clocks))
