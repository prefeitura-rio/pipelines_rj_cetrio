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

year_2024 = 2024
statr_month = 7
start_day = 16
# automatic generated using https://jupyter.dados.rio/lab/tree/bases/rj-cetrio/ocr_radar/generate_queries.ipynb
ocr_radar_equipamento_queries = {
    "equipamento": {
        "dataset_id": "ocr_radar",
        "materialize_after_dump": True,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "overwrite",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[Equipamento]",
        "interval": timedelta(days=1),
    },
}

ocr_radar_equipamento_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 7, 7, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
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


ocr_radar_2024_queries = {
    f"readings_{year_2024}_01": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_01{year_2024}]",
        "start_date": datetime(
            year_2024, statr_month, start_day, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        ),
    },
    f"readings_{year_2024}_02": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_02{year_2024}]",
        "start_date": datetime(
            year_2024, statr_month, start_day, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        ),
    },
    f"readings_{year_2024}_03": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_03{year_2024}]",
        "start_date": datetime(
            year_2024, statr_month, start_day, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        ),
    },
    f"readings_{year_2024}_04": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_04{year_2024}]",
        "start_date": datetime(
            year_2024, statr_month, start_day, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        ),
    },
    f"readings_{year_2024}_05": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_05{year_2024}]",
        "start_date": datetime(
            year_2024, statr_month, start_day, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        ),
    },
    f"readings_{year_2024}_06": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_06{year_2024}]",
        "start_date": datetime(
            year_2024, statr_month, start_day, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        ),
    },
    f"readings_{year_2024}_07": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_07{year_2024}]",
        "start_date": datetime(year_2024, 8, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    f"readings_{year_2024}_08": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_08{year_2024}]",
        "start_date": datetime(year_2024, 9, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    f"readings_{year_2024}_09": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_09{year_2024}]",
        "start_date": datetime(year_2024, 10, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    f"readings_{year_2024}_10": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_10{year_2024}]",
        "start_date": datetime(year_2024, 11, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    f"readings_{year_2024}_11": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_11{year_2024}]",
        "start_date": datetime(year_2024, 12, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    },
    f"readings_{year_2024}_12": {
        "partition_columns": "Data",
        "partition_date_format": "%Y-%m-%d",
        "materialize_after_dump": False,
        "biglake_table": True,
        "materialization_mode": "prod",
        "dump_mode": "append",
        "execute_query": f"SELECT * FROM [DBOCR_{year_2024}].[dbo].[OCR_12{year_2024}]",
        "start_date": datetime(
            year_2024 + 1, 1, 2, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        ),
    },
}


ocr_radar_2024_clocks = generate_dump_db_schedules(
    interval=timedelta(days=365 * 5),
    start_date=datetime(2024, 7, 9, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    runs_interval_minutes=2 * 24 * 60,
    labels=[
        constants.RJ_CETRIO_AGENT_LABEL.value,
    ],
    db_database="DBOCR_2024",
    db_host="10.39.64.50",
    db_port="1433",
    db_type="sql_server",
    dataset_id="ocr_radar_historico",
    infisical_secret_path="/db-ocr-radar",
    table_parameters=ocr_radar_2024_queries,
)


ocr_radar_schedule = Schedule(
    clocks=untuple(ocr_radar_equipamento_clocks)
    # + untuple(ocr_radar_2024_clocks)
)
