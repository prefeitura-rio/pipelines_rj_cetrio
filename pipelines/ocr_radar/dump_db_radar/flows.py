# -*- coding: utf-8 -*-
"""
Database dumping flows for cetrio project..
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_templates.dump_db.flows import flow as dump_sql_flow
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.ocr_radar.dump_db_radar.schedules import ocr_radar_schedule

dump_sql_ocr_radar_flow = deepcopy(dump_sql_flow)
dump_sql_ocr_radar_flow.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
dump_sql_ocr_radar_flow.name = "CETRIO: ocr radar - Ingerir tabelas de banco SQL"
dump_sql_ocr_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_ocr_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_CETRIO_AGENT_LABEL.value,
    ],
)


ocr_radar_default_parameters = {
    "db_database": "DWOCR_Staging",
    "db_host": "10.39.64.50",
    "db_port": "1433",
    "db_type": "sql_server",
    "infisical_secret_path": "/db-ocr-radar",
    "dataset_id": "ocr_radar",
}
dump_sql_ocr_radar_flow = set_default_parameters(
    dump_sql_ocr_radar_flow, default_parameters=ocr_radar_default_parameters
)

dump_sql_ocr_radar_flow.schedule = ocr_radar_schedule
