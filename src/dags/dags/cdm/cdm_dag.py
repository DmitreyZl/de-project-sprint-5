import os
import sys

import pendulum

sys.path.append(os.path.realpath("../"))
from airflow.decorators import dag, task
from dags.cdm.cdm_loader import DataLoader
from dags.connector import ConnectionBuilder


@dag(
    'cdm',
    schedule_interval="10 * * * *",
    start_date=pendulum.datetime(2023, 8, 3, tz="UTC"),
    catchup=False,
    tags=["cdm"],
    is_paused_upon_creation=True,
)
def cdm_courier_ledger_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="cdm_courier_ledger")
    def load():
        report_loader = DataLoader(dwh_connect)
        report_loader.load_report()

    load_dict = load()
    load_dict  #


cdm_courier_ledger_dag = cdm_courier_ledger_dag()
