import os
import sys

import pendulum
from airflow.decorators import dag, task
from dags.dds.data_loader import StgDataLoader

sys.path.append(os.path.realpath("../"))
from dags.connector import ConnectionBuilder


@dag(
    'dds',
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2023, 8, 3, tz="UTC"),
    catchup=False,
    tags=["dds"],
    is_paused_upon_creation=True,
)
def dds_couriers_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dds_courier_load")
    def load():
        courier_loader = StgDataLoader(dwh_connect)
        courier_loader.load_couriers()

    @task(task_id="dds_deliveries_load")
    def load1():
        delivery_loader = StgDataLoader(dwh_connect)
        delivery_loader.load_deliveries()

    load_dict = load()
    load_dict  #
    load_dict1 = load1()
    load_dict1  #


dds_couriers_dag = dds_couriers_dag()
