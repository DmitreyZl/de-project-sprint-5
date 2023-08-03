import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import pendulum
import psycopg
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from bson.objectid import ObjectId
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from requests import get

sys.path.append(os.path.realpath("../"))
from dags.connector import ConnectionBuilder, PgConnect
from dags.stg.api_data_loader import RawAPIDataLoader

@dag(
    'stg',
    schedule_interval="* * * * *",
    start_date=days_ago(7),
    catchup=False,
    tags=["stg"],
    is_paused_upon_creation=True,
)
def stg_deliveries_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="stg_deliveries_load")
    def load():
        delivery_loader = RawAPIDataLoader(dwh_connect)
        delivery_loader.load_deliveries()

    @task(task_id="stg_courier_load")
    def load1():
        courier_loader = RawAPIDataLoader(dwh_connect)
        courier_loader.load_couriers()

    load_dict1 = load1()
    load_dict = load()
    load_dict1  
    load_dict

stg_deliveries_dag = stg_deliveries_dag()
