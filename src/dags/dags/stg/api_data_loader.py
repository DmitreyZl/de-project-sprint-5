import json
import os
import sys
from typing import Dict, List, Optional

from dags.connector import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from requests import get

sys.path.append(os.path.realpath("../"))
from dags.scripts import json2str


class RawCourier(BaseModel):
    courier_id: str
    object_value: str


class RawDelivery(BaseModel):
    delivery_id: str
    object_value: str


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class StgEtlSettingsRepository:
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(
        self, conn: Connection, workflow_key: str, workflow_settings: str
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {"etl_key": workflow_key, "etl_setting": workflow_settings},
            )


class RawAPIDataLoader:
    def __init__(self, stg: PgConnect) -> None:
        self.stg = stg
        self.settings_repository = StgEtlSettingsRepository()
        self.wf_key_couriers = "couriers_origin_to_stg_workflow"
        self.wf_key_deliveries = "deliveries_origin_to_stg_workflow"
        self.api_nickname = "akenona"
        self.api_cohort = "13"
        self.api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
        self.api_header = {
            "X-Nickname": self.api_nickname,
            "X-Cohort": self.api_cohort,
            "X-API-KEY": self.api_key,
        }

    def get_deliveries(self, offset: int) -> List[Dict]:
        print("Get delivery data by API")
        response = get(
            "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=id&sort_direction=asc&limit=50"
            "&offset={}".format(offset),
            headers=self.api_header,
        )
        return response.json()

    def get_couriers(self, offset: int) -> List[Dict]:
        print("Get couriers data by API")
        response = get(
            "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=id&sort_direction=asc&limit=50"
            "&offset={}".format(offset),
            headers=self.api_header,
        )
        return response.json()

    def insert_couriers(self, conn, raw_courier: RawCourier) -> None:
        with conn.cursor() as cur:
            print("Insert couriers data")
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(courier_id, object_value)
                    VALUES (%(courier_id)s, %(object_value)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value
                """,
                {
                    "courier_id": raw_courier.courier_id,
                    "object_value": raw_courier.object_value,
                },
            )

    def insert_deliveries(self, conn, raw_delivery: RawCourier) -> None:
        with conn.cursor() as cur:
            print("Insert deliveries data")
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(delivery_id, object_value)
                    VALUES (%(delivery_id)s, %(object_value)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value
                """,
                {
                    "delivery_id": raw_delivery.delivery_id,
                    "object_value": raw_delivery.object_value,
                },
            )

    def load_couriers(self):
        with self.stg.connection() as conn:
            print("Load connection")
            wf_setting = self.settings_repository.get_setting(
                conn, self.wf_key_couriers
            )
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key_couriers,
                    workflow_settings={"last_loaded_offset": 0},
                )
            last_loaded_offset = wf_setting.workflow_settings["last_loaded_offset"]
            print("Last loaded offset {}".format(last_loaded_offset))
            while True:
                load_queue = self.get_couriers(last_loaded_offset)
                if len(load_queue) == 0:
                    print("Have not data")
                    break
                else:
                    print("Iterate for courier")
                    # iterate for courier
                    for load_data in load_queue:
                        raw_courier = RawCourier(
                            courier_id=load_data["_id"],
                            object_value=json.dumps(load_data),
                        )
                        print(
                            "Raw data id {} obj value {}".format(
                                raw_courier.courier_id, raw_courier.object_value
                            )
                        )
                        # insert into database
                        self.insert_couriers(conn, raw_courier)
                    last_loaded_offset += 50
                    continue

            wf_setting.workflow_settings["last_loaded_offset"] = last_loaded_offset
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn, wf_setting.workflow_key, wf_setting_json
            )
            print(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)

    def load_deliveries(self):
        with self.stg.connection() as conn:
            print("Load connection")
            wf_setting = self.settings_repository.get_setting(
                conn, self.wf_key_deliveries
            )
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key_deliveries,
                    workflow_settings={"last_loaded_offset": 0},
                )
            last_loaded_offset = wf_setting.workflow_settings["last_loaded_offset"]
            print("Last loaded offset {}".format(last_loaded_offset))
            while True:
                load_queue = self.get_deliveries(last_loaded_offset)
                if len(load_queue) == 0:
                    print("Have not data")
                    break
                else:
                    print("Iterate for deliveries")
                    # iterate for delivery
                    for load_data in load_queue:
                        raw_delivery = RawDelivery(
                            delivery_id=load_data["delivery_id"],
                            object_value=json.dumps(load_data),
                        )
                        print(
                            "Raw data id {} obj value {}".format(
                                raw_delivery.delivery_id, raw_delivery.object_value
                            )
                        )
                        # insert into database
                        self.insert_deliveries(conn, raw_delivery)
                    last_loaded_offset += 50
                    continue

            wf_setting.workflow_settings["last_loaded_offset"] = last_loaded_offset
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn, wf_setting.workflow_key, wf_setting_json
            )
            print(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)
