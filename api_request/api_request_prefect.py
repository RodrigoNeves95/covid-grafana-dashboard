import requests
import pandas as pd
import time
import datetime

from loguru import logger
from influxdb import DataFrameClient

from api_request import check_db, check_and_update, get_last_update

import prefect
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from prefect import task, Flow, Parameter
import timedelta


@task(log_stdout=True)
def prefect_task_check_db(influx_client):
    check_db(influx_client)


@task(log_stdout=True)
def prefect_task_check_and_update(influx_client):
    check_and_update(influx_client)


@task(log_stdout=True)
def prefect_task_get_last_update(influx_client):
    get_last_update(influx_client)


@task(log_stdout=True)
def get_influx_client(host, db):
    return DataFrameClient(host=host, database=db)


if __name__ == "__main__":

    schedule = Schedule(clocks=[CronClock("0 13 * * *")])

    INFLUX_HOST = Parameter("host", default="localhost", required=True)
    INFLUX_DATABASE = Parameter("database", default="covid_api", required=True)

    with Flow("covid19_api_scrapper", schedule=schedule) as flow:
        influx_client = get_influx_client(INFLUX_HOST, INFLUX_DATABASE)
        prefect_task_check_db(influx_client)
        prefect_task_check_and_update(influx_client)
        prefect_task_get_last_update(influx_client)

    flow.register()
