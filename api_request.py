import requests
import pandas as pd
import time
import datetime

from loguru import logger
from influxdb import DataFrameClient


def process_data(data):

    data = data.set_index("data")
    data.index = pd.to_datetime(data.index, dayfirst=True)
    data = data.sort_index()

    confirmados = data[[col for col in data.columns if "confirmados" in col]].astype(
        float
    )
    obitos = data[[col for col in data.columns if "obitos" in col]].astype(float)
    recuperados = data[[col for col in data.columns if "recuperados" in col]].astype(
        float
    )
    sintomas = data[[col for col in data.columns if "sintomas" in col]].astype(float)

    geral = data[
        [
            "internados",
            "internados_uci",
            "lab",
            "suspeitos",
            "vigilancia",
            "cadeias_transmissao",
        ]
    ].astype(float)

    confirmados = confirmados.rename(columns={"confirmados": "confirmados_portugal"})
    recuperados = recuperados.rename(columns={"recuperados": "recuperados_portugal"})
    obitos = obitos.rename(columns={"obitos": "obitos_portugal"})
    regioes = [
        "portugal",
        "arsnorte",
        "arscentro",
        "arslvt",
        "arsalentejo",
        "arsalgarve",
        "acores",
        "madeira",
    ]

    for regiao in regioes:
        confirmados[f"confirmados_activos_{regiao}"] = (
            confirmados[f"confirmados_{regiao}"]
            - recuperados[f"recuperados_{regiao}"]
            - obitos[f"obitos_{regiao}"]
        )

    return confirmados, recuperados, obitos, sintomas, geral


def ingest_data(confirmados, recuperados, obitos, sintomas, geral, influx_client):

    logger.info("A ingerir dados para a tabela confirmados...")
    response = influx_client.write_points(confirmados, "confirmados")
    logger.info(f"Resposta {response}")

    logger.info("A ingerir dados para a tabela obitos...")
    response = influx_client.write_points(obitos, "obitos")
    logger.info(f"Resposta {response}")

    logger.info("A ingerir dados para a tabela recuperados...")
    response = influx_client.write_points(recuperados, "recuperados")
    logger.info(f"Resposta {response}")

    logger.info("A ingerir dados para a tabela sintomas...")
    response = influx_client.write_points(sintomas, "sintomas")
    logger.info(f"Resposta {response}")

    logger.info("A ingerir dados para a tabela geral...")


def request_per_date(date, influx_client):
    request_url = f"https://covid19-api.vost.pt/Requests/get_entry/{date}"

    logger.info(f"Getting data for {date}")
    logger.info(f"Request URL - {request_url}")

    request = requests.get(request_url)

    request_status_code = request.status_code

    logger.info(f"Request status code {request_status_code}")

    if request_status_code == 200:
        data = pd.DataFrame.from_dict(request.json())
        confirmados, recuperados, obitos, sintomas, geral = process_data(data)
        ingest_data(confirmados, recuperados, obitos, sintomas, geral, influx_client)

    if request_status_code == 500:
        logger.info(
            "Current day does not exist yet. Data is already in its latest state."
        )


def request_full_dataset(influx_client):
    request_url = "https://covid19-api.vost.pt/Requests/get_full_dataset"

    logger.info("Getting full dataset")
    logger.info(f"Request URL - {request_url}")

    request = requests.get(request_url)

    request_status_code = request.status_code

    logger.info(f"Request status code {request_status_code}")

    if request_status_code == 200:
        data = pd.DataFrame.from_dict(request.json())
        confirmados, recuperados, obitos, sintomas, geral = process_data(data)
        ingest_data(confirmados, recuperados, obitos, sintomas, geral, influx_client)


def request_last_entry(influx_client):
    request_url = "https://covid19-api.vost.pt/Requests/get_last_update"

    logger.info(f"Getting data for {date}")
    logger.info(f"Request URL - {request_url}")

    response = requests.get(request_url)

    request_status_code = request.status_code

    logger.info(f"Request status code {request_status_code}")

    if request_status_code == 200:
        data = pd.DataFrame.from_dict(request.json(), orient="index").T
        confirmados, recuperados, obitos, sintomas, geral = process_data(data)
        ingest_data(confirmados, recuperados, obitos, sintomas, geral, influx_client)

    if request_status_code == 500:
        logger.info(
            "Current day does not exist yet. Data is already in its latest state."
        )


def check_measurements(influx_client):
    measurements = influx_client.get_list_measurements()

    # ToDo: change this to check all measurements name
    if len(measurements) > 1:
        return True
    else:
        return False


def get_last_entry_db(influx_client):
    last_entry_db = influx_client.query(
        "SELECT * FROM confirmados ORDER BY time DESC LIMIT 1"
    )["confirmados"].index

    return pd.Timestamp(last_entry_db.values[0])


def check_db(influx_client):
    dbs = influx_client.get_list_database()
    if influx_client._database not in dbs:
        logger.info("Creating database...")
        influx_client.create_database(influx_client._database)
    else:
        logger.info("Database already created. Skiping...")
        pass


def check_and_update(influx_client):

    if check_measurements(influx_client):
        logger.info("Data already present. Checking where to resume!")
        last_entry_db = get_last_entry_db(influx_client)
        logger.info(f"Last date present in database - {last_entry_db}")

        current_day = datetime.datetime.today()

        if last_entry_db < current_day:
            for date in pd.date_range(last_entry_db, current_day):
                date = date.strftime("%d-%m-%Y")
                request_per_date(date, influx_client)
    else:
        logger.info("Database is empty. Going to populate it")
        request_full_dataset(influx_client)


def get_last_update(influx_client):

    logger.info("Data already present. Checking where to resume!")
    last_entry_db = get_last_entry_db(influx_client).strftime("%d-%m-%Y")
    current_day = datetime.datetime.today().strftime("%d-%m-%Y")
    logger.info(f"Last date present in database - {last_entry_db}")

    if last_entry_db == current_day:
        logger.info("Data is updated!")
        return True

    while True:
        logger.info(current_day)
        logger.info(last_entry_db)

        request_per_date(current_day, influx_client)
        # todo: only read this when gets updated
        last_entry_db = get_last_entry_db(influx_client)

        if last_entry_db == current_day:
            return True


if __name__ == "__main__":

    INFLUX_HOST = "influxdb"
    INFLUX_DATABASE = "covid_api"

    influx_client = DataFrameClient(INFLUX_HOST, database=INFLUX_DATABASE)

    check_db(influx_client)
    check_and_update(influx_client)
    while True:
        get_last_update(influx_client)
        logger.info("Trying again in 60 seconds!")
        time.sleep(60)
