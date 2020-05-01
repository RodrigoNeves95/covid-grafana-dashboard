import requests
import pandas as pd
import time
import datetime

from influxdb import DataFrameClient

INFLUX_HOST = "influxdb"


def process_data(data):

    data = data.set_index("data")

    data.index = pd.to_datetime(data.index, dayfirst=True)
    data = data.sort_index()

    data = data

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


def ingest_data(confirmados, recuperados, obitos, sintomas, geral):

    influx_cli = DataFrameClient(host=INFLUX_HOST, database="covid_api")

    print("A ingerir dados para a tabela confirmados...")
    response = influx_cli.write_points(confirmados, "confirmados", database="covid_api")
    print(f"Resposta {response}")

    print("A ingerir dados para a tabela obitos...")
    response = influx_cli.write_points(obitos, "obitos", database="covid_api")
    print(f"Resposta {response}")

    print("A ingerir dados para a tabela recuperados...")
    response = influx_cli.write_points(recuperados, "recuperados", database="covid_api")
    print(f"Resposta {response}")

    print("A ingerir dados para a tabela sintomas...")
    response = influx_cli.write_points(sintomas, "sintomas", database="covid_api")
    print(f"Resposta {response}")

    print("A ingerir dados para a tabela geral...")
    response = influx_cli.write_points(geral, "geral", database="covid_api")
    print(f"Resposta {response}")

    influx_cli.close()


def check_db():
    influx_cli = DataFrameClient(host=INFLUX_HOST, database="covid_api")
    try:
        influx_cli.get_list_measurements()
    except Exception as e:
        influx_cli.create_database("covid_api")


def check_db_data():
    influx_cli = DataFrameClient(host=INFLUX_HOST, database="covid_api")

    measurements = influx_cli.get_list_measurements()

    if len(measurements) > 1:
        print("Data already present. Checking where to resume!")
        last_entry_db = influx_cli.query(
            "SELECT * FROM confirmados ORDER BY time DESC LIMIT 1"
        )["confirmados"].index

        current_day = datetime.datetime.today()

        if pd.Timestamp(last_entry_db.values[0]) < current_day:
            for date in pd.date_range(
                pd.Timestamp(last_entry_db.values[0]), current_day
            ):
                date = date.strftime("%d-%m-%Y")
                try:
                    print(f"Fetching data for {date}")
                    response = requests.get(
                        f"https://covid19-api.vost.pt/Requests/get_entry/{date}"
                    )
                    if response.status_code == 500:
                        print("Data is updated. Proceeding...")
                    elif response.status_code == 200:
                        json = response.json()
                        data = pd.DataFrame.from_dict(json)
                        (
                            confirmados,
                            recuperados,
                            obitos,
                            sintomas,
                            geral,
                        ) = process_data(data)
                        ingest_data(confirmados, recuperados, obitos, sintomas, geral)
                    else:
                        raise RuntimeError
                except Exception as e:
                    print(e)
    else:
        data = pd.DataFrame.from_dict(
            requests.get(
                "https://covid19-api.vost.pt/Requests/get_full_dataset"
            ).json(),
        )

        confirmados, recuperados, obitos, sintomas, geral = process_data(data)
        ingest_data(confirmados, recuperados, obitos, sintomas, geral)


def query_api():
    influx_cli = DataFrameClient(host=INFLUX_HOST, database="covid_api")

    while True:
        last_entry_db = influx_cli.query(
            "SELECT * FROM confirmados ORDER BY time DESC LIMIT 1"
        )["confirmados"].index

        try:
            response = requests.get(
                "https://covid19-api.vost.pt/Requests/get_last_update"
            )
            if response.status_code == 500:
                print(response.json())
            elif response.status_code == 200:
                json = response.json()
                data = pd.DataFrame.from_dict(json, orient="index",).T
                print("Checking if data is already updated!")
                timestamp = pd.Timestamp(
                    pd.to_datetime(data.data, dayfirst=True, utc=True).values[0]
                ).strftime("%d-%m-%Y")
                if (last_entry_db < pd.to_datetime(data.data, dayfirst=True, utc=True))[
                    0
                ]:
                    print(f"Data updated for day {timestamp}")
                    print("updating data with new cases.!!!!!")
                    (confirmados, recuperados, obitos, sintomas, geral,) = process_data(
                        data
                    )

                    ingest_data(confirmados, recuperados, obitos, sintomas, geral)
                else:
                    print("Data is already updated!")
                    print("Going to sleep for 60 sec!")
                    time.sleep(60)

            else:
                raise RuntimeError
        except Exception as e:
            print(e)


if __name__ == "__main__":

    check_db()
    check_db_data()
    while True:
        query_api()
