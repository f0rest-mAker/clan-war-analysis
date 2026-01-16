import json
import csv
import os
import psycopg2
import requests
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, task
from airflow.utils import timezone

from custom_sensors.date_time_sensor import CustomDateTimeSensorAsync


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")

DATA_PATH = os.path.join(AIRFLOW_HOME, "data", "clan_war")
RAW_PATH = os.path.join(DATA_PATH, "raw")
PROCESSED_PATH = os.path.join(DATA_PATH, "processed")

os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(PROCESSED_PATH, exist_ok=True)

CLAN_TAG = "2RVCU8GQL"
REQUEST_URL = f"https://api.clashofclans.com/v1/clans/%23{CLAN_TAG}/currentwar"

with DAG(
    "clan_war_dag",
    schedule=None,
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_next_run = CustomDateTimeSensorAsync(
        task_id="wait_for_next_run",
        target_time="{{ dag_run.conf.get('wait_until', logical_date.isoformat()) }}"
    )

    @task(task_id="get_current_ip")
    def get_ip():
        ip_request = requests.get("https://ip.me/")
        return ip_request.text.replace("\n", "")

    @task(task_id="login_and_generate_a_token")
    def login_and_generate_a_token(**context):
        s = requests.Session()
        email = str(Variable.get("email"))
        password = str(Variable.get("password"))
        log_in = {"email": email, "password": password}
        # authorize to clash of clans api
        response = s.post(
            "https://developer.clashofclans.com/api/login", json=log_in
        ).json()
        if response["status"]["message"] != "ok":
            raise AirflowFailException(f"Failed login: {response['status']['message']}")

        # getting all keys
        ip_address = context["ti"].xcom_pull(task_ids="get_current_ip")
        response = s.post("https://developer.clashofclans.com/api/apikey/list").json()
        if response["status"]["message"] != "ok":
            raise AirflowFailException(
                "Failed to get keys: response['status']['message']"
            )

        all_keys = response["keys"]

        # if find ip match, return key, else delete it
        for key in all_keys:
            if ip_address in key["cidrRanges"]:
                return key["key"]
            else:
                s.post(
                    "https://developer.clashofclans.com/api/apikey/revoke",
                    json={"id": key["id"]},
                )

        # creating new key for new current ip
        creating_key_payload = {
            "name": "api from python",
            "description": "created at " + str(datetime.now()),
            "cidrRanges": ip_address,
        }
        response = s.post(
            "https://developer.clashofclans.com/api/apikey/create",
            json=creating_key_payload,
        ).json()
        if response["status"]["message"] != "ok":
            raise AirflowFailException(
                f"Failed to create a new key: {response['status']['message']}"
            )

        return response["key"]["key"]

    @task(task_id="fetch_clan_war_data")
    def fetch_clan_war_data(**context):
        TOKEN = context["ti"].xcom_pull(task_ids="login_and_generate_a_token")
        headers = {"Authorization": "Bearer " + TOKEN}
        request = requests.get(REQUEST_URL, headers=headers)

        if request.status_code != 200:
            error_message = f"Failed to download json file. Request status code: {request.status_code}\n"
            request_result = json.loads(request.text)
            for key, info in request_result.items():
                error_message += f"{key}: {info}\n"
            raise AirflowFailException(error_message)

        json_data = json.loads(request.text)

        output_path = os.path.join(RAW_PATH, "clan_war_data.json")

        with open(output_path, "w") as file:
            json.dump(json_data, file, indent=4)

    @task(task_id="create_clans_csv")
    def create_clans_csv():
        input_file = os.path.join(RAW_PATH, "clan_war_data.json")
        with open(input_file, "r") as file:
            data = json.load(file)
        clans_data = [["clan_tag", "clan_name", "clan_level", "war_start_time"]]
        for clan_type in ["clan", "opponent"]:
            clan = data[clan_type]
            clans_data.append([
                clan["tag"],
                clan["name"],
                clan["clanLevel"],
                data["startTime"]
            ])
        output_file = os.path.join(PROCESSED_PATH, "clans.csv")
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(clans_data)

    @task(task_id="create_players_csv")
    def create_players_csv():
        input_file = os.path.join(RAW_PATH, "clan_war_data.json")
        with open(input_file, "r") as file:
            data = json.load(file)
        players_data = [["player_tag", "player_name", "clan_tag", "townhall_level", "war_start_time"]]
        for clan_type in ["clan", "opponent"]:
            clan = data[clan_type]
            clan_tag = clan["tag"]
            for member in clan["members"]:
                players_data.append([
                    member["tag"],
                    member["name"],
                    clan_tag,
                    member["townhallLevel"],
                    data["startTime"]
                ])
        output_file = os.path.join(PROCESSED_PATH, "players.csv")
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(players_data)
    
    @task(task_id="create_war_csv")
    def create_war_csv():
        input_file = os.path.join(RAW_PATH, "clan_war_data.json")
        with open(input_file, "r") as file:
            data = json.load(file)
        war_data = [[
            "start_time",
            "end_time",
            "team_size",
            "allay_clan_tag",
            "opponent_clan_tag",
            "allay_attacks",
            "allay_stars",
            "allay_destruction_percentage",
            "opponent_attacks",
            "opponent_stars",
            "opponent_destruction_percentage",
            "is_allay_winner",
            "is_draw"
        ]]
        allay_clan = data["clan"]
        opponent_clan = data["opponent"]
        allay_stars = allay_clan["stars"]
        opponent_stars = opponent_clan["stars"]
        allay_destruction_percantage = allay_clan["destructionPercentage"]
        opponent_destruction_percantage = opponent_clan["destructionPercentage"]
        is_draw = 0
        if allay_stars >= opponent_stars or allay_destruction_percantage > opponent_destruction_percantage:
            is_allay_winner = 1
        elif allay_stars <= opponent_stars or allay_destruction_percantage < opponent_destruction_percantage:
            is_allay_winner = 0
        else:
            is_allay_winner = 0
            is_draw = 1
        war_data.append([
            data["startTime"],
            data["endTime"],
            data["teamSize"],
            allay_clan["tag"],
            opponent_clan["tag"],
            allay_clan["attacks"],
            allay_stars,
            allay_destruction_percantage,
            opponent_clan["attacks"],
            opponent_stars,
            opponent_destruction_percantage,
            is_allay_winner,
            is_draw
        ])
        output_file = os.path.join(PROCESSED_PATH, "war.csv")
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(war_data)

    @task(task_id="create_war_participants_csv")
    def create_war_participants_csv():
        input_file = os.path.join(RAW_PATH, "clan_war_data.json")
        with open(input_file, "r") as file:
            data = json.load(file)
        war_participants_data = [[
            "player_tag",
            "map_position",
            "war_start_time"
        ]]
        for clan_type in ["clan", "opponent"]:
            clan = data[clan_type]
            for member in clan["members"]:
                war_participants_data.append([
                    member["tag"],
                    member["mapPosition"],
                    data["startTime"]
                ])
        output_file = os.path.join(PROCESSED_PATH, "war_participants.csv")
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(war_participants_data)

    @task(task_id="create_attacks_csv")
    def create_attacks_csv():
        input_file = os.path.join(RAW_PATH, "clan_war_data.json")
        with open(input_file, "r") as file:
            data = json.load(file)
        attacks_data = [[
            "player_tag",
            "opponent_tag",
            "stars",
            "percentage",
            "attack_duration",
            "attack_order",
            "war_start_time"
        ]]
        for type_clan in ["clan", "opponent"]:
            clan = data[type_clan]
            for member in clan["members"]:
                if not("attacks" in member):
                    continue
                for attack in member["attacks"]:
                    attacks_data.append([
                        member["tag"],
                        attack["defenderTag"],
                        attack["stars"],
                        attack["destructionPercentage"],
                        attack["duration"],
                        attack["order"],
                        data["startTime"]
                    ])
        output_file = os.path.join(PROCESSED_PATH, "attacks.csv")
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(attacks_data)

    @task.branch(task_id="check_war_status")
    def check_war_status():
        input_file = os.path.join(RAW_PATH, "clan_war_data.json")
        with open(input_file, "r") as file:
            json_data = json.load(file)

        if json_data["state"] in ["notInWar", "inMatchmaking"]:
            return "clan_not_in_war"
        elif json_data["state"] == "preparation":
            return "clan_preparations"
        elif json_data["state"] == "inWar":
            return "clan_in_war"
        elif json_data["state"] == "warEnded":
             return "clan_war_ended"

    @task(task_id="clan_not_in_war")
    def clan_not_in_war():
        print("Clan is not in war.")

    @task(task_id="clan_preparations")
    def clan_preparations():
        print("Clan is in preparation phase.")

    @task(task_id="clan_in_war")
    def clan_in_war():
        print("Clan is currently in war.")

    @task(task_id="clan_war_ended")
    def clan_war_ended():
        print("War has ended. Proceeding to load data.")

    @task(task_id="load_clans_to_staging")
    def load_clans_to_staging():
        conn = BaseHook.get_connection("clan_war_db")
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        ) as connection:
            with connection.cursor() as cursor:
                input_file = os.path.join(PROCESSED_PATH, "clans.csv")
                with open(input_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)
                    cursor.execute("TRUNCATE TABLE stg.clans")
                    for row in reader:
                        clan_tag, clan_name, clan_level, clan_war_time = row
                        cursor.execute("""
                            INSERT INTO stg.clans (clan_tag, clan_name, clan_level, clan_war_time)
                            VALUES (%s, %s, %s, %s)
                        """, (clan_tag, clan_name, int(clan_level), clan_war_time))
                connection.commit()

    @task(task_id="load_players_to_staging")
    def load_players_to_staging():
        conn = BaseHook.get_connection("clan_war_db")
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        ) as connection:
            with connection.cursor() as cursor:
                input_file = os.path.join(PROCESSED_PATH, "players.csv")
                with open(input_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)
                    cursor.execute("TRUNCATE TABLE stg.players")
                    for row in reader:
                        player_tag, player_name, clan_tag, townhall_level, clan_war_time = row
                        cursor.execute("""
                            INSERT INTO stg.players (player_tag, player_name, clan_tag, townhall_level, clan_war_time)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (player_tag, player_name, clan_tag, int(townhall_level), clan_war_time))
                connection.commit()

    @task(task_id="load_war_to_staging")
    def load_war_to_staging():
        conn = BaseHook.get_connection("clan_war_db")
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        ) as connection:
            with connection.cursor() as cursor:
                input_file = os.path.join(PROCESSED_PATH, "war.csv")
                with open(input_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)
                    cursor.execute("TRUNCATE TABLE stg.wars")
                    for row in reader:
                        start_time, end_time, team_size, allay_clan_tag, opponent_clan_tag, \
                        allay_attacks, allay_stars, allay_destruction, opponent_attacks, \
                        opponent_stars, opponent_destruction, is_allay_winner, is_draw = row
                        cursor.execute("""
                            INSERT INTO stg.wars (
                                start_time, end_time, team_size, allay_clan_tag, opponent_clan_tag,
                                allay_attacks, allay_stars, allay_destruction_percentage,
                                opponent_attacks, opponent_stars, opponent_destruction_percentage,
                                is_allay_winner, is_draw
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (start_time, end_time, int(team_size), allay_clan_tag, opponent_clan_tag,
                              int(allay_attacks), int(allay_stars), float(allay_destruction),
                              int(opponent_attacks), int(opponent_stars), float(opponent_destruction),
                              bool(int(is_allay_winner)), bool(int(is_draw))))
                connection.commit()

    @task(task_id="load_war_participants_to_staging")
    def load_war_participants_to_staging():
        conn = BaseHook.get_connection("clan_war_db")
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        ) as connection:
            with connection.cursor() as cursor:
                input_file = os.path.join(PROCESSED_PATH, "war_participants.csv")
                with open(input_file, "r") as file:
                    cursor.execute("TRUNCATE TABLE stg.war_participants")
                    reader = csv.reader(file)
                    next(reader)
                    for row in reader:
                        player_tag, map_position, war_start_time = row
                        cursor.execute("""
                            INSERT INTO stg.war_participants (player_tag, map_position, war_start_time)
                            VALUES (%s, %s, %s)
                        """, (player_tag, int(map_position), war_start_time))
                connection.commit()

    @task(task_id="load_attacks_to_staging")
    def load_attacks_to_staging():
        conn = BaseHook.get_connection("clan_war_db")
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        ) as connection:
            with connection.cursor() as cursor:
                input_file = os.path.join(PROCESSED_PATH, "attacks.csv")
                with open(input_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)
                    cursor.execute("TRUNCATE TABLE stg.attacks")
                    for row in reader:
                        player_tag, opponent_tag, stars, percentage, attack_duration, attack_order, war_start_time = row
                        cursor.execute("SELECT player_key FROM dds.dim_players WHERE player_tag = %s AND is_current = TRUE", (player_tag,))
                        cursor.execute("""
                            INSERT INTO stg.attacks (player_tag, opponent_player_tag, stars, percentage, attack_duration, attack_order, war_start_time)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (player_tag, opponent_tag, int(stars), int(percentage), int(attack_duration), int(attack_order), war_start_time))
                connection.commit()

    @task(task_id="calculate_trigger_time", trigger_rule="none_failed")
    def calculate_trigger_time(**context):
        input_file = os.path.join(RAW_PATH, "clan_war_data.json")
        with open(input_file, "r") as file:
            data = json.load(file)
        if data["state"] in ["notInWar", "inMatchmaking"]:
            trigger_time = timezone.utcnow() + timedelta(hours=12)
        elif data["state"] == "warEnded":
            trigger_time = timezone.utcnow() + timedelta(days=1)
        else:
            end_time_str = data["endTime"]
            end_time = datetime.strptime(end_time_str, "%Y%m%dT%H%M%S.%fZ")
            trigger_time = end_time + timedelta(minutes=1)
        return trigger_time.replace(tzinfo=timezone.utc).isoformat()
    
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id="clan_war_dag",
        conf={
            "wait_until": "{{ ti.xcom_pull(task_ids='calculate_trigger_time') }}"
        }
    )


    ip = get_ip()
    token = login_and_generate_a_token()
    fetch = fetch_clan_war_data()
    branch = check_war_status()
    not_in_war = clan_not_in_war()
    preparations = clan_preparations()
    in_war = clan_in_war()
    ended = clan_war_ended()
    clans_csv = create_clans_csv()
    players_csv = create_players_csv()
    war_csv = create_war_csv()
    participants_csv = create_war_participants_csv()
    attacks_csv = create_attacks_csv()
    calculate_trigger = calculate_trigger_time()

    start_staging = EmptyOperator(task_id="start_staging")

    staging_clans = load_clans_to_staging()
    staging_players = load_players_to_staging()
    staging_war = load_war_to_staging()
    staging_participants = load_war_participants_to_staging()
    staging_attacks = load_attacks_to_staging()

    start_loading_dds = EmptyOperator(task_id="start_loading_dds")

    load_players = SQLExecuteQueryOperator(
        task_id="load_players",
        conn_id="clan_war_db",
        sql="SELECT dds.load_dim_players()"
    )
    load_clans = SQLExecuteQueryOperator(
        task_id="load_clans",
        conn_id="clan_war_db",
        sql="SELECT dds.load_dim_clans()"
    )
    load_war = SQLExecuteQueryOperator(
        task_id="load_war",
        conn_id="clan_war_db",
        sql="SELECT dds.load_fact_war()"
    )
    load_attacks = SQLExecuteQueryOperator(
        task_id="load_attacks",
        conn_id="clan_war_db",
        sql="SELECT dds.load_fact_attacks()"
    )
    load_war_participants = SQLExecuteQueryOperator(
        task_id="load_war_participants",
        conn_id="clan_war_db",
        sql="SELECT dds.load_dim_war_participants()"
    )


    start >> wait_for_next_run >> ip >> token >> fetch >> branch
    branch >> [not_in_war, preparations, in_war, ended]
    [not_in_war, preparations, in_war] >> calculate_trigger
    calculate_trigger >> trigger_next
    ended >> [clans_csv, players_csv, war_csv, participants_csv, attacks_csv] >> start_staging
    start_staging >> [staging_clans, staging_players, staging_war, staging_participants, staging_attacks] >> start_loading_dds
    start_loading_dds >> load_players >> load_clans >> load_war
    load_war >> [load_attacks, load_war_participants] >> calculate_trigger >> trigger_next
