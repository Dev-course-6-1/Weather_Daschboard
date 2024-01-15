from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pulgins import slack

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='project_redshift')
    return hook.get_conn().cursor()


@task
def etl(schema, table, lat, lon, api_key):

    # https://openweathermap.org/api/one-call-api
    # api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API key}
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    response = requests.get(url)
    data = json.loads(response.text)


    ret = []
    d = data["list"][0]
    day = datetime.strptime(d["dt_txt"], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    ret.append("('{}',{},{},{},{})".format(day, d["main"]["temp"],d["main"]["feels_like"], d["main"]["temp_min"], d["main"]["temp_max"]))

    cur = get_Redshift_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    DATETIME timestamp,
    TEMPERATURE float,
    PERTEMP float,
    MINTEMP float,
    MAXTEMP float
    );"""
    logging.info(create_table_sql)

    insert_sql = f"INSERT INTO {schema}.{table} VALUES " + ",".join(ret)
    logging.info(insert_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id = 'get_seoul_weather',
    start_date = datetime(2024,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = timedelta(hours = 3),  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack.on_failure_callback
    }
) as dag:

    etl("raw_data", "SEOULTEMP", 37.5665, 126.9780, Variable.get("ow_api_key"))
