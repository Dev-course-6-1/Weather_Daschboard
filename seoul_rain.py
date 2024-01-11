from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
from airflow.hooks.postgres_hook import PostgresHook
import logging
import xmltodict
import json

# redshift 연결정보
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='my_redshift')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 서울시 강수량 정보 api
@task
def get_rain_info():
    url = 'http://openapi.seoul.go.kr:8088/5a46614d656a77353132364857467759/xml/ListRainfallService/1/1000/'
    res = requests.get(url)
    xml_data = res.text

    # XML을 JSON으로 변환
    json_data = json.loads(json.dumps(xmltodict.parse(xml_data), ensure_ascii=False))
    row_data = json_data["ListRainfallService"]["row"]
    
    # 구 이름과 강수량만 추출
    result = [{data['GU_NAME']: data['RAINFALL10']} for data in row_data]

    return  result

# incremental update 형식의 로드
# incremental update 형식의 로드
@task
def load(schema, table, rains):
    logging.info("Load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
            CREATE TABLE {schema}.{table} (
                gu_name VARCHAR(255),
                rainfall float
            );""")
        
        # gu_name을 기준으로 중복 제거하고 가장 큰 rainfall 값으로 데이터 삽입
        unique_gu_names = set([list(c.keys())[0] for c in rains])
        for gu_name in unique_gu_names:
            max_rainfall = max([float(list(c.values())[0]) for c in rains if list(c.keys())[0] == gu_name])
            sql = f"INSERT INTO {schema}.{table} VALUES ('{gu_name}', {max_rainfall});"
            cur.execute(sql)
            
        cur.execute("COMMIT;")
        logging.info("Load done")
    # except Exception as error:
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")



with DAG(
    dag_id='rain_info',
    start_date=datetime(2023, 12, 25),
    catchup=False,
    tags=['API'],
    schedule='* 1 * * *'
) as dag:

    results = get_rain_info()
    load("raw_data", "rain_info", results)
