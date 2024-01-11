from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG 정의
dag = DAG('air_quality_etl',
          default_args=default_args,
          description='ETL for air quality data',
          schedule_interval='35 * * * *', #필요시 시간 수정
          max_active_runs=1)


def validate_and_convert(value, data_type):
    try:
        if data_type == 'float':
            return float(value) if value.replace('.', '', 1).isdigit() else None
        elif data_type == 'int':
            return int(value) if value.isdigit() else None
    except ValueError:
        return None


@task(dag=dag)
def extract():
    logging.info("Extracting data")
    api_key = Variable.get("air_api_key")
    url = f'http://openAPI.seoul.go.kr:8088/{api_key}/json/ListAirQualityByDistrictService/1/25/'
    response = requests.get(url)
    return response.json()['ListAirQualityByDistrictService']['row']


@task(dag=dag)
def transform(extract_data):
    transformed_data = []

    for item in extract_data:
        item["MSRDATE"] = datetime.strptime(item["MSRDATE"], '%Y%m%d%H%M').strftime('%Y/%m/%d %H:%M')
        for key in ["NITROGEN", "OZONE", "CARBON", "SULFUROUS"]:
            item[key] = validate_and_convert(item[key], 'float')
        for key in ["PM10", "PM25"]:
            item[key] = validate_and_convert(item[key], 'int')

        transformed_data.append(item)

    return transformed_data


@task(dag=dag)
def load(transformed_data):
    check_query = """
    SELECT 1 FROM raw_data.airquality WHERE MSRDATE = %s AND MSRADMCODE = %s
    """

    insert_query = """
    INSERT INTO raw_data.airquality (MSRDATE, MSRADMCODE, MSRSTENAME, MAXINDEX, GRADE, POLLUTANT, NITROGEN, OZONE, CARBON, SULFUROUS, PM10, PM25)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with PostgresHook(postgres_conn_id='project_redshift').get_conn() as conn:
        with conn.cursor() as cursor:
            for item in transformed_data:
                
                cursor.execute(check_query, (item['MSRDATE'], item['MSRADMCODE']))
                if cursor.fetchone() is None:
                    
                    cursor.execute(insert_query, (item['MSRDATE'], item['MSRADMCODE'], item['MSRSTENAME'], item['MAXINDEX'], item['GRADE'], item['POLLUTANT'], item['NITROGEN'], item['OZONE'], item['CARBON'], item['SULFUROUS'], item['PM10'], item['PM25']))

        conn.commit()

extract_task = extract()
transform_task = transform(extract_task)
load_task = load(transform_task)

extract_task >> transform_task >> load_task