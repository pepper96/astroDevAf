from datetime import datetime
from airflow import DAG
import requests
import logging
from airflow.operators.python import PythonOperator

API ="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

#TODO Extract 
def extract_bitcoin():
    return requests.get(API).json()["bitcoin"]

#TODO process
def process_bitcoin(ti):
    response = ti.xcom_pull(task_ids="extract_bitcoin_from_api_trad")
    logging.info(response)
    processed_data = {"usd":response["usd"],"change":response["usd_24h_change"]}
    ti.xcom_push(key="processed_data",value=processed_data)

#TODO store
def store_bitcoin(ti):
    data = ti.xcom_pull(task_ids="process_bitcoin_from_api_trad",key="processed_data")
    logging.info(data)


with DAG(
    dag_id = "cls-bitcoin_trad",
    schedule="@daily",
    start_date= datetime(year=2025,month=2,day=8),
    catchup=False
):
    #TODO Task1
    extract_bitcoin_from_api_trad = PythonOperator(
        task_id = "extract_bitcoin_from_api_trad",
        python_callable=extract_bitcoin
    )
    #TODO Task2
    process_bitcoin_from_api_trad = PythonOperator(
        task_id="process_bitcoin_from_api_trad",
        python_callable=process_bitcoin
    )
    #TODO Task3
    store_bitcoin_from_api_trad = PythonOperator(
        task_id="store_bitcoin_from_api_trad",
        python_callable=store_bitcoin
    )

    #TODO Dependencies
    extract_bitcoin_from_api_trad >> process_bitcoin_from_api_trad >> store_bitcoin_from_api_trad