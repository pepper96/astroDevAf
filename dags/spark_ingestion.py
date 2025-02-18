import logging
import requests

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

@dag(
    dag_id="tf-bitcoin-var",
    schedule="@daily",
    start_date=datetime(year=2025,month=2,day=17),
    catchup=False)

def bitcoin():
    @task(task_id="extract",retries=2)
    def extract_bitcoin():
        api_url = Variable.get("bitcoin_api_url")
        return requests.get(api_url).json()["bitcoin"]
    
    @task(task_id="transform")
    def process_bitcoin(response):
        return {"usd":response["usd"],"change":response["usd_24h_change"]}
    
    @task(task_id="store")
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data["usd"]}, change:{data["change"]}")
    

    store_bitcoin(process_bitcoin(extract_bitcoin()))


bitcoin()