from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
import requests
import logging

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

@dag(
    dag_id="tg2-bitcoin",
    schedule="@daily",
    start_date=datetime(year=2025, month=2, day=8),
    catchup=False
)
def main():

    transform = TaskGroup("transform")
    store = TaskGroup("store")


    # Task1
    @task(task_id="extract", retries=2,task_group=transform)
    def extract_bitcoin():
        return requests.get(API).json()['bitcoin']
    
    # Task2
    @task(task_id="transform",task_group=transform)
    def process_bitcoin(response):
        return {"usd": response["usd"], "change": response["usd_24h_change"]}


    # Task3
    @task(task_id="store",task_group=store)
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data['usd']}, change: {data['change']}")


    store_bitcoin(process_bitcoin(extract_bitcoin()))


main()
