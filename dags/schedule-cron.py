import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup


API ="https://api.coingecko.com/api/v3/simple/price?ids=\
bitcoin&vs_currencies=usd&include_market_cap=true&include_\
    24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


''' 
Cron [Preset Schedule Expressions]:

@once: Run the DAG once as soon as the DAG is trigged
@hourly: Run the DAG every hour (0 * * * *)
@weekly: Run the DAG once a week on Sundat at midnight (0 0 * * 0)
@monthly: Run the DAG once a month on the first day of the month at midnight (0 0 1 * *)
@yearly or @annualy: Run the DAG once a year on January 1st at midnight (0 0 1 1 *)
'''



@dag(
    dag_id = "schedule-cron",
    schedule="*/30 * * * *",
    start_date=datetime(year=2025,month=2,day=11),
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