import requests
import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime



bitcoin_api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
price_threshold = 90000


@dag(
    dag_id="crypto-bitcoin-price-branch",
    schedule="@daily",
    start_date=datetime(year=2025,month=2,day=19),
    catchup=False
)



def bitcoin_branch():
    start_task = EmptyOperator(task_id="start")
    @task(task_id="fetch_bitcoin_price")
    def fetch_bitcoin_price():
        response = requests.get(bitcoin_api_url).json()
        price = response['bitcoin']['usd']
        logging.info(f"The current price of Bitcoin is ${price}")
        return price

    @task.branch(task_id = "branch_decision")
    def branch_based_on_price(price: float):
        if price > price_threshold:
            logging.info(f"The bitcoin price (${price}) is above the threshold")
            return "process_price"
        else:
            logging.info(f"The bitcoin price (${price}) is below the threshold")
            return "skip_processing"

    
    @task(task_id = "process_price")
    def process_price():
        logging.info("Processing Bitcoin price as it's above the threshold")
        return "Price processed"

    skip_processing = EmptyOperator(task_id = "skip_processing")

    join = EmptyOperator(
        task_id = "join",
        trigger_rule = "none_failed_min_one_success"
    )

    price = fetch_bitcoin_price()
    decision = branch_based_on_price(price)
    start_task >> price >> decision
    decision >> process_price() >> join
    decision >> skip_processing >> join

bitcoin_branch()