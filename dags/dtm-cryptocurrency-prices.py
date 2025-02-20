import requests
import logging

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


#List of cryptocurrencies to fetch - input for dynamic task maping
cryptocurrencies = ["ethereum","dogecoin"]
api_call_template = "http://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd"


@dag(
    dag_id = "dtm-cryptocurrency-prices",
    schedule_interval="@daily",
    start_date=datetime(year=2025, month=2, day=19),
    catchup=False
)


def crypto_prices():
    #Task to dinamically map over the list of cryptocurrencies with custom index name
    @task(map_index_template="{{crypto}}")

    def fetch_price(crypto: str):
        #Use the cryptocurrencies name in the task name
        context = get_current_context()
        context["crypto"] = crypto

        #API call to fetch the price of cryptocurrency
        api_url = api_call_template.format(crypto=crypto)
        response = requests.get(api_url).json()
        price = response[crypto]["usd"]
        logging.info(f"the price of {crypto} is ${price}")

        return price
    #Dinamically map the fetch_price task over the list of cryptocurrencies
    prices = fetch_price.partial().expand(crypto = cryptocurrencies)
    prices


#instantiate the DAG
crypto_prices()