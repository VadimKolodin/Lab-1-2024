import os
from datetime import datetime

import numpy as np
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from elasticsearch import Elasticsearch

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="file_processing",
    default_args=default_args,
    description='DAG for training classifier and getting mlflow logs',
    schedule_interval=None,
) as dag:
    @task(task_id="read")
    def read_file():
        files = os.listdir("data")
        data = pd.concat([pd.read_csv(f"data/{file}") for file in files],axis=0)
        data.to_csv("tempfile.csv")


    @task(task_id="filter_data")
    def filter_data():
        data = pd.read_csv("tempfile.csv")
        data = data[data['designation'].notna()]
        data = data[data['region_1'].notna()]
        data.to_csv("tempfile.csv")


    @task(task_id="transform_price")
    def transform_price():
        data = pd.read_csv("tempfile.csv")
        data['price'] = data['price'].replace({np.nan: 0.0})
        data.to_csv("tempfile.csv")


    @task(task_id="save_to_result_table")
    def save_to_result_table():
        data = pd.read_csv("tempfile.csv")
        data.to_csv("result.csv", index=False)


    @task(task_id="save_to_elasticsearch")
    def save_to_elasticsearch():
        data = pd.read_csv("tempfile.csv")
        client = Elasticsearch(hosts=["http://elasticsearch-kibana:9200"])
        for ind, line  in data.iterrows():
            client.index(index="lab1", body=line.to_json())

    read, filter_, transform, save, save_to_es = read_file(), filter_data(), transform_price(), save_to_result_table(), save_to_elasticsearch()
    read >> filter_ >> transform >> [save, save_to_es]

