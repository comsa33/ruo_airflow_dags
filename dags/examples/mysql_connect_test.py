from __future__ import annotations

import datetime
import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

local_tz = pendulum.timezone("Asia/Seoul")

required_packages = [
    "airflow-providers-mysql"
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["comsa333@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag = DAG(
    "mysql_connect_test",
    default_args=default_args,
    description="A simple test for MySQL connection",
    schedule_interval=datetime.timedelta(days=1),
    catchup=False,
    tags=["example"],
)


def mysql_connect_test():
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    logging.info("mysql_connect_test")
    hook = MySqlHook(mysql_conn_id="ruo_mysql")
    df = hook.get_pandas_df("SELECT * FROM news_scraper.daum_news LIMIT 10")
    logging.info(df)


with dag:
    mysql_connect_test_task = PythonVirtualenvOperator(
        task_id="mysql_connect_test",
        python_callable=mysql_connect_test,
        requirements=required_packages,
    )

    mysql_connect_test_task

if __name__ == "__main__":

    mysql_connect_test()
