import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['juan'])
def my_awesome_dag():
    """
    ### My Dag !

    Well, this is my DAG

    This has 3 tasks:

    - Task 1
    - Task 2
    - Task 3
    """

    @task()
    def extract():
        """
        #### Task Number 1
        Task description
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        print("Logggggggging")

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Task Number 2
        Another Task description
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        print("Brooooooos")

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        """
        #### Task Number 3
        Yet Another Task description
        """
        print(f"Total order value is: {total_order_value:.2f}")


    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

my_dag = my_awesome_dag()