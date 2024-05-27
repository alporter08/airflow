import pendulum
from airflow.decorators import dag, task, task_group


@dag(start_date=pendulum.now())
def some_dag():
    @task
    def some_task_1():
        print("hello world 1")

    @task
    def some_task_2():
        print("hello world 2")

    @task
    def some_task_3():
        print("hello world 3")

    @task_group
    def example_group():
        some_task_2()
        some_task_3()

    some_task_1() >> example_group()


some_dag()
