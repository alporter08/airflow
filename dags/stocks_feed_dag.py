import pendulum
from datetime import datetime, timedelta
import requests
import logging
import json
import time

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

logger = logging.getLogger(__name__)

base = ObjectStoragePath("s3://aws_default@alp-airflow/stocks_feed/")


@dag(
    schedule=AfterWorkdayTimetable(),
    start_date=pendulum.datetime(2024, 5, 1, tz="UTC"),
    catchup=True,
    tags=["prod"],
)
def stocks_feed_dag():
    """
    ### Object Storage Tutorial Documentation
    This is a tutorial DAG to showcase the usage of the Object Storage API.
    Documentation that goes along with the Airflow Object Storage tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/objectstorage.html)
    """

    @task
    def get_stocks_data(**kwargs) -> ObjectStoragePath:
        """
        Get stocks data from Polygon API. The data is saved as parquet.
        """

        API_KEY = Variable.get("POLYGON_API_KEY")

        execution_date = str(kwargs["logical_date"].date())
        logging.info(f"Execution date is :{execution_date}")
        start_date = str(kwargs["data_interval_start"].date())
        logging.info(f"Start date is :{start_date}")
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{execution_date}?adjusted=true&apiKey={API_KEY}"
        r = requests.get(url, timeout=60)
        data = r.json()

        # ensure the bucket exists
        base.mkdir(exist_ok=True)

        formatted_date = execution_date.format("YYYYMMDD")
        path = base / "daily_stocks_raw" / f"{formatted_date}.json"

        with path.open("w") as file:
            json.dump(data, file)

        time.sleep(13)
        return path

    @task
    def make_parquet(path: ObjectStoragePath):

        import pandas as pd

        with open(path) as json_file:
            data = json.load(json_file)
        df = pd.DataFrame(data["results"])
        print(df.head())

    obj_path = get_stocks_data()
    make_parquet(obj_path)


dag_object = stocks_feed_dag()


if __name__ == "__main__":
    dag_object.test()
