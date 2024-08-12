import pendulum
from datetime import datetime, timedelta
import requests
import logging
import json
import time

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from workday import AfterWorkdayTimetable

logger = logging.getLogger(__name__)

base = ObjectStoragePath("s3://aws_default@alp-airflow/stocks_feed/")


def process_stocks_data(path):
    import pandas as pd

    stocks_data_dtypes = {
        "T": "string",
        "v": "float64",
        "vw": "float64",
        "o": "float64",
        "c": "float64",
        "h": "float64",
        "l": "float64",
        "t": "int",
        "n": "float64",
    }

    with path.open("r") as json_file:
        data = json.load(json_file)
    df = pd.DataFrame(data["results"]).astype(stocks_data_dtypes)
    print(df.head())
    return df


def process_index_data(path):
    import pandas as pd

    index_data_dtypes = {"date": "string", "value": "float64"}
    with path.open("r") as json_file:
        data = json.load(json_file)
    df = pd.DataFrame(data["observations"])
    df = (
        df[df["value"] != "."]
        .drop(["realtime_start", "realtime_end"], axis=1)
        .astype(index_data_dtypes)
        .rename(columns={"value": "close"})
    )
    print(df.head())
    return df


def save_df_to_parquet(df, subdir, fname, start_date):
    formatted_date = start_date.format("YYYYMMDD")
    target_path = base / subdir / f"{formatted_date}_{fname}.parquet"
    target_path.parent.mkdir(exist_ok=True)

    logging.info("Saving to parquet: %s" % target_path)
    with target_path.open("wb") as parquet_file:
        df.to_parquet(parquet_file, index=False)


@dag(
    schedule=AfterWorkdayTimetable(),  # timedelta(days=1),  #
    start_date=pendulum.datetime(2024, 7, 26, tz="US/Eastern"),
    catchup=True,
    tags=["prod"],
)
def stocks_feed_dag_v2():
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
        Get stocks data from Polygon API. The data is saved as json.
        """

        API_KEY = Variable.get("POLYGON_API_KEY")

        execution_date = str(kwargs["logical_date"].date())
        logging.info(f"Execution date is :{execution_date}")
        start_date = str(kwargs["data_interval_start"].date())
        logging.info(f"Start date is :{start_date}")
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{start_date}?adjusted=true&apiKey={API_KEY}"  # f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?sort=asc&apiKey={API_KEY}"
        r = requests.get(url, timeout=60)
        data = r.json()

        # ensure the bucket exists
        base.mkdir(exist_ok=True)

        formatted_date = start_date.format("YYYYMMDD")
        path = base / "daily_stocks_raw" / f"{formatted_date}_daily_stocks_raw.json"

        with path.open("w") as file:
            json.dump(data, file)

        time.sleep(13)
        return path

    @task
    def get_index_data(**kwargs) -> ObjectStoragePath:
        """
        Get SP500 data from FRED API. The data is saved as json.
        """

        start_date = str(kwargs["data_interval_start"].date())

        API_KEY = Variable.get("FRED_API_KEY")

        url = f"https://api.stlouisfed.org/fred/series/observations?series_id=SP500&api_key={API_KEY}&file_type=json"
        r = requests.get(url, timeout=60)
        data = r.json()

        # ensure the bucket exists
        base.mkdir(exist_ok=True)

        formatted_date = start_date.format("YYYYMMDD")
        path = base / "fred_sp500_raw" / f"{formatted_date}_fred_sp500_raw.json"

        with path.open("w") as file:
            json.dump(data, file)

        time.sleep(13)
        return path

    @task
    def make_parquet(path: ObjectStoragePath, source_data, **kwargs):
        start_date = str(kwargs["data_interval_start"].date())
        if source_data == "stocks":
            df = process_stocks_data(path)
            save_df_to_parquet(df, "daily_stocks_parquet", "daily_stocks", start_date)
        elif source_data == "index":
            df = process_index_data(path)
            save_df_to_parquet(df, "fred_sp500_parquet", "fred_sp500", start_date)

    obj_path = get_stocks_data()
    make_parquet(obj_path, "stocks")
    obj_path = get_index_data()
    make_parquet(obj_path, "index")


dag_object = stocks_feed_dag_v2()

if __name__ == "__main__":
    dag_object.test()
