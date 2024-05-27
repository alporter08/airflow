# airflow

How to set up and run airflow on local machine:
* Create a new python virtual environment (python==3.11): `python -m venv .venv`
* Activate venv: `source .venv/bin/activate`
* Install Airflow with Amazon provider:
```
AIRFLOW_VERSION=2.9.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[amazon]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```
* Initialize airflow standalone (no proxy is due to known python bug, and set AIRFLOW_HOME to current dir): `NO_PROXY="*" AIRFLOW_HOME="$(pwd)/airflow" airflow standalone`
* In `airflow/airflow.cfg`, set load_examples to False: `load_examples = False`
* Reset db to remove examples: `NO_PROXY="*" AIRFLOW_HOME="$(pwd)/airflow" airflow db reset`
* Set path to dags: `dags_folder = PATH/TO/dags`
* Add `airflow/` to .gitignore

Inspired from this [post](https://blog.devgenius.io/an-introduction-to-airflow-setting-up-a-local-environment-and-writing-your-first-dag-2060d378acbd)
