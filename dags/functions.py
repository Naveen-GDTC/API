from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from sqlalchemy import create_engine
import pandas as pd
import hvac
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
