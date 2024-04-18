from datetime import datetime, timedelta
import logging
import logging.config

from airflow.decorators import task
from airflow.models.dag import DAG
from plugins.common.s3 import S3BucketConnector
from plugins.scripts.complete_flights.constants import MONGODB
from plugins.scripts.complete_flights.db import AircraftUtilizationClient
from plugins.scripts.complete_flights.transformers import CompleteFlightsETL
from plugins.scripts.opensky.client import OpenSkyClient
from plugins.scripts.opensky.transformers import ActiveFlightsETL


logger = logging.getLogger(__name__)


@task(retries=2, retry_delay=timedelta(seconds=30))
def active_flights_report():
    logger.info("Starting Active Flights ETL task")
    s3_credentials = S3BucketConnector.get_credentials()
    s3_bucket = S3BucketConnector(credentials=s3_credentials)
    opensky_client = OpenSkyClient()
    transformer = ActiveFlightsETL(s3_bucket=s3_bucket, opensky_client=opensky_client)
    transformer.etl()
    logger.info("Active Flights ETL task finished")


@task()
def complete_flights_report():
    logger.info("Starting Complete Flights ETL task")
    s3_credentials = S3BucketConnector.get_credentials()
    s3_bucket = S3BucketConnector(credentials=s3_credentials)
    db_client = AircraftUtilizationClient(credentials=MONGODB)
    transformer = CompleteFlightsETL(s3_bucket=s3_bucket, db_client=db_client)
    transformer.etl()
    logger.info("Complete Flights ETL task finished")


with DAG(
    dag_id="adsb_etl",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
) as dag:
    active_flights_report() >> complete_flights_report()
