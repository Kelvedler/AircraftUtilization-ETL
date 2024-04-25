from datetime import UTC, datetime, timedelta
from io import BytesIO
import unittest

import boto3
from moto import mock_aws
import numpy as np
import pandas as pd
from plugins.common.constants import S3Sts
from plugins.common.exceptions import InvalidResponseError, InvalidSource
from plugins.common.s3 import S3BucketConnector
from plugins.scripts.opensky.client import OpenSkyClient
from plugins.scripts.opensky.transformers import ActiveFlightsETL, SourceReports


class TestActiveFlightsETLMethods(unittest.TestCase):
    def set_default_states_monkey(self) -> None:
        states_exp = {
            "time": 1712338230,
            "states": [
                {
                    "icao24": "a23456",
                    "callsign": "Speedbird",
                    "origin_country": "Ukraine",
                    "time_position": 1712338230,
                    "last_contact": 1712338130,
                    "longitude": -37.80467681,
                    "latitude": 144.9659498,
                    "baro_altitude": 700.25,
                    "on_ground": False,
                    "velocity": 240.52,
                    "true_track": 5.154,
                    "vertical_rate": 6.3,
                    "sensors": None,
                    "geo_altitude": 620.25,
                    "squawk": "Code",
                    "spi": False,
                    "position_source": 0,
                },
            ],
        }
        self.opensky_client.get_states = lambda: states_exp

    def setUp(self) -> None:
        self.mock = mock_aws()
        self.mock.start()
        s3_credentials = S3Sts(
            REGION="us-east-2",
            ROLE_ARN="arn:aws:iam::123456789012:role/TestRunner",
            BUCKET="test-bucket",
            ROLE_SESSION="TestRunner",
        )
        self.s3_endpoint_url = f"https://s3.{s3_credentials.REGION}.amazonaws.com"
        self.s3_service_name = "sts"

        self.s3 = boto3.resource("s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(
            Bucket=s3_credentials.BUCKET,
            CreateBucketConfiguration={"LocationConstraint": s3_credentials.REGION},
        )
        self.s3_bucket = self.s3.Bucket(s3_credentials.BUCKET)
        self.s3_bucket_connection = S3BucketConnector(credentials=s3_credentials)

        opensky_auth = "test"
        self.opensky_client = OpenSkyClient(auth=opensky_auth)
        self.set_default_states_monkey()
        self.transformer = ActiveFlightsETL(
            s3_bucket=self.s3_bucket_connection, opensky_client=self.opensky_client
        )

    def tearDown(self) -> None:
        self.mock.stop()

    def test_extract_opensky_states_ok(self) -> None:
        states_data_exp = {
            "icao24": ["a23456"],
            "last_contact": [1712338130],
            "velocity": [240.52],
            "vertical_rate": [6.3],
        }
        states_exp = pd.DataFrame(data=states_data_exp)

        states = self.transformer._extract_opensky_states()

        self.assertTrue(states.equals(states_exp))

    def test_extract_opensky_states_invalid(self) -> None:
        no_states_data = {"time": 1712338230}
        self.opensky_client.get_states = lambda: no_states_data
        with self.assertRaises(InvalidResponseError) as _:
            self.transformer._extract_opensky_states()

        invalid_states_data = {"time": 1712338230, "states": "invalid"}
        self.opensky_client.get_states = lambda: invalid_states_data
        with self.assertRaises(InvalidResponseError) as _:
            self.transformer._extract_opensky_states()

        self.set_default_states_monkey()

    def test_extract_latest_source_ok(self) -> None:
        key = "source.parquet"
        data_exp = {
            "icao24": ["a23456", "65432a"],
            "last_contact": [1712338235, 1712338225],
            "velocity": [18.41, 240.52],
            "vertical_rate": [6.11, 0],
            "takeoff_at": [None, 1712338215],
            "flight_last_contact": [None, 1712338225],
            "flight_trajectory": [None, "other"],
            "is_first_contact": [None, False],
        }
        out_buffer = BytesIO()
        latest_source_exp = pd.DataFrame(data=data_exp)
        latest_source_exp.to_parquet(path=out_buffer, index=False)
        self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key)

        latest_source = self.transformer._extract_latest_source()

        self.assertTrue(latest_source.equals(latest_source_exp))

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": key}]})

    def test_extract_latest_source_invalid(self) -> None:
        key = "source.parquet"
        data_exp = {
            "icao24": ["a23456", "65432a"],
            "last_contact": [1712338235, 1712338225],
        }
        out_buffer = BytesIO()
        latest_source_exp = pd.DataFrame(data=data_exp)
        latest_source_exp.to_parquet(path=out_buffer, index=False)
        self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key)

        with self.assertRaises(InvalidSource) as _:
            self.transformer._extract_latest_source()

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": key}]})

    def test_extract_latest_source_empty(self) -> None:
        data_columns = (
            "icao24",
            "last_contact",
            "velocity",
            "vertical_rate",
            "takeoff_at",
            "flight_last_contact",
            "flight_trajectory",
            "is_first_contact",
        )
        latest_source_exp = pd.DataFrame(columns=data_columns)

        latest_source = self.transformer._extract_latest_source()

        self.assertTrue(latest_source.equals(latest_source_exp))

    def test_active_flights_from_source_ok(self) -> None:
        source_data = {
            "icao24": ["a23456", "65432a"],
            "last_contact": [1712338235, 1712338225],
            "velocity": [18.41, 240.52],
            "vertical_rate": [6.11, 0],
            "takeoff_at": [None, 1712338215],
            "flight_last_contact": [None, 1712338225],
            "flight_trajectory": [None, "other"],
            "is_first_contact": [None, False],
        }
        source = pd.DataFrame(data=source_data)
        data_exp = {
            "icao24": ["a23456", "65432a"],
            "takeoff_at": [None, 1712338215],
            "flight_last_contact": [None, 1712338225],
            "flight_trajectory": [None, "other"],
            "is_first_contact": [None, False],
        }
        active_flights_exp = pd.DataFrame(data=data_exp)

        active_flights = self.transformer._active_flights_from_source(source=source)

        self.assertTrue(active_flights.equals(active_flights_exp))

    def test_update_flight_last_contact_ok(self) -> None:
        source_data = {
            "icao24": ["a23456", "65432a", "1b3456"],
            "last_contact": [0, 1712338237, 1712338135],
            "velocity": [137.18, 240.52, 18.41],
            "vertical_rate": [-1.1, 0, 6.11],
            "takeoff_at": [0, 1712338215, 1712338205],
            "flight_last_contact": [1712338203, 1712338115, 0],
            "flight_trajectory": ["descend", "other", "climb"],
            "is_first_contact": [False, True, np.NaN],
        }
        source = pd.DataFrame(data=source_data)
        data_exp = {
            "icao24": ["a23456", "65432a", "1b3456"],
            "last_contact": [0, 1712338237, 1712338135],
            "velocity": [137.18, 240.52, 18.41],
            "vertical_rate": [-1.1, 0, 6.11],
            "takeoff_at": [0, 1712338215, 1712338205],
            "flight_last_contact": [1712338203, 1712338237, 1712338135],
            "flight_trajectory": ["descend", "other", "climb"],
            "is_first_contact": [False, True, np.NaN],
        }
        result_exp = pd.DataFrame(data=data_exp)

        result = self.transformer._update_flight_last_contact(source=source)

        self.assertTrue(result.equals(result_exp))

    def test_define_first_contact_ok(self) -> None:
        source_data = {
            "icao24": ["a23456", "65432a", "1b3456"],
            "last_contact": [0, 1712338237, 1712338135],
            "velocity": [137.18, 240.52, 18.41],
            "vertical_rate": [-1.1, 0, 6.11],
            "takeoff_at": [0, 1712338215, 1712338205],
            "flight_last_contact": [1712338203, 1712338237, 1712338135],
            "flight_trajectory": ["descend", "other", "climb"],
            "is_first_contact": [False, True, pd.NA],
        }
        source = pd.DataFrame(data=source_data)
        data_exp = {
            "icao24": ["a23456", "65432a", "1b3456"],
            "last_contact": [0, 1712338237, 1712338135],
            "velocity": [137.18, 240.52, 18.41],
            "vertical_rate": [-1.1, 0, 6.11],
            "takeoff_at": [0, 1712338215, 1712338205],
            "flight_last_contact": [1712338203, 1712338237, 1712338135],
            "flight_trajectory": ["descend", "other", "climb"],
            "is_first_contact": [False, False, True],
        }
        result_exp = pd.DataFrame(data=data_exp)

        result = self.transformer._define_first_contact(source=source)

        self.assertTrue(result.equals(result_exp))

    def test_remove_inactive_ok(self) -> None:
        inactive_last_contact = round(
            (datetime.now(tz=UTC) - timedelta(minutes=21)).timestamp()
        )
        active_last_contact = round(
            (datetime.now(tz=UTC) - timedelta(minutes=19)).timestamp()
        )
        active_flights_source_data = {
            "icao24": ["a23456", "65432a"],
            "takeoff_at": [1712338205, 1712338215],
            "flight_last_contact": [active_last_contact, inactive_last_contact],
            "flight_trajectory": ["climb", "other"],
            "is_first_contact": [False, False],
        }
        active_flights_source = pd.DataFrame(data=active_flights_source_data)
        active_flights_exp_data = {
            "icao24": ["a23456"],
            "takeoff_at": [1712338205],
            "flight_last_contact": [active_last_contact],
            "flight_trajectory": ["climb"],
            "is_first_contact": [False],
        }
        active_flights_exp = pd.DataFrame(data=active_flights_exp_data)

        active_flights_result = self.transformer._remove_inactive(
            active_flights=active_flights_source
        )

        self.assertTrue(active_flights_result.equals(active_flights_exp))

    def test_extract_ok(self) -> None:
        states_data_exp = {
            "icao24": ["a23456"],
            "last_contact": [1712338130],
            "velocity": [240.52],
            "vertical_rate": [6.3],
        }
        states_exp = pd.DataFrame(data=states_data_exp)

        key = "source.parquet"
        source_data_exp = {
            "icao24": ["a23456", "65432a"],
            "last_contact": [1712338235, 1712338225],
            "velocity": [18.41, 240.52],
            "vertical_rate": [6.11, 0],
            "takeoff_at": [None, 1712338215],
            "flight_last_contact": [None, 1712338225],
            "flight_trajectory": [None, "other"],
            "is_first_contact": [None, False],
        }
        out_buffer = BytesIO()
        latest_source_exp = pd.DataFrame(data=source_data_exp)
        latest_source_exp.to_parquet(path=out_buffer, index=False)
        self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key)

        source_reports = self.transformer._extract()

        self.assertTrue(source_reports.states.equals(states_exp))
        self.assertTrue(source_reports.latest_source.equals(latest_source_exp))

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": key}]})

    def test_transform_ok(self) -> None:
        active_last_contact = round(datetime.now(tz=UTC).timestamp())
        states_data = {
            "icao24": ["65432a", "12c456"],
            "last_contact": [
                active_last_contact,
                active_last_contact,
            ],
            "velocity": [210.11, 18.41],
            "vertical_rate": [-0.7, 6.11],
        }
        states = pd.DataFrame(data=states_data)
        source_data = {
            "icao24": ["a23456", "65432a", "1b3456"],
            "last_contact": [
                0,
                active_last_contact - 15 * 60,
                active_last_contact - 5 * 60,
            ],
            "velocity": [137.18, 240.52, 18.41],
            "vertical_rate": [-1.1, 0, 6.11],
            "takeoff_at": [0, 1712338215, 1712338205],
            "flight_last_contact": [
                active_last_contact - 25 * 60,
                active_last_contact - 15 * 60,
                active_last_contact - 5 * 60,
            ],
            "flight_trajectory": ["descend", "other", "climb"],
            "is_first_contact": [False, False, True],
        }
        latest_source = pd.DataFrame(data=source_data)
        source_reports = SourceReports(states=states, latest_source=latest_source)

        result_data = {
            "icao24": ["65432a", "12c456", "1b3456"],
            "last_contact": [active_last_contact, active_last_contact, 0],
            "velocity": [210.11, 18.41, 0.00],
            "vertical_rate": [-0.70, 6.11, 0.00],
            "takeoff_at": [
                1712338215,
                0,
                1712338205,
            ],
            "flight_last_contact": [
                active_last_contact,
                active_last_contact,
                active_last_contact - 5 * 60,
            ],
            "flight_trajectory": ["other", np.NaN, "climb"],
            "is_first_contact": [False, True, False],
        }
        result_exp = pd.DataFrame(result_data)
        result_exp = result_exp.astype(
            {
                "last_contact": pd.Int32Dtype(),
                "takeoff_at": pd.Int32Dtype(),
                "flight_last_contact": pd.Int32Dtype(),
            }
        )

        result = self.transformer._transform(source_reports=source_reports)

        self.assertTrue(result.equals(result_exp))

    def test_load_ok(self) -> None:
        key = "source.parquet"
        source_data_exp = {
            "icao24": ["65432a", "12c456", "1b3456"],
            "last_contact": [1712338215, 1712338215, 0],
            "velocity": [210.11, 18.41, 0.00],
            "vertical_rate": [-0.70, 6.11, 0.00],
            "takeoff_at": [
                1712338215,
                0,
                1712338205,
            ],
            "flight_last_contact": [
                1712338215,
                1712338215,
                1712338110,
            ],
            "flight_trajectory": ["other", np.NaN, "climb"],
            "is_first_contact": [False, True, False],
        }
        source_exp = pd.DataFrame(source_data_exp)

        self.transformer._load(source=source_exp)

        data = self.s3_bucket.Object(key=key).get().get("Body").read()
        out_buffer = BytesIO(data)
        result = pd.read_parquet(out_buffer)

        self.assertTrue(result.equals(source_exp))

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": key}]})


if __name__ == "__main__":
    unittest.main()
