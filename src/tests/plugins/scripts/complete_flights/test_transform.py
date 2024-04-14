from io import BytesIO
import unittest

import boto3
from moto import mock_aws
import pandas as pd
from plugins.common.constants import S3Sts
from plugins.common.s3 import S3BucketConnector
from plugins.scripts.complete_flights.db import AircraftUtilizationClient
from plugins.scripts.complete_flights.transformers import CompleteFlightsETL


class AircraftUtilizationStub(AircraftUtilizationClient):
    def __init__(self) -> None:
        pass

    def write_flights(self, df: pd.DataFrame) -> None:
        pass


class TestCompleteFlightsETLMethods(unittest.TestCase):
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

        db_client = AircraftUtilizationStub()

        self.transformer = CompleteFlightsETL(
            s3_bucket=self.s3_bucket_connection, db_client=db_client
        )

    def tearDown(self) -> None:
        self.mock.stop()

    def test_is_takeoff_ok(self) -> None:
        data = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 240.52,
            "vertical_rate": 6.3,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "climb",
            "is_first_contact": True,
        }
        row = pd.Series(data=data)

        result = self.transformer._is_takeoff(row=row)

        self.assertTrue(result)

    def test_is_takeoff_not_first_contact(self) -> None:
        data = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 240.52,
            "vertical_rate": 6.3,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "climb",
            "is_first_contact": False,
        }
        row = pd.Series(data=data)

        result = self.transformer._is_takeoff(row=row)

        self.assertFalse(result)

    def test_is_takeoff_no_vertical_rate(self) -> None:
        data = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 240.52,
            "vertical_rate": 0,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "climb",
            "is_first_contact": True,
        }
        row = pd.Series(data=data)

        result = self.transformer._is_takeoff(row=row)

        self.assertFalse(result)

    def test_is_landing_ok(self) -> None:
        data = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 9.52,
            "vertical_rate": 0,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": True,
        }
        row = pd.Series(data=data)

        result = self.transformer._is_landing(row=row)

        self.assertTrue(result)

    def test_is_landing_no_last_contact(self) -> None:
        data = {
            "icao24": "a23456",
            "last_contact": 0,
            "velocity": 9.52,
            "vertical_rate": 0,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": True,
        }
        row = pd.Series(data=data)

        result = self.transformer._is_landing(row=row)

        self.assertFalse(result)

    def test_is_landing_invalid_velocity(self) -> None:
        data_high_velocity = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 11.52,
            "vertical_rate": 0,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": True,
        }
        row_high_velocity = pd.Series(data=data_high_velocity)

        result_high_velocity = self.transformer._is_landing(row=row_high_velocity)

        self.assertFalse(result_high_velocity)

    def test_is_landing_invalid_vertical_rate(self) -> None:
        data_invalid_vertical_rate1 = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 8.52,
            "vertical_rate": 1.1,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": True,
        }
        row_invalid_vertical_rate1 = pd.Series(data=data_invalid_vertical_rate1)
        data_invalid_vertical_rate2 = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 8.52,
            "vertical_rate": -1.1,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": True,
        }
        row_invalid_vertical_rate2 = pd.Series(data=data_invalid_vertical_rate2)
        result_invalid_vertical_rate1 = self.transformer._is_landing(
            row=row_invalid_vertical_rate1
        )
        result_invalid_vertical_rate2 = self.transformer._is_landing(
            row=row_invalid_vertical_rate2
        )

        self.assertFalse(result_invalid_vertical_rate1)
        self.assertFalse(result_invalid_vertical_rate2)

    def test_determine_flight_status_other(self) -> None:
        result_exp = "other"
        data = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 110.52,
            "vertical_rate": -1.1,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": False,
        }
        row = pd.Series(data=data)

        result = self.transformer._determine_flight_status(row=row)

        self.assertEqual(result, result_exp)

    def test_determine_flight_trajectory_climb(self) -> None:
        result_exp = "climb"
        data = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 110.52,
            "vertical_rate": 1.1,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": False,
        }
        row = pd.Series(data=data)

        result = self.transformer._determine_flight_trajectory(row=row)

        self.assertEqual(result, result_exp)

    def test_determine_flight_trajectory_descend(self) -> None:
        result_exp = "descend"
        data1 = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 110.52,
            "vertical_rate": -1.1,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "climb",
            "is_first_contact": False,
        }
        row1 = pd.Series(data=data1)
        data2 = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 110.52,
            "vertical_rate": 0,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "descend",
            "is_first_contact": False,
        }
        row2 = pd.Series(data=data2)

        result1 = self.transformer._determine_flight_trajectory(row=row1)
        result2 = self.transformer._determine_flight_trajectory(row=row2)

        self.assertEqual(result1, result_exp)
        self.assertEqual(result2, result_exp)

    def test_determine_flight_trajectory_other(self) -> None:
        result_exp = "other"
        data = {
            "icao24": "a23456",
            "last_contact": 1712338130,
            "velocity": 110.52,
            "vertical_rate": 0,
            "takeoff_at": 1712337230,
            "flight_last_contact": 1712338130,
            "flight_trajectory": "climb",
            "is_first_contact": False,
        }
        row = pd.Series(data=data)

        result = self.transformer._determine_flight_trajectory(row=row)

        self.assertEqual(result, result_exp)

    def test_extract_ok(self) -> None:
        key = "source.parquet"
        data_exp = {
            "icao24": ["65432a", "1b3456"],
            "last_contact": [1712338215, 0],
            "velocity": [210.11, 0.00],
            "vertical_rate": [-0.70, 0.00],
            "takeoff_at": [
                1712338215,
                1712338205,
            ],
            "flight_last_contact": [
                1712338215,
                1712338110,
            ],
            "flight_trajectory": ["other", "climb"],
            "is_first_contact": [False, False],
        }
        out_buffer = BytesIO()
        source_exp = pd.DataFrame(data=data_exp)
        source_exp.to_parquet(path=out_buffer, index=False)
        self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key)

        source = self.transformer._extract()

        self.assertTrue(source.equals(source_exp))

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": key}]})

    def test_transform_active_ok(self) -> None:
        data = {
            "icao24": ["65432a", "1b3456", "12c456"],
            "last_contact": [1712338215, 0, 1712338315],
            "velocity": [110.11, 0.00, 71.14],
            "vertical_rate": [7.49, 0.00, 11.38],
            "takeoff_at": [1712338215, 1712338205, 0],
            "flight_last_contact": [
                1712338215,
                1712338110,
                1712338315,
            ],
            "flight_trajectory": ["other", "climb", "climb"],
            "is_first_contact": [False, False, True],
            "flight_status": ["other", "other", "takeoff"],
        }
        active = pd.DataFrame(data=data)
        data_exp = {
            "icao24": ["65432a", "1b3456", "12c456"],
            "last_contact": [1712338215, 0, 1712338315],
            "velocity": [110.11, 0.00, 71.14],
            "vertical_rate": [7.49, 0.00, 11.38],
            "takeoff_at": [1712338215, 1712338205, 1712338315],
            "flight_last_contact": [
                1712338215,
                1712338110,
                1712338315,
            ],
            "flight_trajectory": ["climb", "other", "climb"],
            "is_first_contact": [False, False, True],
        }
        active_exp = pd.DataFrame(data=data_exp)

        result = self.transformer._transform_active(active=active)

        self.assertTrue(result.equals(active_exp))

    def test_transform_complete_ok(self) -> None:
        data = {
            "icao24": ["65432a", "1b3456"],
            "last_contact": [1712338215, 1712338315],
            "velocity": [9.11, 0.00],
            "vertical_rate": [0.00, 0.00],
            "takeoff_at": [1712329013, 0],
            "flight_last_contact": [
                1712338215,
                1712338315,
            ],
            "flight_trajectory": ["descend", "descend"],
            "is_first_contact": [False, False],
            "flight_status": ["landing", "landing"],
        }
        complte = pd.DataFrame(data=data)
        data_exp = {
            "icao24": ["65432a"],
            "flight_duration_minutes": 154,
            "landed_at": 1712338215,
        }
        result_exp = pd.DataFrame(data=data_exp)
        result_exp["landed_at"] = pd.to_datetime(
            result_exp["landed_at"], unit="s", utc=True
        )

        result = self.transformer._transform_complete(complete=complte)

        self.assertTrue(result.equals(result_exp))

    def test_etl_empty_source(self) -> None:
        log_exp = "Empty source report"
        with self.assertLogs() as logm:
            self.transformer.etl()
            self.assertIn(log_exp, logm.output[-1])


if __name__ == "__main__":
    unittest.main()
