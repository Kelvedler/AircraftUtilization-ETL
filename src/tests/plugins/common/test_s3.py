from io import BytesIO
from typing import Dict, List
import unittest

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
import pandas as pd
from plugins.common.constants import S3Sts

from plugins.common.s3 import S3BucketConnector


class TestS3BucketConnectorMethods(unittest.TestCase):
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

    def tearDown(self) -> None:
        self.mock.stop()

    def test_get_code_from_client_error_ok(self) -> None:
        code_exp = "NoSuchKey"
        try:
            self.s3_bucket.Object(key="test").get().get("Body").read()
        except Exception as err:
            if isinstance(err, ClientError):
                result = self.s3_bucket_connection._get_code_from_client_error(err=err)

                self.assertEqual(result, code_exp)

    def test_read_parquet_ok(self) -> None:
        filename = "test_file"
        key = f"{filename}.parquet"
        out_buffer = BytesIO()
        test_data: Dict[str, List[int]] = {"col1": [1, 2], "col2": [3, 4]}
        df_expected = pd.DataFrame(data=test_data)
        df_expected.to_parquet(path=out_buffer, index=False)
        self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key)

        result = self.s3_bucket_connection.read_parquet(filename=filename)

        self.assertTrue(result.equals(df_expected))

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": key}]})

    def test_read_parquet_empty(self) -> None:
        filename = "test_file"

        result = self.s3_bucket_connection.read_parquet(filename=filename)

        self.assertTrue(result.empty)

    def test_upload_to_parquet(self) -> None:
        filename = "test_file"
        key = f"{filename}.parquet"
        test_data: Dict[str, List[int]] = {"col1": [1, 2], "col2": [4, 5]}
        df_expected = pd.DataFrame(data=test_data)

        self.s3_bucket_connection.upload_to_parquet(df=df_expected, filename=filename)

        data = self.s3_bucket.Object(key=key).get().get("Body").read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)

        self.assertTrue(df_result.equals(df_expected))

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": key}]})


if __name__ == "__main__":
    unittest.main()
