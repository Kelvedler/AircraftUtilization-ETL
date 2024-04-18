from functools import singledispatchmethod
from io import BytesIO
import logging
from typing import Union

import boto3
from botocore.exceptions import ClientError
from iam_rolesanywhere_session import IAMRolesAnywhereSession
import pandas as pd
from pandas.core.api import DataFrame

from plugins.common.constants import (
    S3_ROLES_ANYWHERE,
    S3_SERVICE_NAME,
    S3_STS,
    S3RolesAnywhere,
    S3Sts,
    all_fields_present,
)
from plugins.common.exceptions import InvalidCredentials


class S3BucketConnector:
    def __init__(self, credentials: Union[S3Sts, S3RolesAnywhere]) -> None:
        self._logger = logging.getLogger(__name__)

        self._bucket_name = credentials.BUCKET
        self._endpoint_url = f"https://{credentials.REGION}.amazonaws.com"
        self._session = self._get_session(credentials)
        self._s3 = self._session.client("s3")

    @staticmethod
    def get_credentials() -> Union[S3Sts, S3RolesAnywhere]:
        if S3_SERVICE_NAME == "sts":
            return S3_STS
        elif S3_SERVICE_NAME == "rolesanywhere":
            return S3_ROLES_ANYWHERE
        else:
            raise NotImplementedError(f"Unknown service name: {S3_SERVICE_NAME}")

    @singledispatchmethod
    @staticmethod
    def _get_session(credentials: Union[S3Sts, S3RolesAnywhere]) -> boto3.Session:
        raise NotImplementedError("Unknown service name")

    @_get_session.register
    @staticmethod
    def _(credentials: S3Sts) -> boto3.Session:
        if not all_fields_present(credentials):
            raise InvalidCredentials("S3 sts credentials are not valid")

        role_credentials = boto3.client("sts").assume_role(
            RoleArn=credentials.ROLE_ARN,
            RoleSessionName=credentials.ROLE_SESSION,
        )["Credentials"]

        return boto3.Session(
            aws_access_key_id=role_credentials["AccessKeyId"],
            aws_secret_access_key=role_credentials["SecretAccessKey"],
            aws_session_token=role_credentials["SessionToken"],
            region_name=credentials.REGION,
        )

    @_get_session.register
    @staticmethod
    def _(credentials: S3RolesAnywhere) -> boto3.Session:
        if not all_fields_present(credentials):
            raise InvalidCredentials("S3 rolesanywhere credentials are not valid")

        roles_anywhere_session = IAMRolesAnywhereSession(
            profile_arn=credentials.PROFILE_ARN,
            role_arn=credentials.ROLE_ARN,
            trust_anchor_arn=credentials.TRUST_ANCHOR_ARN,
            certificate=credentials.CERTIFICATE_PATH,
            private_key=credentials.PRIVATE_KEY_PATH,
            region=credentials.REGION,
        )
        return roles_anywhere_session.get_session()

    @staticmethod
    def _get_code_from_client_error(err: ClientError) -> str:
        error_resp = err.response.get("Error")
        if not error_resp:
            return ""
        code = error_resp.get("Code")
        return code if code else ""

    def read_parquet(self, filename: str) -> pd.DataFrame:
        key = filename + ".parquet"
        file = f"{self._endpoint_url}/{self._bucket_name}/{key}"
        self._logger.info(f"Reading file {file}")
        try:
            data = (
                self._s3.get_object(Bucket=self._bucket_name, Key=key)
                .get("Body")
                .read()
            )
        except ClientError as e:
            if self._get_code_from_client_error(e) == "NoSuchKey":
                self._logger.info(f"File {file} not found")
                return pd.DataFrame()
            else:
                raise
        out_buffer = BytesIO(data)
        df = pd.read_parquet(out_buffer)
        return df

    def upload_to_parquet(self, df: DataFrame, filename: str) -> None:
        key = filename + ".parquet"
        file = f"{self._endpoint_url}/{self._bucket_name}/{key}"
        self._logger.info(f"Writing file {file}")

        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        self._s3.put_object(
            Body=out_buffer.getvalue(), Bucket=self._bucket_name, Key=key
        )
