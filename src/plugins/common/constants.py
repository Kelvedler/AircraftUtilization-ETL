import os
from typing import NamedTuple, Union


class SourceColumns(NamedTuple):
    ICAO24: str = "icao24"
    LAST_CONTACT: str = "last_contact"
    VELOCITY: str = "velocity"
    VERTICAL_RATE: str = "vertical_rate"
    TAKEOFF_AT: str = "takeoff_at"
    FLIGHT_LAST_CONTACT: str = "flight_last_contact"
    FLIGHT_TRAJECTORY: str = "flight_trajectory"
    IS_FIRST_CONTACT: str = "is_first_contact"


class ActiveFlightsColumns(NamedTuple):
    ICAO24: str = "icao24"
    TAKEOFF_AT: str = "takeoff_at"
    FLIGHT_LAST_CONTACT: str = "flight_last_contact"
    FLIGHT_TRAJECTORY: str = "flight_trajectory"
    IS_FIRST_CONTACT: str = "is_first_contact"


class S3Sts(NamedTuple):
    REGION: str
    ROLE_ARN: str
    BUCKET: str
    ROLE_SESSION: Union[str, None]


class S3RolesAnywhere(NamedTuple):
    REGION: str
    ROLE_ARN: str
    BUCKET: str
    PROFILE_ARN: Union[str, None]
    TRUST_ANCHOR_ARN: Union[str, None]
    CERTIFICATE_PATH: Union[str, None]
    PRIVATE_KEY_PATH: Union[str, None]


SOURCE_COLUMNS = SourceColumns()
SOURCE_FILENAME = "source"
ACTIVE_FLIGHTS_COLUMNS = ActiveFlightsColumns()
S3_STS = S3Sts(
    REGION=os.environ["S3_REGION"],
    ROLE_ARN=os.environ["S3_ROLE_ARN"],
    BUCKET=os.environ["S3_BUCKET"],
    ROLE_SESSION=os.getenv(key="S3_ROLE_SESSION", default=None),
)
S3_ROLES_ANYWHERE = S3RolesAnywhere(
    REGION=os.environ["S3_REGION"],
    ROLE_ARN=os.environ["S3_ROLE_ARN"],
    BUCKET=os.environ["S3_BUCKET"],
    PROFILE_ARN=os.getenv(key="S3_PROFILE_ARN", default=None),
    TRUST_ANCHOR_ARN=os.getenv(key="S3_TRUST_ANCHOR_ARN", default=None),
    CERTIFICATE_PATH=os.getenv(key="S3_CERTIFICATE_PATH", default=None),
    PRIVATE_KEY_PATH=os.getenv(key="S3_PRIVATE_KEY_PATH", default=None),
)
S3_SERVICE_NAME = os.environ["S3_SERVICE_NAME"]
