import os
from typing import NamedTuple, Union


def all_fields_present(self: NamedTuple):
    values = tuple(self._asdict().values())
    for value in values:
        if value is None:
            return False
    return True


class SourceColumns(NamedTuple):
    ICAO24: str = "icao24"
    LAST_CONTACT: str = "last_contact"
    VELOCITY: str = "velocity"
    VERTICAL_RATE: str = "vertical_rate"
    TAKEOFF_AT: str = "takeoff_at"
    FLIGHT_LAST_CONTACT: str = "flight_last_contact"
    FLIGHT_TRAJECTORY: str = "flight_trajectory"
    IS_FIRST_CONTACT: str = "is_first_contact"


class MetaColumns(NamedTuple):
    ICAO24: str = "icao24"
    REGISTRATION: str = "registration"
    MODEL: str = "model"
    MANUFACTURER_ICAO: str = "manufacturericao"
    OWNER: str = "owner"
    OPERATOR: str = "operator"
    BUILT: str = "built"


class ActiveFlightsColumns(NamedTuple):
    ICAO24: str = "icao24"
    TAKEOFF_AT: str = "takeoff_at"
    FLIGHT_LAST_CONTACT: str = "flight_last_contact"
    FLIGHT_TRAJECTORY: str = "flight_trajectory"
    IS_FIRST_CONTACT: str = "is_first_contact"


class S3Sts(NamedTuple):
    REGION: Union[str, None]
    ROLE_ARN: Union[str, None]
    BUCKET: Union[str, None]
    ROLE_SESSION: Union[str, None]


class S3RolesAnywhere(NamedTuple):
    REGION: Union[str, None]
    ROLE_ARN: Union[str, None]
    BUCKET: Union[str, None]
    PROFILE_ARN: Union[str, None]
    TRUST_ANCHOR_ARN: Union[str, None]
    CERTIFICATE_PATH: Union[str, None]
    PRIVATE_KEY_PATH: Union[str, None]


SOURCE_COLUMNS = SourceColumns()
SOURCE_FILENAME = os.getenv(key="SOURCE_FILENAME", default="source")
META_COLUMNS = MetaColumns()
META_FILENAME = os.getenv(key="META_FILENAME", default="metafile")
ACTIVE_FLIGHTS_COLUMNS = ActiveFlightsColumns()
S3_STS = S3Sts(
    REGION=os.getenv(key="S3_REGION", default=None),
    ROLE_ARN=os.getenv(key="S3_ROLE_ARN", default=None),
    BUCKET=os.getenv(key="S3_BUCKET", default=None),
    ROLE_SESSION=os.getenv(key="S3_ROLE_SESSION", default=None),
)
S3_ROLES_ANYWHERE = S3RolesAnywhere(
    REGION=os.getenv(key="S3_REGION", default=None),
    ROLE_ARN=os.getenv(key="S3_ROLE_ARN", default=None),
    BUCKET=os.getenv(key="S3_BUCKET", default=None),
    PROFILE_ARN=os.getenv(key="S3_PROFILE_ARN", default=None),
    TRUST_ANCHOR_ARN=os.getenv(key="S3_TRUST_ANCHOR_ARN", default=None),
    CERTIFICATE_PATH=os.getenv(key="S3_CERTIFICATE_PATH", default=None),
    PRIVATE_KEY_PATH=os.getenv(key="S3_PRIVATE_KEY_PATH", default=None),
)
S3_SERVICE_NAME = os.getenv(key="S3_SERVICE_NAME", default="sts")
