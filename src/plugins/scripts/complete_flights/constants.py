import os
from typing import NamedTuple, Union


def to_int_or_none(value: str) -> Union[int, None]:
    try:
        return int(value)
    except ValueError:
        return None


class CompleteFlightsColumns(NamedTuple):
    ICAO24: str = "icao24"
    FLIGHT_DURATION_MINUTES: str = "flight_duration_minutes"
    LANDED_AT: str = "landed_at"


class CompleteFlightsColumnsV2(NamedTuple):
    ICAO24: str = "icao24"
    FLIGHT_DURATION_MINUTES: str = "flight_duration_minutes"
    LANDED_AT: str = "landed_at"
    REGISTRATION: str = "registration"
    MODEL: str = "model"
    MANUFACTURER_ICAO: str = "manufacturer_icao"
    OWNER: str = "owner"
    OPERATOR: str = "operator"
    BUILT: str = "built"


class FlightStatuses(NamedTuple):
    TAKEOFF: str = "takeoff"
    OTHER: str = "other"
    LANDING: str = "landing"


class FlightTrajectories(NamedTuple):
    CLIMB: str = "climb"
    DESCEND: str = "descend"
    OTHER: str = "other"


class Mongodb(NamedTuple):
    HOST: Union[str, None]
    PORT: Union[int, None]
    USERNAME: Union[str, None]
    PASSWORD: Union[str, None]
    DB: Union[str, None]


COMPLETE_FLIGHTS_COLUMNS = CompleteFlightsColumns()
COMPLETE_FLIGHTS_COLUMNS_V2 = CompleteFlightsColumnsV2()
FLIGHT_STATUSES = FlightStatuses()
FLIGHT_TRAJECTORIES = FlightTrajectories()
FLIGHT_STATUS_COLUMN = "flight_status"

MONGODB = Mongodb(
    HOST=os.getenv(key="MONGODB_HOST", default=None),
    PORT=to_int_or_none(os.getenv(key="MONGODB_PORT", default="")),
    USERNAME=os.getenv(key="MONGODB_USERNAME", default=None),
    PASSWORD=os.getenv(key="MONGODB_PASSWORD", default=None),
    DB=os.getenv(key="MONGODB_DB", default=None),
)
