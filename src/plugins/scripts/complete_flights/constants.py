import os
from typing import NamedTuple


class CompleteFlightsColumns(NamedTuple):
    ICAO24: str = "icao24"
    FLIGHT_DURATION_MINUTES: str = "flight_duration_minutes"
    LANDED_AT: str = "landed_at"


class FlightStatuses(NamedTuple):
    TAKEOFF: str = "takeoff"
    OTHER: str = "other"
    LANDING: str = "landing"


class FlightTrajectories(NamedTuple):
    CLIMB: str = "climb"
    DESCEND: str = "descend"
    OTHER: str = "other"


class Mongodb(NamedTuple):
    HOST: str
    PORT: int
    USERNAME: str
    PASSWORD: str


COMPLETE_FLIGHTS_COLUMNS = CompleteFlightsColumns()
FLIGHT_STATUSES = FlightStatuses()
FLIGHT_TRAJECTORIES = FlightTrajectories()
FLIGHT_STATUS_COLUMN = "flight_status"

MONGODB = Mongodb(
    HOST=os.environ["MONGODB_HOST"],
    PORT=int(os.environ["MONGODB_PORT"]),
    USERNAME=os.environ["MONGODB_USERNAME"],
    PASSWORD=os.environ["MONGODB_PASSWORD"],
)
