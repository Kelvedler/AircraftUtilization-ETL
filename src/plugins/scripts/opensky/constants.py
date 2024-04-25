import os
from typing import NamedTuple


class StatesColumns(NamedTuple):
    ICAO24: str = "icao24"
    CALLSIGN: str = "callsign"
    ORIGIN_COUNTRY: str = "origin_country"
    TIME_POSITION: str = "time_position"
    LAST_CONTACT: str = "last_contact"
    LONGITUDE: str = "longitude"
    LATITUDE: str = "latitude"
    BARO_ALTITUDE: str = "baro_altitude"
    ON_GROUND: str = "on_ground"
    VELOCITY: str = "velocity"
    TRUE_TRACK: str = "true_track"
    VERTICAL_RATE: str = "vertical_rate"
    SENSORS: str = "sensors"
    GEO_ALTITUDE: str = "geo_altitude"
    SQUAWK: str = "squawk"
    SPI: str = "spi"
    POSITION_SOURCE: str = "position_source"


STATES_COLUMNS = StatesColumns()
OPENSKY_AUTH = os.getenv(key="OPENSKY_AUTH", default=None)
