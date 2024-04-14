import logging
from typing import TypedDict

import pandas as pd
import pymongo
from pymongo.collection import Collection
from pymongo.errors import CollectionInvalid

from plugins.scripts.complete_flights.constants import (
    COMPLETE_FLIGHTS_COLUMNS,
    Mongodb as MongoCredentials,
)


class Flights(TypedDict):
    icao24: str
    landed_at: int
    duration_minutes: int


class AircraftUtilizationClient:
    def __init__(self, credentials: MongoCredentials) -> None:
        client: pymongo.MongoClient = pymongo.MongoClient(
            host=credentials.HOST,
            port=credentials.PORT,
            username=credentials.USERNAME,
            password=credentials.PASSWORD,
        )
        self._db = client["aircraft-utilization"]
        self._logger = logging.getLogger(__name__)

    def _flights_collection(self) -> Collection[Flights]:
        FLIGHTS_EXPIRATION_SECONDS = 60 * 60 * 24 * 365
        try:
            flights = self._db.create_collection(
                name="flights",
                timeseries={
                    "timeField": COMPLETE_FLIGHTS_COLUMNS.LANDED_AT,
                    "metaField": COMPLETE_FLIGHTS_COLUMNS.ICAO24,
                    "granularity": "hours",
                },
                expireAfterSeconds=FLIGHTS_EXPIRATION_SECONDS,
            )
        except CollectionInvalid as e:
            self._logger.debug(e)
            flights = self._db["flights"]
        return flights

    def write_flights(self, df: pd.DataFrame) -> None:
        flights = self._flights_collection()
        documents = [
            Flights(
                icao24=r[COMPLETE_FLIGHTS_COLUMNS.ICAO24],
                landed_at=r[COMPLETE_FLIGHTS_COLUMNS.LANDED_AT],
                duration_minutes=r[COMPLETE_FLIGHTS_COLUMNS.FLIGHT_DURATION_MINUTES],
            )
            for r in df.to_dict("records")
        ]
        if documents:
            flights.insert_many(documents=documents)
        else:
            self._logger.info("Empty document. Nothing to write")
