from datetime import datetime
import logging
from typing import Optional, TypedDict

import pandas as pd
from plugins.common.constants import all_fields_present
from plugins.common.exceptions import InvalidCredentials
from plugins.scripts.complete_flights.constants import (
    COMPLETE_FLIGHTS_COLUMNS,
    Mongodb as MongoCredentials,
)
import pymongo
from pymongo.collection import Collection
from pymongo.errors import CollectionInvalid


class Flights(TypedDict):
    icao24: str
    landed_at: int
    duration_minutes: int
    registration: Optional[str]
    model: Optional[str]
    manufacturer_icao: Optional[str]
    owner: Optional[str]
    operator: Optional[str]
    built: Optional[datetime]


class AircraftUtilizationClient:
    def __init__(self, credentials: MongoCredentials) -> None:
        if not all_fields_present(credentials):
            raise InvalidCredentials("MongoDB credentials are not valid")
        client: pymongo.MongoClient = pymongo.MongoClient(
            host=credentials.HOST,
            port=credentials.PORT,
            username=credentials.USERNAME,
            password=credentials.PASSWORD,
        )
        self._db = client[credentials.DB]
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
        columns = COMPLETE_FLIGHTS_COLUMNS
        documents = [
            Flights(
                icao24=r[columns.ICAO24],
                landed_at=r[columns.LANDED_AT],
                duration_minutes=r[columns.FLIGHT_DURATION_MINUTES],
                registration=r[columns.REGISTRATION],
                model=r[columns.MODEL],
                manufacturer_icao=r[columns.MANUFACTURER_ICAO],
                owner=r[columns.OWNER],
                operator=r[columns.OPERATOR],
                built=r[columns.BUILT],
            )
            for r in df.to_dict("records")
        ]
        if documents:
            flights.insert_many(documents=documents)
        else:
            self._logger.info("Empty document. Nothing to write")
