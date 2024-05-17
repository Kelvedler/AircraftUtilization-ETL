from typing import Optional, TypedDict

import numpy as np
import pandas as pd
from plugins.scripts.complete_flights.constants import COMPLETE_FLIGHTS_COLUMNS, MONGODB
import pymongo
from pymongo.collection import Collection
from pymongo.errors import CollectionInvalid


FLIGHTS_EXPIRATION_SECONDS = 60 * 60 * 24 * 365

client: pymongo.MongoClient = pymongo.MongoClient(
    host=MONGODB.HOST,
    port=MONGODB.PORT,
    username=MONGODB.USERNAME,
    password=MONGODB.PASSWORD,
)

old_db = client["aircraft-utilization"]

new_db = client["aircraft-utilization-main"]


class OldFlights(TypedDict):
    icao24: str
    landed_at: int
    duration_minutes: int


class NewFlights(TypedDict):
    icao24: str
    landed_at: int
    duration_minutes: int
    registration: Optional[str]
    model: Optional[str]
    manufacturer_icao: Optional[str]
    owner: Optional[str]
    operator: Optional[str]
    built: Optional[str]


def new_flights_collection() -> Collection[NewFlights]:
    try:
        new_flights = new_db.create_collection(
            name="flights",
            timeseries={
                "timeField": COMPLETE_FLIGHTS_COLUMNS.LANDED_AT,
                "metaField": COMPLETE_FLIGHTS_COLUMNS.ICAO24,
                "granularity": "hours",
            },
            expireAfterSeconds=FLIGHTS_EXPIRATION_SECONDS,
        )
    except CollectionInvalid:
        new_flights = new_db["flights"]
    return new_flights


def get_metadata() -> pd.DataFrame:
    url = "http://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
    df = pd.read_csv(url)
    return df


def flights_to_typed(df: pd.DataFrame) -> list[NewFlights]:
    flights = [
        NewFlights(
            icao24=r["icao24"],
            landed_at=r["landed_at"],
            duration_minutes=r["duration_minutes"],
            registration=r["registration"],
            model=r["model"],
            manufacturer_icao=r["manufacturericao"],
            owner=r["owner"],
            operator=r["operator"],
            built=r["built"],
        )
        for r in df.to_dict("records")
    ]
    return flights


def prepare_for_insert(flights_batch: list, metadata: pd.DataFrame) -> list[NewFlights]:
    df = pd.DataFrame(flights_batch)
    df = df.merge(metadata, on="icao24", how="left")
    df = df.replace({np.nan: None})
    return flights_to_typed(df)


def upload_to_new_db() -> None:
    metadata = get_metadata()
    old_flights = old_db["flights"]
    new_flights = new_flights_collection()
    flights_batch = []
    for old_flights in old_flights.find():
        flights_batch.append(old_flights)
        if len(flights_batch) >= 5000:
            flights_list = prepare_for_insert(
                flights_batch=flights_batch, metadata=metadata
            )
            new_flights.insert_many(documents=flights_list)
            flights_batch.clear()
    flights_list = prepare_for_insert(flights_batch=flights_batch, metadata=metadata)
    new_flights.insert_many(documents=flights_list)


if __name__ == "__main__":
    upload_to_new_db()
