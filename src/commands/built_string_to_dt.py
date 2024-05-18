from datetime import datetime
from typing import Optional, TypedDict
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

old_db = client["aircraft-utilization-main"]

new_db = client["aircraft-utilization-main-1"]


class OldFlights(TypedDict):
    icao24: str
    landed_at: int
    duration_minutes: int
    registration: Optional[str]
    model: Optional[str]
    manufacturer_icao: Optional[str]
    owner: Optional[str]
    operator: Optional[str]
    built: Optional[str]


class NewFlights(TypedDict):
    icao24: str
    landed_at: int
    duration_minutes: int
    registration: Optional[str]
    model: Optional[str]
    manufacturer_icao: Optional[str]
    owner: Optional[str]
    operator: Optional[str]
    built: Optional[datetime]


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


def upload_to_new_db() -> None:
    old_flights = old_db["flights"]
    new_flights = new_flights_collection()
    flights_batch = []
    for old_flight in old_flights.find():
        built_str = old_flight["built"]
        if built_str:
            try:
                built = datetime.strptime(built_str, "%Y-%m-%d")
            except ValueError:
                built = None
        else:
            built = None
        new_flight = NewFlights(
            icao24=old_flight["icao24"],
            landed_at=old_flight["landed_at"],
            duration_minutes=old_flight["duration_minutes"],
            registration=old_flight["registration"],
            model=old_flight["model"],
            manufacturer_icao=old_flight["manufacturer_icao"],
            owner=old_flight["owner"],
            operator=old_flight["operator"],
            built=built,
        )
        flights_batch.append(new_flight)
        if len(flights_batch) >= 5000:
            new_flights.insert_many(documents=flights_batch)
            flights_batch.clear()
    new_flights.insert_many(documents=flights_batch)


if __name__ == "__main__":
    upload_to_new_db()
