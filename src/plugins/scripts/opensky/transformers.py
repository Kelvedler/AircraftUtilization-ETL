from datetime import UTC, datetime, timedelta
import logging
from typing import NamedTuple

import pandas as pd
from plugins.common.constants import (
    ACTIVE_FLIGHTS_COLUMNS,
    META_COLUMNS,
    META_FILENAME,
    SOURCE_COLUMNS,
    SOURCE_FILENAME,
)
from plugins.common.exceptions import InvalidResponseError, InvalidSource
from plugins.common.s3 import S3BucketConnector
from plugins.scripts.complete_flights.constants import COMPLETE_FLIGHTS_COLUMNS_V2
from plugins.scripts.opensky.client import OpenSkyClient
from plugins.scripts.opensky.constants import STATES_COLUMNS


class SourceReports(NamedTuple):
    states: pd.DataFrame
    latest_source: pd.DataFrame


class ActiveFlightsETL:
    INACTIVITY_MAX_MINUTES = 20

    def __init__(
        self, s3_bucket: S3BucketConnector, opensky_client: OpenSkyClient
    ) -> None:
        self.s3_bucket = s3_bucket
        self.opensky_client = opensky_client
        self._logger = logging.getLogger(__name__)

    def _extract_opensky_states(self) -> pd.DataFrame:
        states_response = self.opensky_client.get_states()
        try:
            states = states_response["states"]
        except KeyError as e:
            raise InvalidResponseError(e)

        try:
            states = pd.DataFrame(data=states, columns=STATES_COLUMNS)
        except ValueError as e:
            raise InvalidResponseError(e)

        states = states[
            [
                STATES_COLUMNS.ICAO24,
                STATES_COLUMNS.LAST_CONTACT,
                STATES_COLUMNS.VELOCITY,
                STATES_COLUMNS.VERTICAL_RATE,
            ]
        ]

        return states

    def _extract_latest_source(self) -> pd.DataFrame:
        latest_source = self.s3_bucket.read_parquet(filename=SOURCE_FILENAME)
        if latest_source.empty:
            latest_source = pd.DataFrame(columns=tuple(SOURCE_COLUMNS))
        elif not pd.Series(tuple(SOURCE_COLUMNS)).isin(latest_source.columns).all():
            raise InvalidSource("Latest source dataframe lacks required columns")
        return latest_source

    def _active_flights_from_source(self, source: pd.DataFrame) -> pd.DataFrame:
        active_flights = source[list(ACTIVE_FLIGHTS_COLUMNS)]
        return active_flights

    def _update_flight_last_contact(self, source: pd.DataFrame) -> pd.DataFrame:
        valid_last_contact_mask = source[SOURCE_COLUMNS.LAST_CONTACT] != 0
        source.loc[valid_last_contact_mask, SOURCE_COLUMNS.FLIGHT_LAST_CONTACT] = (
            source[SOURCE_COLUMNS.LAST_CONTACT]
        )
        return source

    def _define_first_contact(self, source: pd.DataFrame) -> pd.DataFrame:
        source[[SOURCE_COLUMNS.IS_FIRST_CONTACT]] = source[
            [SOURCE_COLUMNS.IS_FIRST_CONTACT]
        ].replace(to_replace=[pd.NA, True], value=[True, False])
        return source

    def _remove_inactive(self, active_flights: pd.DataFrame) -> pd.DataFrame:
        inactivity_limit = round(
            (
                datetime.now(tz=UTC)
                - timedelta(minutes=self.__class__.INACTIVITY_MAX_MINUTES)
            ).timestamp()
        )
        active_mask = (
            active_flights[ACTIVE_FLIGHTS_COLUMNS.FLIGHT_LAST_CONTACT]
            > inactivity_limit
        )
        active = active_flights.loc[active_mask]
        return active

    def _extract(self) -> SourceReports:
        self._logger.info("Extracting Opensky states")
        states = self._extract_opensky_states()
        latest_source = self._extract_latest_source()
        return SourceReports(states=states, latest_source=latest_source)

    def _transform(self, source_reports: SourceReports) -> pd.DataFrame:
        self._logger.info("Performing Opensky states transformation")
        active_flights = self._active_flights_from_source(
            source=source_reports.latest_source
        )
        active_flights = self._remove_inactive(active_flights=active_flights)
        source = source_reports.states.merge(
            active_flights, how="outer", on=SOURCE_COLUMNS.ICAO24
        )
        source[
            [
                SOURCE_COLUMNS.LAST_CONTACT,
                SOURCE_COLUMNS.VELOCITY,
                SOURCE_COLUMNS.VERTICAL_RATE,
                SOURCE_COLUMNS.TAKEOFF_AT,
                SOURCE_COLUMNS.FLIGHT_LAST_CONTACT,
            ]
        ] = source[
            [
                SOURCE_COLUMNS.LAST_CONTACT,
                SOURCE_COLUMNS.VELOCITY,
                SOURCE_COLUMNS.VERTICAL_RATE,
                SOURCE_COLUMNS.TAKEOFF_AT,
                SOURCE_COLUMNS.FLIGHT_LAST_CONTACT,
            ]
        ].fillna(
            0
        )
        source = source.astype(
            {
                SOURCE_COLUMNS.LAST_CONTACT: pd.Int32Dtype(),
                SOURCE_COLUMNS.TAKEOFF_AT: pd.Int32Dtype(),
                SOURCE_COLUMNS.FLIGHT_LAST_CONTACT: pd.Int32Dtype(),
            }
        )
        source = self._define_first_contact(source=source)
        source = self._update_flight_last_contact(source=source)
        return source

    def _load(self, source: pd.DataFrame) -> None:
        self._logger.info("Uploading source report")
        self.s3_bucket.upload_to_parquet(df=source, filename=SOURCE_FILENAME)

    def etl(self) -> None:
        source_reports = self._extract()
        source = self._transform(source_reports=source_reports)
        self._load(source=source)


class MetadataETL:
    def __init__(
        self, s3_bucket: S3BucketConnector, opensky_client: OpenSkyClient
    ) -> None:
        self._s3_bucket = s3_bucket
        self._opensky_client = opensky_client
        self._logger = logging.getLogger(__name__)

    def _extract(self) -> pd.DataFrame:
        self._logger.info("Extracting metadata")
        source_metadata = self._opensky_client.get_aircraft_database()
        return source_metadata

    def _transform(self, source_metadata: pd.DataFrame) -> pd.DataFrame:
        self._logger.info("Performing Opensky metadata transformation")
        source_columns = META_COLUMNS
        columns = COMPLETE_FLIGHTS_COLUMNS_V2
        metadata = source_metadata[
            [
                META_COLUMNS.ICAO24,
                META_COLUMNS.REGISTRATION,
                META_COLUMNS.MODEL,
                META_COLUMNS.MANUFACTURER_ICAO,
                META_COLUMNS.OWNER,
                META_COLUMNS.OPERATOR,
                META_COLUMNS.BUILT,
            ]
        ]
        metadata.rename(
            columns={source_columns.MANUFACTURER_ICAO: columns.MANUFACTURER_ICAO},
            inplace=True,
        )
        return metadata

    def _load(self, metadata: pd.DataFrame) -> None:
        self._logger.info("Uploading metadata")
        self._s3_bucket.upload_to_parquet(df=metadata, filename=META_FILENAME)

    def etl(self) -> None:
        source_metadata = self._extract()
        metadata = self._transform(source_metadata=source_metadata)
        self._load(metadata=metadata)
