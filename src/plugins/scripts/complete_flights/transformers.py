import logging
import math
from typing import NamedTuple

import numpy as np
import pandas as pd
from plugins.common.constants import SOURCE_COLUMNS
from plugins.common.s3 import S3BucketConnector
from plugins.scripts.complete_flights.constants import (
    COMPLETE_FLIGHTS_COLUMNS,
    FLIGHT_STATUSES,
    FLIGHT_STATUS_COLUMN,
    FLIGHT_TRAJECTORIES,
)
from plugins.scripts.complete_flights.db import AircraftUtilizationClient


class TransformedFlights(NamedTuple):
    active: pd.DataFrame
    complete: pd.DataFrame


class CompleteFlightsETL:
    def __init__(
        self,
        s3_bucket: S3BucketConnector,
        db_client: AircraftUtilizationClient,
        source_filename: str,
        meta_filename: str,
    ) -> None:
        self.s3_bucket = s3_bucket
        self.db_client = db_client
        self.source_filename = source_filename
        self.meta_filename = meta_filename
        self._logger = logging.getLogger(__name__)

    def _is_takeoff(self, row: pd.Series) -> bool:
        if (row[SOURCE_COLUMNS.IS_FIRST_CONTACT] == True) and (
            row[SOURCE_COLUMNS.VERTICAL_RATE] > 0
        ):
            return True
        return False

    def _is_landing(self, row: pd.Series) -> bool:
        if (
            row[SOURCE_COLUMNS.LAST_CONTACT] != 0
            and (
                row[SOURCE_COLUMNS.VERTICAL_RATE] == 0
                or pd.isna(row[SOURCE_COLUMNS.VERTICAL_RATE])
            )
            and (
                (
                    row[SOURCE_COLUMNS.FLIGHT_TRAJECTORY] == FLIGHT_TRAJECTORIES.DESCEND
                    and (row[SOURCE_COLUMNS.VELOCITY] < 10)
                )
                or (
                    row[SOURCE_COLUMNS.VELOCITY] == 0
                    or pd.isna(row[SOURCE_COLUMNS.VELOCITY])
                )
            )
        ):
            return True
        return False

    def _determine_flight_status(self, row: pd.Series) -> str:
        if self._is_takeoff(row=row):
            return FLIGHT_STATUSES.TAKEOFF
        elif self._is_landing(row=row):
            return FLIGHT_STATUSES.LANDING
        else:
            return FLIGHT_STATUSES.OTHER

    def _determine_flight_trajectory(self, row: pd.Series) -> str:
        if row[SOURCE_COLUMNS.VERTICAL_RATE] > 0:
            return FLIGHT_TRAJECTORIES.CLIMB
        elif (row[SOURCE_COLUMNS.VERTICAL_RATE] < 0) or (
            row[SOURCE_COLUMNS.FLIGHT_TRAJECTORY] == FLIGHT_TRAJECTORIES.DESCEND
        ):
            return FLIGHT_TRAJECTORIES.DESCEND
        else:
            return FLIGHT_TRAJECTORIES.OTHER

    def _extract(self) -> pd.DataFrame:
        self._logger.info("Extracting source report")
        source = self.s3_bucket.read_parquet(filename=self.source_filename)

        return source

    def _transform_active(self, active: pd.DataFrame) -> pd.DataFrame:
        active = active.copy()
        takeoff_mask = active[FLIGHT_STATUS_COLUMN] == FLIGHT_STATUSES.TAKEOFF
        active.loc[takeoff_mask, SOURCE_COLUMNS.TAKEOFF_AT] = active.loc[
            takeoff_mask, SOURCE_COLUMNS.FLIGHT_LAST_CONTACT
        ]

        active[SOURCE_COLUMNS.FLIGHT_TRAJECTORY] = active.apply(
            self._determine_flight_trajectory, axis=1
        )
        active.drop(
            [
                FLIGHT_STATUS_COLUMN,
            ],
            axis=1,
            inplace=True,
        )
        return active

    def _add_metadata(
        self, complete: pd.DataFrame, metadata: pd.DataFrame
    ) -> pd.DataFrame:
        self._logger.info("Adding metadata to complete flights")
        columns = COMPLETE_FLIGHTS_COLUMNS
        complete = complete.merge(right=metadata, on=columns.ICAO24, how="left")
        complete = complete.replace({np.nan: None})
        return complete

    def _transform_complete(
        self, complete: pd.DataFrame, metadata: pd.DataFrame
    ) -> pd.DataFrame:
        valid_mask = complete[SOURCE_COLUMNS.TAKEOFF_AT] != 0
        complete = complete.loc[
            valid_mask,
            [
                SOURCE_COLUMNS.ICAO24,
                SOURCE_COLUMNS.TAKEOFF_AT,
                SOURCE_COLUMNS.LAST_CONTACT,
            ],
        ]

        def get_flight_duration_minutes(row: pd.Series) -> int:
            return math.ceil(
                (row[SOURCE_COLUMNS.LAST_CONTACT] - row[SOURCE_COLUMNS.TAKEOFF_AT]) / 60
            )

        complete[COMPLETE_FLIGHTS_COLUMNS.FLIGHT_DURATION_MINUTES] = complete.apply(
            get_flight_duration_minutes, axis=1, result_type="reduce"
        )
        complete[COMPLETE_FLIGHTS_COLUMNS.LANDED_AT] = pd.to_datetime(
            complete[SOURCE_COLUMNS.LAST_CONTACT], unit="s", utc=True
        )
        complete.drop(
            [SOURCE_COLUMNS.TAKEOFF_AT, SOURCE_COLUMNS.LAST_CONTACT],
            axis=1,
            inplace=True,
        )
        complete = self._add_metadata(complete=complete, metadata=metadata)
        return complete

    def _transform(
        self, source: pd.DataFrame, metadata: pd.DataFrame
    ) -> TransformedFlights:
        self._logger.info("Performing report transformation")
        source[FLIGHT_STATUS_COLUMN] = source.apply(
            self._determine_flight_status, axis=1
        )

        active_mask = source[FLIGHT_STATUS_COLUMN] != FLIGHT_STATUSES.LANDING
        complete_mask = source[FLIGHT_STATUS_COLUMN] == FLIGHT_STATUSES.LANDING

        active = self._transform_active(active=source.loc[active_mask])
        complete = self._transform_complete(
            complete=source.loc[complete_mask], metadata=metadata
        )

        return TransformedFlights(active=active, complete=complete)

    def _load(self, flights: TransformedFlights) -> None:
        self._logger.info("Uploading reports")
        self.s3_bucket.upload_to_parquet(
            df=flights.active, filename=self.source_filename
        )
        self.db_client.write_flights(df=flights.complete)

    def etl(self) -> None:
        source = self._extract()
        if source.empty:
            self._logger.warning("Empty source report")
            return
        metadata = self.s3_bucket.read_parquet(filename=self.meta_filename)
        flights = self._transform(source=source, metadata=metadata)
        self._load(flights=flights)
