import logging
from typing import Union
import pandas as pd

import requests

from plugins.common.exceptions import InvalidCredentials, InvalidResponseError


class OpenSkyClient:
    def __init__(self, auth: Union[str, None]) -> None:
        if not isinstance(auth, str):
            raise InvalidCredentials("Opensky credentials are not valid")
        self._auth = auth
        base_url = "http://opensky-network.org"
        self._api_url = f"{base_url}/api"
        self._metadata_url = f"{base_url}/datasets/metadata"
        self._logger = logging.getLogger(__name__)

    def get_states(self) -> dict:
        url = f"{self._api_url}/states/all"
        headers = {"Authorization": f"Basic {self._auth}"}

        self._logger.info("Fetching aircraft states")
        response = requests.get(url=url, headers=headers, timeout=5)

        rate_limit_remaining = response.headers.get("X-Rate-Limit-Remaining")
        self._logger.info(f"Rate limit remaining: {rate_limit_remaining}")

        if response.status_code == requests.codes.ok:
            return response.json()

        raise InvalidResponseError(
            f"Failed to fetch states, status code: {response.status_code}"
        )

    def get_aircraft_database(self) -> pd.DataFrame:
        url = f"{self._metadata_url}/aircraftDatabase.csv"
        self._logger.info("Fetching aircraft database")
        df = pd.read_csv(url)
        return df
