import logging

import requests

from plugins.common.exceptions import InvalidResponseError


class OpenSkyClient:
    def __init__(self) -> None:
        self._api_url = "http://opensky-network.org/api"
        self._logger = logging.getLogger(__name__)

    def get_states(self) -> dict:
        states_path = "/states/all"
        url = self._api_url + states_path

        self._logger.info("Fetching aircraft states")
        response = requests.get(url=url, timeout=5)
        if response.status_code == requests.codes.ok:
            return response.json()

        raise InvalidResponseError(
            f"Failed to fetch states, status code: {response.status_code}"
        )
