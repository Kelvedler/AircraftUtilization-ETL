import logging
from typing import Union

import requests

from plugins.common.exceptions import InvalidCredentials, InvalidResponseError


class OpenSkyClient:
    def __init__(self, auth: Union[str, None]) -> None:
        if not isinstance(auth, str):
            raise InvalidCredentials("Opensky credentials are not valid")
        self._auth = auth
        self._api_url = "http://opensky-network.org/api"
        self._logger = logging.getLogger(__name__)

    def get_states(self) -> dict:
        states_path = "/states/all"
        url = self._api_url + states_path
        headers = {"Authorization": f"Basic {self._auth}"}

        self._logger.info("Fetching aircraft states")
        response = requests.get(url=url, headers=headers, timeout=5)

        rate_limit_remaining = response.headers.get("X-Rate-Limit-Remaining")
        print(rate_limit_remaining)
        self._logger.info(f"Rate limit remaining: {rate_limit_remaining}")

        if response.status_code == requests.codes.ok:
            return response.json()

        raise InvalidResponseError(
            f"Failed to fetch states, status code: {response.status_code}"
        )
