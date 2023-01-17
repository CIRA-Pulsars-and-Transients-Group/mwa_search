#!/usr/bin/env python3

import requests
import json
from . import auth as smart


def get_table(
    table: str,
    token: str = None,
    base_url: str = None,
    **kwargs,
) -> requests.Response:
    """Return a full database table

    :param table: Database table name
    :type table: str
    :param token: Database access token, defaults to None
    :type token: str, optional
    :param base_url: Database access base URL, defaults to None
    :type base_url: str, optional
    :return: Response from the database in the natural requests.Response format
    :rtype: requests.Response
    """

    # Open a session to the SMART database
    with smart.Session(token=token, base_url=base_url) as smart_session:
        # Send a GET request to the appropriate view
        view = f"api/{table}/"
        response = smart_session.get(view, **kwargs)
        response.raise_for_status()

    return response
