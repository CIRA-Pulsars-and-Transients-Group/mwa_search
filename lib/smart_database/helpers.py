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


def get_beams_in_ra_range(
    ra1: float,
    ra2: float,
    observation_id: str = None,
    token: str = None,
    base_url: str = None,
) -> requests.Response:
    """Return all Beam objects that sit between the provided Right Ascension range.

    Note: For coordinates where the user wants to wrap at R.A. = 360 degrees (24hr),
    ``ra2`` can be smaller than ``ra1`` in which case the API call will return
    the union of ``ra1`` <= R.A. < 360 and 0 <= R.A. <= ``ra2``.

    :param ra1: R.A. 'lower' limit
    :type ra1: float
    :param ra2: R.A. 'upper' limit
    :type ra2: float
    :param observation_id: Observation within which to restrict the search, defaults to None
    :type observation_id: str, optional
    :param token: Authentication token, defaults to None
    :type token: str, optional
    :param base_url: Database access URL, defaults to None
    :type base_url: str, optional
    :return: A requests.Response object containing the returned Beams
    :rtype: requests.Response
    """
    # Prepare query payload
    payload = {"ra_between": f"{ra1},{ra2}"}
    if observation_id:
        payload.update({"observation": f"{observation_id}"})

    # Open a session to the SMART database
    with smart.Session(token=token, base_url=base_url) as smart_session:
        # Send a GET request to the appropriate view
        view = f"api/beam/"
        response = smart_session.get(view, params=payload)
        response.raise_for_status()

    return response


def get_beams_in_dec_range(
    dec1: float,
    dec2: float,
    observation_id: str = None,
    token: str = None,
    base_url: str = None,
) -> requests.Response:
    """Return all Beam objects that sit between the provided Declination range.

    Note: For coordinates where both Declinations are <0, the "most negative"
    Declination limit should be assigned to ``dec1``

    :param dec1: Declination 'lower' limit (i.e., closest to -90 degrees)
    :type dec1: float
    :param dec2: Declination 'upper' limit (i.e., closest to +90 degrees)
    :type dec2: float
    :param observation_id: Observation within which to restrict the search, defaults to None
    :type observation_id: str, optional
    :param token: Authentication token, defaults to None
    :type token: str, optional
    :param base_url: Database access URL, defaults to None
    :type base_url: str, optional
    :return: A requests.Response object containing the returned Beams
    :rtype: requests.Response
    """

    # Prepare query payload
    payload = {"dec_between": f"{dec1},{dec2}"}
    if observation_id:
        payload.update({"observation": f"{observation_id}"})

    # Open a session to the SMART database
    with smart.Session(token=token, base_url=base_url) as smart_session:
        # Send a GET request to the appropriate view
        view = f"api/beam/"
        response = smart_session.get(view, params=payload)
        response.raise_for_status()

    return response


def get_beams_in_radius(
    ra: float,
    dec: float,
    radius: float,
    observation_id: str = None,
    token: str = None,
    base_url: str = None,
) -> requests.Response:
    """Return all Beam objects that reside within the sky area traced by a
    circle of the provided ``radius``, centred on the given ``ra`` and ``dec``.

    :param ra: Right Ascension of the centre of the cone
    :type ra: float
    :param dec: Declination of the centre of the cone
    :type dec: float
    :param radius: Radius, in degrees, to search out to from the centre point
    :type radius: float
    :param observation_id: Observation within which to restrict the search, defaults to None
    :type observation_id: str, optional
    :param token: Authentication token, defaults to None
    :type token: str, optional
    :param base_url: Database access URL, defaults to None
    :type base_url: str, optional
    :return: A requests.Response object containing the returned Beams
    :rtype: requests.Response
    """
    # Prepare query payload
    payload = {"cone_search": f"{ra},{dec},{radius}"}
    if observation_id:
        payload.update({"observation": f"{observation_id}"})

    # Open a session to the SMART database
    with smart.Session(token=token, base_url=base_url) as smart_session:
        # Send a GET request to the appropriate view
        view = f"api/beam/"
        response = smart_session.get(view, params=payload)
        response.raise_for_status()

    return response
