#!/usr/bin/env python
"""
Handles the authentication for access to the SMART database
"""

import requests
import os
from urllib.parse import urljoin


class TokenAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Token {}".format(self.token)
        return r


class Session:
    """A thin wrapper around a requests.session instance for
    slightly easier access to the SMART database.
    """

    def __init__(self, token=None, base_url=None):
        """
        Creates a session to the SMART database

        If the token is supplied, use it. Otherwise, try to get it from
        the SMART_TOKEN environment variable.

        If a base URL is supplied, use that as the SMART database URL.
        Otherwise, try to get it from the SMART_BASE_URL environment
        variable.
        """
        self.session = None

        if token is None:
            token = os.environ.get("SMART_TOKEN")
        self.__token = token  # this is "hidden" to the outside world
        # To access this attribute outside of the object itself, one must
        # use a special syntax: <instance>._Session__token

        if base_url is None:
            base_url = os.environ.get("SMART_BASE_URL")
        self.base_url = base_url

        self.session = requests.session()
        self.session.auth = TokenAuth(self.__token)

    def __enter__(self):
        """Enter context by providing this object"""
        return self  # return the full object since we have wrapped utility methods

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context by closing database connection"""
        self.close()
        if exc_val:
            raise

    def __del__(self):
        self.close()

    def post(self, view_url, **kwargs):
        """
        A wrapper for the underlying session.post() call

        The URL used for the session.post() call is the base_url with
        the view_url appended to it.
        """
        url = urljoin(self.base_url, view_url)
        return self.session.post(url, **kwargs)

    def patch(self, view_url, **kwargs):
        """
        A wrapper for the underlying session.patch() call

        The URL used for the session.patch() call is the base_url with
        the view_url appended to it.
        """
        url = urljoin(self.base_url, view_url)
        return self.session.patch(url, **kwargs)

    def put(self, view_url, **kwargs):
        """
        A wrapper for the underlying session.put() call

        The URL used for the session.put() call is the base_url with
        the view_url appended to it.
        """
        url = urljoin(self.base_url, view_url)
        return self.session.put(url, **kwargs)

    def get(self, view_url, **kwargs):
        """
        A wrapper for the underlying session.get() call

        The URL used for the session.get() call is the base_url with
        the view_url appended to it.
        """
        url = urljoin(self.base_url, view_url)
        return self.session.get(url, **kwargs)

    def delete(self, view_url, **kwargs):
        """
        A wrapper for the underlying session.delete() call

        The URL used for the session.delete() call is the base_url with
        the view_url appended to it.
        """
        url = urljoin(self.base_url, view_url)
        return self.session.delete(url, **kwargs)

    def close(self):
        """Close the database session"""
        self.session.close()


def add_standard_args(parser):
    """
    Adds "token" and "base_url" arguments to the supplied parser
    """
    parser.add_argument(
        "--token",
        help="Your authentication token for access to the SMART database. If not supplied, the value of the environment variable SMART_TOKEN is used.",
    )
    parser.add_argument(
        "--base_url",
        help="The base URL for the SMART database. If not supplied, the value of the environment variable SMART_BASE_URL is used.",
    )
