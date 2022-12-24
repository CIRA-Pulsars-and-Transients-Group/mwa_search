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
        r.headers['Authorization'] = "Token {}".format(self.token)
        return r

class Session():
    def __init__(self, token=None, base_url=None):
        '''
        Creates a session to the SMART database

        If the token is supplied, use it. Otherwise, try to get it from
        the SMART_TOKEN environment variable.

        If a base URL is supplied, use that as the SMART database URL.
        Otherwise, try to get it from the SMART_BASE_URL environment
        variable.
        '''
        self.session = requests.session()
        if token is None:
            token = os.environ.get("SMART_TOKEN")
        self.session.auth = TokenAuth(token)

        if base_url is None:
            base_url = os.environ.get("SMART_BASE_URL")
        self.base_url = base_url

    def post(self, view_url, **kwargs):
        '''
        A wrapper for the underlying session.post() call

        The URL used for the session.post() call is the base_url with
        the view_url appended to it.
        '''
        url = urljoin(self.base_url, view_url)
        return self.session.post(url, **kwargs)

    def get(self, view_url, **kwargs):
        '''
        A wrapper for the underlying session.get() call

        The URL used for the session.get() call is the base_url with
        the view_url appended to it.
        '''
        url = urljoin(self.base_url, view_url)
        return self.session.get(url, **kwargs)

    def __del__(self):
        self.session.close()

def add_standard_args(parser):
    '''
    Adds "token" and "base_url" arguments to the supplied parser
    '''
    parser.add_argument('--token', help='Your authentication token for access to the SMART database. If not supplied, the value of the environment variable SMART_TOKEN is used.')
    parser.add_argument('--base_url', help='The base URL for the SMART database. If not supplied, the value of the environment variable SMART_BASE_URL is used.')
