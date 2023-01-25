#!/usr/bin/env python
"""
Add rows to a specified table
"""

import argparse
import json
import requests
from smart_database import auth as smart

def add_to_table(table, token=None, base_url=None, **kwargs):

    # Open a session to the SMART database
    smart_session = smart.Session(token=token, base_url=base_url)

    # Send a GET request to the appropriate view
    view = f'api/{table}/'
    response = smart_session.post(view, **kwargs)

    return response

def main():
    # Parse command line options
    parser = argparse.ArgumentParser(description='Add rows to a specified table')

    # Add the standard database-authentication options
    smart.add_standard_args(parser)

    # Add other arguments specific to this script
    parser.add_argument('table', help='The table to be polled.')
    parser.add_argument('json_file', type=argparse.FileType('r'), help='File containing json object(s) to be added to the table')
    parser.add_argument('--one_by_one', action='store_true', help='If the file contains a list of JSON objects, upload JSON objects one at a time, instead of uploading them as a single multi-component object.')

    args = parser.parse_args()

    # Get the json object from the file
    json_loaded = json.load(args.json_file)

    def upload_single_object(args, json_object):
        try:
            response = add_to_table(
                args.table,
                token=args.token,
                base_url=args.base_url,
                json=json_object,
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)

    if isinstance(json_loaded, list) and args.one_by_one:
        for json_object in json_loaded:
            upload_single_object(args, json_object)
    else:
        upload_single_object(args, json_loaded)


if __name__ == '__main__':
    main()
