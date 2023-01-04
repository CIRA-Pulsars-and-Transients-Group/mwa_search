#!/usr/bin/env python
"""
Poll the SMART database for the contents of a specified table
"""

import argparse
import json
import requests
from smart_database import auth as smart

def get_table(table, token=None, base_url=None, **kwargs):

    # Open a session to the SMART database
    smart_session = smart.Session(token=token, base_url=base_url)

    # Send a GET request to the appropriate view
    view = f'api/{table}/'
    rows = smart_session.get(view, **kwargs)

    return rows

def main():
    # Parse command line options
    parser = argparse.ArgumentParser(description='Poll the SMART database for the contents of a specified table')

    # Add the standard database-authentication options
    smart.add_standard_args(parser)

    # Add other arguments specific to this script
    parser.add_argument('table', help='The table to be polled.')
    parser.add_argument('--format', default='csv', help="Output format: json, csv (default)")
    parser.add_argument('--delimiter', default=',', help="When --format=csv, use this delimiter to separate columns")
    parser.add_argument('--param', action='append', help="Supply an extra parameter to the GET request (can be used multiple times)")

    args = parser.parse_args()

    # Convert the params argument into a dictionary
    params_dict = {key_value[0]: key_value[1] for key_value in [param.split("=") for param in args.param]} if args.param else None

    try:
        rows = get_table(
            args.table,
            token=args.token,
            base_url=args.base_url,
            params=params_dict,
        )
        rows.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
        return

    if args.format == 'csv':
        for row in rows.json():
            print(args.delimiter.join([str(row[k]) for k in row]))
    elif args.format == 'json':
        print(json.dumps(rows.json(), indent=4))
    else:
        raise ValueError(f"'{args.format}' format not supported")

if __name__ == '__main__':
    main()
