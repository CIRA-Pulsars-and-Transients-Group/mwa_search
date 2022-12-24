#!/usr/bin/env python
"""
Poll the SMART database for the contents of a specified table
"""

import argparse
import requests
import smart_database_auth as smart

def get_table(table, token=None, base_url=None):

    # Open a session to the SMART database
    smart_session = smart.Session(token=token, base_url=base_url)

    # Send a GET request to the appropriate view
    view = f'{table}/'
    rows = smart_session.get(view)

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

    args = parser.parse_args()

    rows = get_table(
            args.table,
            token=args.token,
            base_url=args.base_url,
            )

    if args.format == 'csv':
        for row in rows.json():
            print(args.delimiter.join([str(row[k]) for k in row]))
    elif args.format == 'json':
        print(rows.json())
    else:
        raise ValueError(f"'{args.format}' format not supported")

if __name__ == '__main__':
    main()
