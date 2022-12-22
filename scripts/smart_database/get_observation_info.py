#!/usr/bin/env python
"""
Poll the SMART database for info about a given observation
"""

import argparse
import requests
import smart_database_auth as smart

def get_observation_info(obsid, token=None, base_url=None):

    # Open a session to the SMART database
    smart_session = smart.Session(token=token, base_url=base_url)

    # Send a GET request to the appropriate view
    view = f'observation_info/{obsid}/'
    observation_info = smart_session.get(view).json()

    return observation_info

def main():
    # Parse command line options
    parser = argparse.ArgumentParser(description='Poll the SMART database for info about a given observation')

    # Add the standard database-authentication options
    smart.add_standard_args(parser)

    # Add other arguments specific to this script
    parser.add_argument('obsid', help='The observation ID')
    parser.add_argument('--pretty', action='store_true', help="Print the values to the screen in a human-readable format")

    args = parser.parse_args()

    observation_info = get_observation_info(
            args.obsid,
            token=args.token,
            base_url=args.base_url,
            )

    if args.pretty:
        for field, value in observation_info.items():
            print(f"{field:20s} {value}")
    else:
        print(observation_info)

if __name__ == '__main__':
    main()
