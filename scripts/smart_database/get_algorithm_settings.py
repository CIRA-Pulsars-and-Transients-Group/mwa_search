#!/usr/bin/env python
"""
Poll the SMART database for the settings used for a given survey chapter and algorithm
"""

import argparse
import requests
import smart_database_auth as smart

def get_algorithm_settings(survey_chapter, algorithm, token=None, base_url=None):

    # Open a session to the SMART database
    smart_session = smart.Session(token=token, base_url=base_url)

    # Send a GET request to the appropriate view
    view = 'algorithm_setting/'
    params = {'survey_chapter': survey_chapter,
            'algorithm': algorithm,
            }
    algorithm_settings = smart_session.get(view, params=params).json()

    return algorithm_settings

def main():
    # Parse command line options
    parser = argparse.ArgumentParser(description='Poll the SMART database for the settings used for a given survey chapter and algorithm')
    parser.add_argument('--token', help='Your authentication token for access to the SMART database. If not supplied, the value of the environment variable SMART_TOKEN is used.')
    parser.add_argument('--base_url', help='The base URL for the SMART database. If not supplied, the value of the environment variable SMART_BASE_URL is used.')
    parser.add_argument('survey_chapter', help='Filter the results by the supplied survey chapter.')
    parser.add_argument('algorithm', help='Filter the results by the supplied algorithm')
    parser.add_argument('--pretty', action='store_true', help="Print the values to the screen in a human-readable format")

    args = parser.parse_args()

    algorithm_settings = get_algorithm_settings(
            args.survey_chapter,
            args.algorithm,
            token=args.token,
            base_url=args.base_url,
            )

    if args.pretty:
        for a in algorithm_settings:
            print(a['algorithm_parameter__name'], a['value'])
    else:
        print(algorithm_settings)

if __name__ == '__main__':
    main()
