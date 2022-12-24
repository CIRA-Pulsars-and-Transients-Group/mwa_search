#! /usr/bin/env python
"""
Add rows to the search_parameters table
"""

from argparse import ArgumentParser
import concurrent.futures
import csv
import logging
import os
from pathlib import Path
import sys
from urllib.parse import urljoin

import requests

MAX_THREADS = 10
TABLE_TO_PATH = {
    'search_parameters': './search_parameters_create/',
    'ml_parameters': './ml_parameters_create/',
    'ml_candidates': './ml_candidates_create/',
    'candidates': './candidates_create/',
    'ratings': './ratings_create/',
    'beams': './beams_create/',
    'pulsar': './pulsar_create/',
    'supercomputers': './supercomputers_create/',
    'users': './users_create/',
    'observation_setting': './observation_setting_create/',
    'followups': './followups_create/',
    'calibrator': './calibrator_create/',
    'detection': './detection_create/',
    'detection_file': './detection_file_create/'
}
DEFAULT_TARGET = 'http://localhost:8000'
TOKEN_ENV_VAR_NAME = "SMART_TOKEN"
BASE_URL_ENV_VAR_NAME = "SMART_BASE_URL"

logging.basicConfig(format="%(levelname)s %(message)s")
log = logging.getLogger()
log.setLevel(logging.INFO)




class TokenAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = "Token {}".format(self.token)
        return r


def get_upload_url(*, base_url, table_name):
    return urljoin(base_url, TABLE_TO_PATH[table_name])


class RowUploader:
    def __init__(self, *, session, table, directory, base_url):
        """
        parameters:
        -----------
        session : `requests.session`
            A connection session

        table : str
            The name of the table that we are uploading to

        directory : str
            The base directory that we prepend to all filenames

        """
        self.session = session
        self.table = table
        self.directory = Path(directory)
        self.url = get_upload_url(base_url=base_url, table_name=table)

    def __call__(self, row):
        """
        parameters:
        -----------
        row : dict
            The data that is being uploaded
        """
        data = {}
        for key, val in row.items():
            data[key.lower().strip()] = val.strip()

        if self.table == 'candidates':
            # fidget the data/files to include the files for upload
            with (
                open(self.directory.joinpath(data['pfd_path']), 'rb') as pfd,
                open(self.directory.joinpath(data['png_path']), 'rb') as png,
            ):
                files = {'pfd': pfd, 'png': png}
                del data['pfd_path'], data['png_path']
                r = self.session.post(self.url, data=data, files=files)
        elif self.table == 'calibrator':
            # fidget the data/files to include the files for upload
            with open(self.directory.joinpath(data['cal_file']), 'rb') as cal:
                files = {'cal_file': cal}
                del data['cal_file']
                r = self.session.post(self.url, data=data, files=files)
        elif self.table == 'detection_file':
            # fidget the data/files to include the files for upload
            with open(self.directory.joinpath(data['path']), 'rb') as df:
                files = {'path': df}
                del data['path']
                r = self.session.post(self.url, data=data, files=files)
        else:
            r = self.session.post(self.url, data=data)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            log.error("Response: %s", r.text)
            raise
        else:
            log.debug("Response: %s", r.text)
        return "{0} : {1}".format(data, r.status_code)


def upload(
    csvfile, *, table='sp', directory=None, token, base_url, max_threads
):
    """
    Upload a csv table to the SMART survey database via the web front-end.
    The table name is one of:
    search_parameters
    ml_parameters
    ml_candidates
    candidates
    ratings
    pulsar
    super_computers
    observation_setting
    beams
    followups
    calibrator
    detection
    detection_file


    dir is optional and represents the directory in which files are stored.
    dir will be prepended to all filenames listed in a csv file when trying
    to locate the file.

    Parameters
    ==========
    csvfile : str
      path to csv file for upload
    table : str
      destination table name
    directory : str
      Base directory for filenames listed within a csv file
    """
    # set up a session with the given auth
    session = requests.session()
    session.auth = TokenAuth(token)
    row_uploader = RowUploader(
        session=session, table=table, directory=directory, base_url=base_url,
    )

    log.info("Sending rows from {0} to {1}".format(csvfile, table))
    with open(csvfile) as f:
        reader = csv.DictReader(f, delimiter=',')
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_threads
        ) as executor:
            for result in executor.map(row_uploader, reader):
                log.info(result)
    log.info("Completed")
    session.close()
    return


def main():
    parser = ArgumentParser(description="Upload a csv table to the SMART survey database")
    parser.add_argument("csv",
                        help='CSV format table for upload')
    g1 = parser.add_argument_group("Table Selection (destination)")
    g1.add_argument('--sp', dest='table',
                    action='store_const', const='search_parameters',
                    help="SearchParameters")
    g1.add_argument('--mlp', dest='table',
                    action='store_const', const='ml_parameters',
                    help='MlParameters')
    g1.add_argument('--mlc', dest='table',
                    action='store_const', const='ml_candidates',
                    help='MlCandidates')
    g1.add_argument('--can', dest='table',
                    action='store_const', const='candidates',
                    help="Candidates [use option --dir]")
    g1.add_argument('--rat', dest='table',
                    action='store_const', const='ratings',
                    help='Ratings')
    g1.add_argument('--psr', dest='table',
                    action='store_const', const='pulsar',
                    help="Pulsar")
    g1.add_argument('--sc', dest='table',
                    action='store_const', const='supercomputers',
                    help="SuperComputers")
    g1.add_argument('--obs', dest='table',
                    action='store_const', const='observation_setting',
                    help="ObservationSetting")
    g1.add_argument('--beam', dest='table',
                    action='store_const', const='beams',
                    help="Beams")
    g1.add_argument('--fup', dest='table',
                    action='store_const', const='followups',
                    help="Followups")
    g1.add_argument('--cal', dest='table',
                    action='store_const', const='calibrator',
                    help="Calibrator [use option --dir]")
    g1.add_argument('--det', dest='table',
                    action='store_const', const='detection',
                    help="Detection")
    g1.add_argument('--df', dest='table',
                    action='store_const', const='detection_file',
                    help="DetectionFile [use option --dir]")

    g2 = parser.add_argument_group("Extra options")
    g2.add_argument('--dir', dest='dir', default='',
                    help='Base directory in which files are stored.')
    g2.add_argument('--nthreads', dest='max_threads', default=MAX_THREADS, type=int,
                    help='Limit maximum number of threads. Default: {0:d}'.format(MAX_THREADS))

    options = parser.parse_args()

    # ensure that file exists for reading
    if not os.path.exists(options.csv):
        log.error("Error: {0} not found".format(options.csv))
        return 1

    if not options.table:
        log.error("Must specify a table selection")
        return 1

    token = os.environ.get(TOKEN_ENV_VAR_NAME)
    if token is None:
        log.error("Token not found, set {}".format(TOKEN_ENV_VAR_NAME))
        return 1

    base_url = os.environ.get(BASE_URL_ENV_VAR_NAME) or DEFAULT_TARGET
    log.info("Base URL %s", base_url)

    return upload(
        options.csv, table=options.table, directory=options.dir, token=token,
        base_url=base_url, max_threads=options.max_threads
    )


if __name__ == '__main__':
    sys.exit(main())
