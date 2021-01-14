#! /usr/bin/env python
import argparse
import logging

from dpp.helper_obs_info import find_pulsars_in_fov_main

logger = logging.getLogger(__name__)


def main(kwargs):
    find_pulsars_in_fov_main(kwargs)


if __name__ == "__main__":
    loglevels = dict(DEBUG=logging.DEBUG,
                     INFO=logging.INFO,
                     WARNING=logging.WARNING)
    parser = argparse.ArgumentParser(description="""
    Writes a file full of all the names and pointings of sources in the FoV.
    """)
    parser.add_argument('-o', '--obsid', type=str,
            help='The observation ID of the fits file to be searched')
    parser.add_argument("-b", "--begin", type=int, help="First GPS time to process [no default]")
    parser.add_argument("-e", "--end", type=int, help="Last GPS time to process [no default]")
    parser.add_argument("-a", "--all", action="store_true", default=False,
            help="Perform on entire observation span. Use instead of -b & -e")
    parser.add_argument("-f", "--fwhm", type=float, default=None,
            help="FWHM of the observation in degrees. If no value given the FWHM will be estimated.")
    parser.add_argument("-s", "--search_radius", type=float, default=0.02,
            help="The radius to search (create beams within) in degrees to account for ionosphere. Default: 0.02 degrees")
    parser.add_argument("-k", "--no_known_pulsars", action="store_true", default=False,
            help="Do no include known pulsars. Default: False")
    parser.add_argument("-c", "--no_search_cands", action="store_true", default=False,
            help="Do no include search_cands. Default: False")
    parser.add_argument("-L", "--loglvl", type=str, help="Logger verbosity level. Default: INFO",
                        default="INFO")
    args=parser.parse_args()

    # set up the logger for stand-alone execution
    logger.setLevel(loglevels[args.loglvl])
    ch = logging.StreamHandler()
    ch.setLevel(loglevels[args.loglvl])
    formatter = logging.Formatter('%(asctime)s  %(filename)s  %(name)s  %(lineno)-4d  %(levelname)-9s :: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.propagate = False
    kwargs = vars(args)
    main(kwargs)