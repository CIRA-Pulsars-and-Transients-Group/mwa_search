from mwalib import MetafitsContext
import logging
import os
import sys
import glob
import subprocess
import numpy as np

logger = logging.getLogger(__name__)


def load_metafits_context(data_dir, obsid, metafits_file):
    """
    Load the context from the metafits files of an observation. If the metafits file does not exist, download it from
    http://ws.mwatelescope.org/metadata

    Parameters
    ----------
    data_dir : `str`
        The directory to the metafits file.
    obsid : `int`
        The MWA Observation ID.
    metafits_file : `str`
        The metafits file name.

    Returns
    -------
    metafits_context : MetafitsContext
        The MetafitsContext object containing the information on the observation
    """

    metafits_filepath = os.path.join(data_dir, metafits_file)
    if not os.path.exists(metafits_filepath):
        logger.warning(f"{metafits_filepath} does not exists")
        logger.warning("Will download it from the archive. This can take a "
                      "while so please do not ctrl-C.")
        logger.warning("At the moment, even through the downloaded file is "
                       "labelled as a ppd file this is not true.")
        logger.warning("This is hopefully a temporary measure.")

        get_metafits = f"wget http://ws.mwatelescope.org/metadata/fits?obs_id={obsid} -O {metafits_filepath}"
        try:
            subprocess.call(get_metafits, shell=True)
        except:
            logger.error(f"Couldn't download {metafits_filepath}. Aborting.")
            sys.exit(0)

    return MetafitsContext(metafits_filepath)


def obs_max_min(data_dir, lochan):
    if not os.path.exists(data_dir):
        logger.error(f"Path to the combined voltages {data_dir} does not exists. Exiting...")
        sys.exit(0)
    all_data = glob.glob(f"{data_dir}/*ch{lochan}*")
    if len(all_data) == 0:
        logger.error(f"Combined voltages in {data_dir} does not exists. Exiting...")
        sys.exit(0)
    obs_times = [int(o.split("_")[-2]) for o in all_data]
    return np.min(obs_times), np.max(obs_times)


def mdir(path, description, gid=34858):
    """Simple function to create directories with the correct group permissions (771).

    Parameters
    ----------
    path : `str`
        The path of the directory we want to create.
    description : `str`
        The description of the directory to be printed to logger.
    gid : `int`, optional
        The group ID to apply to the directory. |br| Default: 34858 which the mwavcs.
    """
    try:
        # TODO: this doesn't carry over permissions correctly to "combined" for some reason...
        os.makedirs(path)
        # we leave the uid unchanged but change gid to mwavcs
        os.chown(path, -1, gid)
        os.chmod(path, 0o771)
        os.system("chmod -R g+s {0}".format(path))
    except:
        if (os.path.exists(path)):
            logger.info("{0} Directory Already Exists\n".format(description))
        else:
            logger.error("{0} Could not make new directories\n".format(description))
            sys.exit(0)

