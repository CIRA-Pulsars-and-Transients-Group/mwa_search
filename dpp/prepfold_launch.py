#!/usr/bin/env python3

def common_kwargs(pipe, bin_count):
    """Creates a prepfold-friendly dictionary of common arguments to pass to prepfold"""
    name = f"{pipe['obs']['id']}_b{bin_count}_{pipe["source"]["name"]}"
    prep_kwargs = {}
    prep_kwargs["-mask"] = pipe["run_ops"]["mask"]
    prep_kwargs["-o"] = name
    prep_kwargs["-n"] = bin_count
    prep_kwargs["-start"] = pipe["source"]["enter_frac"]
    prep_kwargs["-end"] = pipe["source"]["exit_frac"]
    prep_kwargs["-runavg"] = ""
    prep_kwargs["-noxwin"] = ""
    prep_kwargs["-noclip"] = ""
    prep_kwargs["-nsub"] = 256
    prep_kwargs["-pstep"] = 1
    prep_kwargs["-pdstep"] = 2
    prep_kwargs["-dmstep"] = 1
    prep_kwargs["-npart"] = 120
    prep_kwargs["-npfact"] = 1
    prep_kwargs["-ndmfact"] = 1
    if nbins >= 300:
        prep_kwargs["-nopdsearch"] = ""
    if bin_count == 50:
        prep_kwargs["-npfact"] = 2
        prep_kwargs["-ndmfact"] = 1
    if bin_count == 64:
        prep_kwargs["-npfact"] = 4
        prep_kwargs["-ndmfact"] = 3
    if pipe["source"]["ATNF_P"] < 0.005  # period less than 50ms
        prep_kwargs["-npfact"] = 4
        prep_kwargs["-ndmfact"] = 3
        prep_kwargs["-dmstep"] = 3
        prep_kwargs["-npart"] = 40
    if pipe["source"]["binary"]:
        prep_kwargs["-dm"] = None
        prep_kwargs["-p"] = None
    else:
        prep_kwargs["-dm"] = pipe["source"]["ATNF_DM"]
        prep_kwargs["-p"] = pipe["source"]["ATNF_P"]
    if pipe["source"]["my_DM"]:
        prep_kwargs["-dm"] = pipe["source"]["my_DM"]
    if pipe["source"]["my_P"]:
        prep_kwargs["-dm"] = pipe["source"]["my_P"]

    return prep_kwargs


def prepfold_time_alloc(pipe, prepfold_kwargs):
    """
    Estimates the jobtime for prepfold jobs based on inputs

    Returns:
    --------
    time: string
        The allocated time for the fold job as a string that can be passed to the slurm batch handler
    """
    nopsearch = False
    nopdsearch = False
    nodmsearch = False
    nosearch = False
    if "-nopsearch" in prepfold_kwargs:
        nopsearch = True
    if "-nodmsearch" in prepfold_kwargs:
        nodmsearch = True
    if "-nopdsearch" in prepfold_kwargs:
        nopdsearch = True
    if "-nosearch" in prepfold_kwargs:
        nosearch = True
    npfact = prepfold_dict["-npfact"]
    ndmfact = prepfold_dict["-ndmfact"]
    nbins = prepfold_dict["-n"]
    duration = (pipe["obs"]["end"] - pipe["obs"]["beg"]) * \
                (pipe["source"]["exit_frac"] - pipe["source"]["enter_frac"])

    time = 600
    time += nbins
    time += duration

    if not nosearch:
        ptime = 1
        pdtime = 1
        dmtime = 1
        if not nopsearch:
            ptime = npfact*nbins
        if not nopdsearch:
            pdtime = npfact*nbins
        if not nodmsearch:
            dmtime = ndmfact*nbins
        time += ((ptime * pdtime * dmtime)/1e4)
    time = time*2  # compute time is very sporadic so just give double the allocation time
    if time > 86399.:
        logger.warn("Estimation for prepfold time greater than one day")
        time = 86399

    return time


def add_prepfold_to_commands(run_dir, pulsar=None, commands=None, kwargs):
    """
    Adds prepfold commands to a list

    Parameters:
    -----------
    run_dir: string
        The directory to work in. Typically the pointing directory.
    puslar: string
        OPTIONAL - The J name of the pulsar. If supplied, will use the archived dm and period values for this pulsar. Default: None
    commands: list
        OPTIONAL - A list of commands. Can be empty, this list will be appended to by the function. Default: None
    kwargs: dictionary
        Any arguments that can be handed to prepfold where the key is the argument and the value is the arguemnt's value

    Returns:
    --------
    commands: list
        The commands list that was input with the prepfold commands appended
    """
    if commands is None:
        commands = []

    options = ""
    for key, val in kwargs.items():
        if val is not None or val is True:
            options += " {0} {1}".format(key, val)
    options += " *fits".format(files)
    commands.append(f'cd {run_dir}')
    if pulsar:
        commands.append(f'echo "Folding on known pulsar {pulsar}"')
        commands.append(f'psrcat -e {pulsar} > {pulsar}.eph')
        commands.append(f"sed -i '/UNITS           TCB/d' {pulsar}.eph")
        commands.append(f"prepfold -par {pulsar}.eph {options}")
        commands.append('errorcode=$?')
        commands.append('if [ "$errorcode" != "0" ]; then')
        commands.append('   echo "Folding using the -psr option"')
        commands.append(f'   prepfold -psr {pulsar} {options}')
        commands.append('fi')
    else:  # candidate
        commands.append("prepfold {}".format(option))

    return commands


def submit_prepfold(pipe, run_dir, kwargs):
    """
    Submits a prepfold job for the given parameters

    Parameters:
    -----------
    run_dir: string
        The directory to work in. Typically the pointing directory.
    kwargs: dictionary
        Any arguments that can be handed to prepfold where the key is the argument and the value is the arguemnt's value

    Returns:
    --------
    job_id: int
        The ID of the submitted job
    """
    commands = add_prepfold_to_commands(run_dir, pulsar=pulsar, commands=commands, kwargs)

    # Check if prepfold worked:
    commands.append("errorcode=$?")
    commands.append("echo 'errorcode' $errorcode")
    commands.append('if [ "$errorcode" != "0" ]; then')
    commands.append("   echo 'Prepfold operation failure!'")
    commands.append("   exit $errorcode")
    commands.append("fi")

    batch_dir = os.path.join(
        comp_config['base_product_dir'], pipe["obs"]["id"], "batch")
    batch_name = f"bf_{pipe['source']['name']}_{pipe['obs']['id']}_b{kwargs['-n']}"
    time = prepfold_time_alloc(pipe, kwargs)
    time = str(datetime.timedelta(seconds=int(time)))
    job_id = submit_slurm(batch_name, commands,
                batch_dir=batch_dir,
                slurm_kwargs={"time": time},
                module_list=['presto/master'],
                submit=True)

    logger.info("Submitting prepfold Job")
    logger.info(f"Pointing directory:        {pipe['run_ops']['dir']}")
    logger.info(f"Pulsar name:               {pipe['source']['name']}")
    logger.info(f"Number of bins to fold on: {kwargs['-n']}")
    logger.info(f"Job name:                  {batch_name}"
    logger.info(f"Time Allocation:           {}".format(time))
    logger.info(f"Job ID:                    {}".format(job_id))

    return job_id
