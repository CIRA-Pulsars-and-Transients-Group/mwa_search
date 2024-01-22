
// Set up beamformer output types
bf_out = " -R NONE -U 0,0 -O -X --smart "
if ( params.summed ) {
    bf_out = " -N 1" + bf_out
}
if ( params.ipfb ) {
    bf_out = " -v" + bf_out
    params.outfile = "vdif"
}
else {
    bf_out = " -p" + bf_out
    params.outfile = "fits"
}


process beamform_setup {
    output:
    path "${params.obsid}_beg_end_dur.txt",  emit: beg_end_dur
    path "${params.obsid}_channels.txt", emit: channels
    path "${params.obsid}_pointings.txt", emit: pointings

    """
    #!/usr/bin/env python
    import sys
    import csv
    import numpy as np

    from mwa_search.metafits_utils import load_metafits_context, obs_max_min, mdir

    # Load metafits context for future use
    context = load_metafits_context(
        "${params.vcsdir}/${params.obsid}",
        "${params.obsid}",
        "${params.obsid}.metafits",
    )

    # Get the channel numbers
    channels = np.array([c.rec_chan_number for c in context.metafits_coarse_chans])

    # Work out begin and end time of observation available on disk
    beg, end = obs_max_min("${params.vcsdir}/${params.obsid}/combined", channels[0])
    if not "${params.all}" == "true":
        beg = np.max([int("${params.begin}"), beg])
        end = np.min([int("${params.end}"), end])
    dur = end - beg + 1
    if dur < 0:
        print("negative duration")
        sys.exit(0)

    with open("${params.obsid}_channels.txt", "w") as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        spamwriter.writerow([channels[0]])

    # Make sure all the required directories are made
    mdir("${params.vcsdir}/${params.obsid}", "Data", ${params.gid})
    mdir("${params.vcsdir}/${params.obsid}", "Products", ${params.gid})
    mdir("${params.vcsdir}/batch", "Batch", ${params.gid})
    mdir("${params.vcsdir}/${params.obsid}/pointings", "Pointings", ${params.gid})

    with open("${params.pointing_file}") as infile:
        pointings = infile.readlines()
    npoints = len(pointings)
    with open("${params.obsid}_pointings.txt", "w", newline='') as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        for p in pointings:
            rah, ram, ras = p.split()[0].split(":")
            dech, decm, decs = p.split()[1].split(":")
            ras = np.round(float(ras), 2)
            decs = np.round(float(decs), 2)
            ra = ':'.join([rah, ram, format(ras, "05.2f")])
            dec = ':'.join([dech, decm, format(decs, "05.2f")])
            pointing_dir = '_'.join([ra, dec])
            mdir(f"${params.vcsdir}/${params.obsid}/pointings/{pointing_dir}", "Pointing Directory", ${params.gid})
            spamwriter.writerow([pointing_dir])
    
    with open("${params.obsid}_beg_end_dur.txt", "w") as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        spamwriter.writerow([beg, end, dur, npoints])
    """
}

process combined_data_check {
    when:
    params.no_combined_check == false

    input:
    tuple val(begin), val(end), val(dur), val(npoints)

    """
    #!/usr/bin/env python

    import sys
    from mwa_search.obs_tools import check_data

    #Perform data checks
    dur = ${end} - ${begin} + 1
    check = check_data("${params.obsid}", beg=${begin}, dur=dur)
    if not check:
        print("ERROR: Recombined check has failed. Cannot continue.")
        sys.exit(1)
    else:
        print("Recombined check passed, all files present.")
    """
}


process make_beam {
    label 'gpu'
    label 'vcsbeam'

    time '6h' 
    errorStrategy 'retry'
    maxRetries 0
    maxForks params.max_gpu_jobs

    input:
    tuple val(begin), val(end), val(dur), val(npoints)
    tuple val(channel_id)
    path(pointings)

    output:
    path("*${params.outfile}")

    """
    srun make_mwa_tied_array_beam -m ${params.vcsdir}/${params.obsid}/${params.obsid}.metafits \
        -b ${begin} \
        -T ${dur} \
        -f ${channel_id} \
        -d ${params.vcsdir}/${params.obsid}/combined \
        -P ${params.pointing_file} \
        -C ${params.didir}/${params.calid}_hyperdrive_solutions.bin \
        -c ${params.didir}/../vis/${params.calid}.metafits \
        ${bf_out}
    ls \$(pwd)
    for f in `cat ${pointings} | tr -d '\r'` ; do mv ./*\${f}*.fits ${params.vcsdir}/${params.obsid}/pointings/\$f/; done
    """
}


workflow pre_beamform {
    // Performs metadata calls and data checks
    main:
        beamform_setup()
        // Grab outputs from the CSVs
        beg_end_dur = beamform_setup.out.beg_end_dur.splitCsv()
        channels    = beamform_setup.out.channels.splitCsv()
        pointings   = beamform_setup.out.pointings

        combined_data_check(beamform_setup.out.beg_end_dur.splitCsv())
    emit:
        // Combine all the constant metadata and make it a value channel (with collect) so it will be used for each job
        // Format:  [ utc, begin(GPS), end(GPS), duration(s) ]
        beg_end_dur
        // Channels in the format [ channel_id ]
        channels
        pointings
}


workflow beamform {
    // Beamforms MWA voltage data
    take:
        // Metadata in the format [ begin(GPS), end(GPS), duration(s) ]
        beg_end_dur
        // The index of the first channel
        first_channel
        pointings
    main:
        // Combine the each channel with each pointing (group) so you make a job for each combination
        make_beam(
            beg_end_dur,
            first_channel,
            pointings,
        )
    emit:
        make_beam.out // output files
} 
