
// Set up beamformer output types
bf_out = " -R NONE -U 0,0 -O -X --smart "
if ( params.summed ) {
    bf_out = " -N 1" + bf_out
}
if ( params.ipfb ) {
    bf_out = " -v" + bf_out
}
else {
    bf_out = " -p" + bf_out
}
fi


process beamform_setup {
    output:
    path "${params.obsid}_beg_end_dur.txt",  emit: beg_end_dur
    path "${params.obsid}_channels.txt", emit: channels

    """
    #!/usr/bin/env python
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
    beg, end = obs_max_min(${params.obsid}, channels[0])
    if not "${params.all}" == "true":
        beg = np.max([${params.begin}, beg])
        end = np.min([${params.end}, end])
    dur = end - beg + 1
    with open("${params.obsid}_beg_end_dur.txt", "w") as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        spamwriter.writerow([beg, end, dur])

    with open("${params.obsid}_channels.txt", "w") as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        for gpubox, chan in enumerate(channels, 1):
            spamwriter.writerow([chan, "{:0>3}".format(gpubox)])

    # Make sure all the required directories are made
    mdir("${params.vcsdir}/${params.obsid}", "Data", ${params.gid})
    mdir("${params.vcsdir}/${params.obsid}", "Products", ${params.gid})
    mdir("${params.vcsdir}/batch", "Batch", ${params.gid})
    mdir("${params.vcsdir}/${params.obsid}/pointings", "Pointings", ${params.gid})
    """
}

process combined_data_check {
    when:
    params.no_combined_check == false

    input:
    tuple val(begin), val(end), val(dur)

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

    time "${ task.attempt * ( Float.valueOf(dur) * ( params.bm_read + params.bm_cal + points.size() * ( params.bm_beam + params.bm_write ) ) + 200 ) * 1.2 }s"
    errorStrategy 'retry'
    maxRetries 2
    maxForks params.max_gpu_jobs

    input:
    tuple val(begin), val(end), val(dur)
    tuple val(channel_id), val(gpubox), val(points)

    output:
    tuple val(channel_id), val(points), path("*fits")

    """
    if ${params.offringa}; then
        DI_file="calibration_solution.bin"
        jones_option="-C ${params.didir}/calibration_solution.bin -c ${params.didir}/../${params.calid}.metafits"
    else
        DI_file="hyperdrive_solutions.bin"
        jones_option="-C ${params.didir}/calibration_solution.bin -c ${params.didir}/../${params.calid}.metafits"
    fi

    srun make_mwa_tied_array_beam -m ${params.vcsdir}/${params.obsid}/${params.obsid}.metafits \
        -b ${begin} \
        -T ${dur} \
        -f ${channel_id} \
        -d ${params.vcsdir}/${params.obsid}/combined \
        -P ${params.pointing_file} \
        ${jones_option} \
        ${bf_out} \
    mv */*fits .
    """
}


process make_beam_ipfb {
    label 'gpu'
    label 'vcsbeam'
    publishDir "${params.vcsdir}/${params.obsid}/pointings/${point}", mode: 'copy', enabled: params.publish_fits, pattern: "*hdr"
    publishDir "${params.vcsdir}/${params.obsid}/pointings/${point}", mode: 'copy', enabled: params.publish_fits, pattern: "*vdif"

    time "${ task.attempt * ( Float.valueOf(dur) * ( params.bm_read + params.bm_cal * ( params.bm_beam + params.bm_write ) ) + 200 ) * 1.2 }s"
    errorStrategy 'retry'
    maxRetries 2
    maxForks params.max_gpu_jobs

    when:
    point != " " //Don't run if blank pointing given

    input:
    tuple val(utc), val(begin), val(end), val(dur)
    tuple val(channel_id), val(gpubox), val(point)

    output:
    tuple val(channel_id), val(point), path("*fits"), emit: fits
    tuple val(channel_id), val(point), path("*hdr"), path("*vdif"),  emit: vdif

    """
    if ${params.offringa}; then
        DI_file="calibration_solution.bin"
        jones_option="-O ${params.didir}/calibration_solution.bin -C ${gpubox.toInteger() - 1}"
    else
        jones_option="-J ${params.didir}/DI_JonesMatrices_node${gpubox}.dat"
    fi

    if ${params.publish_fits}; then
        mkdir -p -m 771 ${params.vcsdir}/${params.obsid}/pointings/${point}
    fi
    
    srun make_beam -o ${params.obsid} -b ${begin} -e ${end} -a 128 -n 128 \
-f ${channel_id} \${jones_option} \
-d ${params.vcsdir}/${params.obsid}/combined -P ${point} \
-r 10000 -m ${params.vcsdir}/${params.obsid}/${params.obsid}_metafits_ppds.fits \
-p -v -t 6000 -F ${params.didir}/flagged_tiles.txt -z ${utc} -g 11
    mv */*fits .
    """
}


workflow pre_beamform {
    // Performs metadata calls and data checks
    main:
        beamform_setup()
        // Grab outputs from the CSVs
        beg_end_dur = beamform_setup.out.beg_end_dur.splitCsv()
        channels    = beamform_setup.out.channels.splitCsv()

        combined_data_check(beamform_setup.out.beg_end_dur.splitCsv())
    emit:
        // Combine all the constant metadata and make it a value channel (with collect) so it will be used for each job
        // Format:  [ utc, begin(GPS), end(GPS), duration(s) ]
        beg_end_dur
        // Channels in the format [ channel_id ]
        channels
}


workflow beamform {
    // Beamforms MWA voltage data
    take:
        // Metadata in the format [ begin(GPS), end(GPS), duration(s) ]
        beg_end_dur
        // The index of the first channel
        first_channel
        // params.pointing_file containing a list of pointings in the format HH:MM:SS +-DD:MM:SS
        pointing_file
    main:
        // Combine the each channel with each pointing (group) so you make a job for each combination
        make_beam(
            beg_end_dur,
            first_channel,
            pointing_file,
        )
    emit:
        make_beam.out // [ pointing, fits_file ]
}

workflow beamform_ipfb {
    // Beamforms MWA voltage data and performs and Inverse Polyphase Filter Bank to increase time resolution
    take:
        // Metadata in the format [ utc, begin(GPS), end(GPS), duration(s) ]
        utc_beg_end_dur
        // Channel pair in the format [ channel_id, gpubox_id ]
        channels
        // List of pointings in the format HH:MM:SS_+-DD:MM:SS
        pointings
    main:
        // Combine the each channel with each pointing so you make a job for each combination
        chan_point = channels.combine( pointings.flatten().map{ [ it ] } )
        make_beam_ipfb(
            utc_beg_end_dur,
            chan_point
        )
        // Group by the pointing for splicing
        splice( make_beam_ipfb.out.fits.groupTuple( by: 1, size: 24 ) )
    emit:
        fits = splice.out // [ pointing, fits_file ]
        vdif = make_beam_ipfb.out.vdif // [ channel_id, point, hdr, vdif ]
}
