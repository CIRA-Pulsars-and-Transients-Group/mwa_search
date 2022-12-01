// Some math for the accelsearch command
// convert to freq
min_freq = 1 / params.max_period
max_freq = 1 / params.min_period
// adjust the freq to include the harmonics
min_f_harm = min_freq
max_f_harm = max_freq * params.nharm

// Work out total obs time
// if ( params.all ) {
//     // an estimation since there's no easy way to make this work
//     obs_length = 4805
// }
// else {
//     obs_length = params.end - params.begin + 1
// }



def search_time_estimate(dur, ndm) {
    // Estimate the duration of a search job in seconds
    return params.search_scale * Float.valueOf(dur) * (0.006*Float.valueOf(ndm) + 1)
}

process get_freq_and_dur {
    input:
    tuple val(name), path(fits_file)

    output:
    tuple val(name), path(fits_file), path("freq_dur.csv")

    """
    #!/usr/bin/env python

    import os
    import csv
    from astropy.io import fits

    # Read in fits file to read its header
    hdul = fits.open(r"${fits_file}".replace("\\\\", ""))
    # Grab the centre frequency in MHz
    freq = hdul[0].header['OBSFREQ']
    # Calculate the observation duration in seconds
    dur = hdul[1].header['NAXIS2'] * hdul[1].header['TBIN'] * hdul[1].header['NSBLK']

    # Export both values as a CSV for easy output
    with open("freq_dur.csv", "w") as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        spamwriter.writerow([freq, dur])
    """
}

process ddplan {
    input:
    tuple val(name), path(fits_file), val(centre_freq), val(dur)

    output:
    tuple val(name), path(fits_file), val(centre_freq), val(dur), path('DDplan.txt')

    """
    #!/usr/bin/env python

    from vcstools.catalogue_utils import grab_source_alog
    from mwa_search.dispersion_tools import dd_plan
    import csv

    if '${name}'.startswith('Blind'):
        output = dd_plan(${centre_freq}, 30.72, 3072, 0.1, ${params.dm_min}, ${params.dm_max},
                         min_DM_step=${params.dm_min_step}, max_DM_step=${params.dm_max_step},
                         max_dms_per_job=${params.max_dms_per_job})
    else:
        if '${name}'.startswith('dm_'):
            dm = float('${name}'.split('dm_')[-1].split('_')[0])
        elif '${name}'.startswith('FRB'):
            dm = grab_source_alog(source_type='FRB',
                 pulsar_list=['${name}'], include_dm=True)[0][-1]
        else:
            # Try RRAT first
            rrat_temp = grab_source_alog(source_type='RRATs',
                        pulsar_list=['${name}'.split("_")[0]], include_dm=True)
            if len(rrat_temp) == 0:
                #No RRAT so must be pulsar
                dm = grab_source_alog(source_type='Pulsar',
                     pulsar_list=['${name}'.split("_")[0]], include_dm=True)[0][-1]
            else:
                dm = rrat_temp[0][-1]
        dm_min = float(dm) - 2.0
        if dm_min < 1.0:
            dm_min = 1.0
        dm_max = float(dm) + 2.0
        output = dd_plan(${centre_freq}, 30.72, 3072, 0.1, dm_min, dm_max,
                         min_DM_step=${params.dm_min_step}, max_DM_step=${params.dm_max_step},
                         max_dms_per_job=${params.max_dms_per_job})
    with open("DDplan.txt", "w") as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        for o in output:
            spamwriter.writerow(o)
    """
}


process search_dd_fft_acc {
    label 'cpu'
    label 'presto_search'

    // Max time is 24 hours for many clusters so always use less than that
    time { search_time_estimate(dur, ddplan[3]) < 86400 ? \
            "${search_time_estimate(dur, ddplan[3])}s" :
            "86400s"}
    memory { "${task.attempt * 30} GB"}
    maxRetries 2
    errorStrategy 'retry'
    maxForks params.max_search_jobs

    input:
    tuple val(name), path(fits_files), val(freq), val(dur), val(ddplan)
    // ddplan[0] = dm_min
    // ddplan[1] = dm_max
    // ddplan[2] = dm_step
    // ddplan[3] = ndm
    // ddplan[4] = timeres
    // ddplan[5] = downsamp
    // ddplan[6] = nsub

    output:
    tuple val(name), path("*ACCEL_${params.zmax}"), path("*.inf"), path("*.singlepulse"), path('*.cand')
    //file "*ACCEL_0" optional true
    //Will have to change the ACCEL_0 if I do an accelsearch

    """
    echo "freq dur"
    echo ${freq} ${dur}
    echo "lowdm highdm dmstep ndms timeres downsamp"
    echo ${ddplan[0]} ${ddplan[1]} ${ddplan[2]} ${ddplan[3]} ${ddplan[4]} ${ddplan[5]} ${ddplan[6]}
    numout=${ (int) ( Float.valueOf( dur ) * 10000 / Float.valueOf( ddplan[5] ) ) }
    if (( \$numout % 2 != 0 )) ; then
        numout=\$(expr \$numout + 1)
    fi
    printf "\\n#Dedispersing the time series at \$(date +"%Y-%m-%d_%H:%m:%S") --------------------------------------------\\n"
    prepsubband -ncpus ${task.cpus} -lodm ${ddplan[0]} -dmstep ${ddplan[2]} -numdms ${ddplan[3]} -zerodm -nsub ${ddplan[6]} \
-downsamp ${ddplan[5]} -numout \${numout} -o ${name} ${params.obsid}_*.fits
    printf "\\n#Performing the FFTs at \$(date +"%Y-%m-%d_%H:%m:%S") -----------------------------------------------------\\n"
    realfft *dat
    printf "\\n#Performing the periodic search at \$(date +"%Y-%m-%d_%H:%m:%S") ------------------------------------------\\n"
    for i in \$(ls *.dat); do
        # Somtimes this has a 255 error code when data.pow == 0 so ignore it
        accelsearch -ncpus ${task.cpus} -zmax ${params.zmax} -flo ${min_f_harm} -fhi ${max_f_harm} -numharm ${params.nharm} \${i%.dat}.fft || true
    done
    printf "\\n#Performing the single pulse search at \$(date +"%Y-%m-%d_%H:%m:%S") ------------------------------------------\\n"
    ${params.presto_python_load}
    single_pulse_search.py -p -m 0.5 -b *.dat
    # cat *.singlepulse > ${name}_DM${ddplan[0]}-${ddplan[1]}.subSpS
    printf "\\n#Finished at \$(date +"%Y-%m-%d_%H:%m:%S") ----------------------------------------------------------------\\n"
    """
}


process accelsift {
    label 'cpu'
    label 'presto'

    time '25m'
    errorStrategy 'retry'
    maxRetries 1

    input:
    tuple val(name), path(accel), path(inf), path(single_pulse), path(cands)

    output:
    tuple val(name), path(accel), path(inf), path(single_pulse), path(cands), path("cands_*greped.txt")

    shell:
    '''
    # Remove incomplete or errored files
    for i in !{accel}; do
        if [ $(grep " Number of bins in the time series" $i | wc -l) == 0 ]; then
            rm ${i%%_ACCEL_}*
        fi
    done

    ACCEL_sift.py --file_name !{name}
    if [ -f cands_!{name}.txt ]; then
        grep !{name} cands_!{name}.txt > cands_!{name}_greped.txt
    else
        #No candidates so make an empty file
        touch cands_!{name}_greped.txt
    fi
    '''
}


process prepfold {
    label 'cpu'
    label 'presto'

    publishDir params.out_dir, mode: 'copy', enabled: params.publish_all_prepfold
    time "${ (int) ( params.prepfold_scale * Float.valueOf(dur) ) }s"
    errorStrategy 'retry'
    maxRetries 1

    input:
    tuple val(name), path(fits_files), val(dur), val(cand_line), path(cand_inf), path(cand_file)

    output:
    tuple path("*pfd"), path("*bestprof"), path("*ps"), path("*png")//, optional: true) // some PRESTO installs don't make pngs

    //no mask command currently
    //${cand_line.split()[0].substring(0, cand_line.split()[0].lastIndexOf(":")) + '.cand'}
    """
    echo "${cand_line.split()}"
    # Set up the prepfold options to match the ML candidate profiler
    temp_period=${Float.valueOf(cand_line.split()[7])/1000}
    period=\$(printf "%.8f" \$temp_period)
    if (( \$(echo "\$period > 0.01" | bc -l) )); then
        nbins=100
        ntimechunk=120
        dmstep=1
        period_search_n=1
    else
        # bin size is smaller than time resolution so reduce nbins
        nbins=50
        ntimechunk=40
        dmstep=3
        period_search_n=2
    fi

    # Work out how many dmfacts to use to search +/- 2 DM
    ddm=`echo "scale=10;0.000241*138.87^2*\${dmstep} / (1/\$period *\$nbins)" | bc`
    ndmfact=`echo "1 + 1/(\$ddm*\$nbins)" | bc`
    echo "ndmfact: \$ndmfact   ddm: \$ddm"

    #-p \$period
    prepfold -ncpus ${task.cpus} -o ${cand_line.split()[0]} -n \$nbins -dm ${cand_line.split()[1]} -noxwin -noclip -nsub 256 \
-accelfile ${cand_file} -accelcand ${cand_line.split()[0].split(":")[-1]} \
-npart \$ntimechunk -dmstep \$dmstep -pstep 1 -pdstep 2 -npfact \$period_search_n -ndmfact \$ndmfact -runavg *.fits
    """
}


process search_dd {
    label 'cpu'
    label 'presto_search'

    time { search_time_estimate(dur, ddplan[3]) < 86400 ? \
            "${search_time_estimate(dur, ddplan[3])}s" :
            "86400s"}
    memory { "${task.attempt * 3} GB"}
    maxRetries 2
    maxForks params.max_search_jobs

    input:
    tuple val(name), path(fits_files), val(freq), val(dur), val(ddplan)
    // ddplan[0] = dm_min
    // ddplan[1] = dm_max
    // ddplan[2] = dm_step
    // ddplan[3] = ndm
    // ddplan[4] = timeres
    // ddplan[5] = downsamp
    // ddplan[6] = nsub

    output:
    tuple val(name), path("*.inf"), path("*.singlepulse")
    //Will have to change the ACCEL_0 if I do an accelsearch

    """
    echo "freq dur"
    echo ${freq} ${dur}
    echo "lowdm highdm dmstep ndms timeres downsamp"
    echo ${ddplan[0]} ${ddplan[1]} ${ddplan[2]} ${ddplan[3]} ${ddplan[4]} ${ddplan[5]} ${ddplan[6]}
    numout=${ (int) ( Float.valueOf( dur ) * 10000 / Float.valueOf( ddplan[5] ) ) }
    if (( \$numout % 2 != 0 )) ; then
        numout=\$(expr \$numout + 1)
    fi
    printf "\\n#Dedispersing the time series at \$(date +"%Y-%m-%d_%H:%m:%S") --------------------------------------------\\n"
    prepsubband -ncpus ${task.cpus} -lodm ${ddplan[0]} -dmstep ${ddplan[2]} -numdms ${ddplan[3]} -zerodm -nsub ${ddplan[6]} \
-downsamp ${ddplan[5]} -numout \${numout} -o ${name} ${params.obsid}_*.fits
    printf "\\n#Performing the single pulse search at \$(date +"%Y-%m-%d_%H:%m:%S") ------------------------------------------\\n"
    ${params.presto_python_load}
    single_pulse_search.py -p -m 0.5 -b *.dat
    # cat *.singlepulse > ${name}_DM${ddplan[0]}-${ddplan[1]}.subSpS
    printf "\\n#Finished at \$(date +"%Y-%m-%d_%H:%m:%S") ----------------------------------------------------------------\\n"
    """
}


process single_pulse_searcher {
    label 'cpu'
    label 'large_mem'
    label 'sps'

    time '2h'
    publishDir params.out_dir, mode: 'copy'
    errorStrategy 'ignore'

    input:
    tuple val(name), path(sps), path(fits)

    output:
    path "*pdf", optional: true
    path "*.SpS"

    """
    cat *.singlepulse > ${name}.SpS
    #-SNR_min 4 -SNR_peak_min 4.5 -DM_cand 1.5 -N_min 3
    single_pulse_searcher.py -fits ${fits} -no_store -plot_name ${name}_sps.pdf ${name}.SpS
    """
}


workflow pulsar_search {
    take:
        name_fits_files // [val(candidateName_obsid_pointing), path(fits_files)]
    main:
        // Grab meta data from the fits file
        get_freq_and_dur( name_fits_files ) // [ name, fits_file, freq, dur ]

        // Grab the meta data out of the CSV
        name_fits_freq_dur = get_freq_and_dur.out.map { name, fits, meta -> [ name, fits, meta.splitCsv()[0][0], meta.splitCsv()[0][1] ] }
        ddplan( name_fits_freq_dur )
        // ddplan's output format is [ name, fits_file, centrefreq(MHz), duration(s), DDplan_file ]

        // so split the csv to get the DDplan and transpose to make a job for each row of the plan
        search_dd_fft_acc(
            ddplan.out.map { name, fits, freq, dur, ddplan -> [ name, fits, freq, dur, ddplan.splitCsv() ] }.transpose()
        )
        // Output format: [ name,  ACCEL_summary, presto_inf, single_pulse, periodic_candidates ]

        // Get all the inf, ACCEL and single pulse files and sort them into groups with the same name key
        inf_accel_sp_cand = search_dd_fft_acc.out.transpose().groupTuple()
        accelsift( inf_accel_sp_cand )

        // For each line of each candidate file and treat it as a candidate
        accel_cands = accelsift.out.map{ it[-1].splitCsv() }.flatten().map{ it -> [it.split()[0].split("_ACCEL")[0], it ] }
        // Grab the inf and cand files for the key (name including DM)
        inf_cands  = search_dd_fft_acc.out.map{ it[2] }.flatten().map{ [ it.baseName, it ] } // inf files
            .concat( search_dd_fft_acc.out.map{ it[4] }.flatten().map{ [ it.baseName.split("_ACCEL")[0], it ] } ) // cand files
            .groupTuple( size: 2 ).map{ [ it[0], it[1][0], it[1][1] ] } // grouping and reorganising
        // For each accel cand, pair it with its inf and cand files
        accel_inf_cands = accel_cands.cross( inf_cands )
            // Reorganise it so it's back to the just name key style
            .map{ [ it[0][0].split("_DM")[0], it[0][1], it[1][1], it[1][2] ] }
        // Pair them with fits files and metadata so they are read to fole
        cands_for_prepfold = name_fits_freq_dur.cross( accel_inf_cands )
            .map{ [ it[0][0], it[0][1], it[0][3], it[1][1], it[1][2], it[1][3] ] }
            // [ name, fits_files, dur, cand_line, cand_inf, cand_file ]
        prepfold( cands_for_prepfold )

        // Combined the grouped single pulse files with the fits files
        single_pulse_searcher(
            inf_accel_sp_cand.map{ [ it[0], it[3] ] }.concat( name_fits_files ).groupTuple().map{ [ it[0], it[1][0], it[1][1] ] }
        )
    emit:
        // [ pfd, bestprof, ps, png ]
        prepfold.out
}

workflow single_pulse_search {
    take:
        name_fits_files
    main:
        get_centre_freq()
        ddplan( name_fits_files.map{ it -> it[0] }.combine(get_centre_freq.out.splitCsv()) )
        search_dd( // combine the fits files and ddplan witht he matching name key (candidateName_obsid_pointing)
                   ddplan.out.splitCsv().map{ it -> [ it[0], [ it[1], it[2], it[3], it[4], it[5], it[6], it[7] ] ] }.concat(name_fits_files).groupTuple().\
                   // Find for each ddplan match that with the fits files and the name key then change the format to [val(name), val(dm_values), path(fits_files)]
                   map{ it -> [it[1].init(), [[it[0], it[1].last()]]].combinations() }.flatMap().map{ it -> [it[1][0], it[0], it[1][1]]} )
        single_pulse_searcher( search_dd.out.map{ it -> [it[0], [it[1]].flatten().findAll { it != null } + [it[2]].flatten().findAll { it != null }] }.\
                               groupTuple( size: total_dm_jobs, remainder: true).map{ it -> [it[0], it[1].flatten()] }.\
                               // Add fits files
                               concat(name_fits_files).groupTuple( size: 2 ).map{ it -> [it[0], it[1][0], it[1][1]]}  )
        // Get all the inf and single pulse files and sort them into groups with the same basename (obsid_pointing)
    emit:
        single_pulse_searcher.out[0]
        single_pulse_searcher.out[1]
}
