// Some math for the accelsearch command
// convert to freq
min_freq = 1 / params.max_period
max_freq = 1 / params.min_period
// adjust the freq to include the harmonics
min_f_harm = min_freq
max_f_harm = max_freq * params.nharm

// Set up presto dedispersion settings
dedisp_options = " "
if ( params.noweights ) {
    dedisp_options = " -noweights " + dedisp_options
}
if ( params.noscales ) {
    dedisp_options = " -noscales " + dedisp_options
}
if ( params.nooffsets ) {
    dedisp_options = " -nooffsets " + dedisp_options
}

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
    search_time = params.search_scale * Float.valueOf(dur) * (0.006*Float.valueOf(ndm) + 1)
    // Max time is 24 hours for many clusters so always use less than that
    if ( search_time < 86400 ) {
        return "${search_time}s"
    }
    else {
        return "86400s"
    }
}

process get_freq_and_dur {
    input:
    tuple val(obsid), path(fits_dir)

    output:
    tuple val(obsid), path(fits_dir), path("name_freq_dur.csv")

    """
    #!/usr/bin/env python

    import os
    import csv
    import glob
    from astropy.io import fits
    from mwa_search.metafits_utils import mdir

    # Read in fits file to read its header
    dur = 0
    pointing_dir = "${fits_dir}".replace('\\\\', '')
    print(pointing_dir)
    for i, fits_file in enumerate(glob.glob(f"${params.vcsdir}/${obsid}/pointings/{pointing_dir}/*.fits")):
        hdul = fits.open(fits_file)
        if i == 0:
            name = f"${params.cand}_{fits_file.split('/')[-1].split('_ch')[0]}"
            # Grab the centre frequency in MHz
            freq = hdul[0].header['OBSFREQ']
        # Calculate the observation duration in seconds
        dur += hdul[1].header['NAXIS2'] * hdul[1].header['TBIN'] * hdul[1].header['NSBLK']

    mdir("${params.out_dir}", "Output Directory", ${params.gid})

    # Export both values as a CSV for easy output
    with open("name_freq_dur.csv", "w") as outfile:
        spamwriter = csv.writer(outfile, delimiter=',')
        spamwriter.writerow([name, freq, dur])
    """
}

process ddplan {
    input:
    tuple val(obsid), val(name), path(fits_path), val(centre_freq), val(dur)

    output:
    tuple val(obsid), val(name), path(fits_path), val(centre_freq), val(dur), path('DDplan*.txt')

    """
    #!/usr/bin/env python

    import csv
    import numpy as np
    from vcstools.catalogue_utils import grab_source_alog
    from mwa_search.dispersion_tools import dd_plan

    if '${name}'.startswith('Blind'):
        output = dd_plan(${centre_freq}, 30.72, 3072, 0.1, ${params.dm_min}, ${params.dm_max},
                         0.2, 0.8, smear_fact=3, nsub_smear_fact=3,
                         min_dm_step=${params.dm_min_step}, max_dm_step=${params.dm_max_step},
                         max_dms_per_job=${params.max_dms_per_job}, max_nsub=384)
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

    # Make a file for each work function batch
    work_function_batch = []
    wfi = 0
    wf_sum = 0.
    total_dm_steps = int(np.array(output).sum(axis=0)[3])
    for dd_line in output:
        dm_min, dm_max, dm_step, ndm, timeres, downsamp, nsub, total_work_factor = dd_line
        if total_work_factor > ${params.max_work_function}:
            # Ouput a file with just the max_work_function worth of DMs
            with open(f"DDplan_{wfi:03d}_a{total_dm_steps}_n{ndm}.txt", "w") as outfile:
                spamwriter = csv.writer(outfile, delimiter=',')
                spamwriter.writerow([
                    dm_min,
                    dm_max,
                    dm_step,
                    ndm,
                    timeres,
                    downsamp,
                    nsub,
                    total_work_factor
                ])
            wfi += 1
            continue

        # Append the leftover to the batch list

        # check if the batch list is ready to output
        wf_sum += total_work_factor
        if wf_sum > ${params.max_work_function}:
            # Ouput file with multiple ddplan lines
            local_dm_steps = int(ndm + np.array(work_function_batch).sum(axis=0)[3])
            with open(f"DDplan_{wfi:03d}_a{total_dm_steps}_n{local_dm_steps}.txt", "w") as outfile:
                spamwriter = csv.writer(outfile, delimiter=',')
                for out_line in work_function_batch:
                    spamwriter.writerow(out_line)
                # Ouput final new line
                spamwriter.writerow([
                    dm_min,
                    dm_max,
                    dm_step,
                    ndm,
                    timeres,
                    downsamp,
                    nsub,
                    total_work_factor
                ])
                wfi += 1
                work_function_batch = []

            wf_sum = 0
            continue

        work_function_batch.append([dm_min, dm_max, dm_step, ndm, timeres, downsamp, nsub, total_work_factor])

    # Write final file
    if work_function_batch:
        local_dm_steps = int(np.array(work_function_batch).sum(axis=0)[3])
        with open(f"DDplan_{wfi:03d}_a{total_dm_steps}_n{local_dm_steps}.txt", "w") as outfile:
            spamwriter = csv.writer(outfile, delimiter=',')
            for out_line in work_function_batch:
                spamwriter.writerow(out_line)
    """
}


process rfifind {
    label 'cpu'
    label 'presto_rfifind'

    time '4h'
    memory '16 GB'

    input:
    tuple val(obsid), val(name), path(fits_dir), val(freq), val(dur)

    output:
    tuple val(obsid), path("*rfifind.mask"), path("*rfifind.stats")

    """
    if ${params.rfifind}; then
        rfifind -blocks 16 -o ${name} ${params.vcsdir}/${obsid}/pointings/${fits_dir}/*.fits
    else
        touch ${name}_rfifind.mask
        touch ${name}_rfifind.stats
    fi
    """
} 
    

process search_dd_fft_acc {
    label 'cpu'
    label 'presto_search'

    time { search_time_estimate(dur, params.max_work_function) }
    memory { "${task.attempt * 4} GB"}
    maxRetries 1
    errorStrategy 'retry'
    maxForks params.max_search_jobs
    publishDir params.out_dir, mode: 'copy'

    input:
    tuple val(obsid), val(name), path(fits_dir), val(freq), val(dur), val(ndms_job), val(ddplans), path(rfifind_mask), path(rfifind_stats)

    output:
    tuple val(name), path("*ACCEL_${params.zmax}.tar"), path("*inf.tar"), path("*singlepulse.tar"), path('*cand.tar'), path('*ffa.tar')

    """
    printf "\\n#Dedispersing the time series at \$(date +"%Y-%m-%d_%H:%m:%S") --------------------------------------------\\n"
    # Loop over ddplan lines
    for ddplan in "${ddplans.join('" "').replace(", ", ",")}"; do
        # cut off [ and ]
        ddplan=\$( echo "\${ddplan}" | cut -d "[" -f 2 | cut -d "]" -f 1 )
        echo \${ddplan}
        # Split into each value
        IFS=, read -r dm_min dm_max dm_step ndm timeres downsamp nsub wf <<< \${ddplan}
        # Calculate the number of output data points
        numout=\$(bc <<< "scale=0; ${dur} * 10000 / \${downsamp}")
        numout=\$(printf "%.0f\n" "\${numout}")
        if (( \$numout % 2 != 0 )) ; then
            numout=\$(expr \$numout + 1)
        fi
        if ${params.rfifind}; then
            rfifind_command="-mask ${name}_rfifind.mask"
        else
            rfifind_command=""
        fi 
        echo "Performing dedispersion with"
        echo "    dm_min: \${dm_min}, dm_max: \${dm_max}, dm_step: \${dm_step}"
        echo "    ndm: \${ndm}, timeres: \${timeres}, downsamp: \${downsamp}, nsub: \${nsub}, nout: \${numout}"
        echo "    dedispersion options : ${dedisp_options},"
        prepsubband -ncpus ${task.cpus} -lodm \${dm_min} -dmstep \${dm_step} -numdms \${ndm} -nsub \${nsub} \
-downsamp \${downsamp} -numout \${numout} ${dedisp_options} -o ${name} \${rfifind_command} \
${params.vcsdir}/${obsid}/pointings/${fits_dir}/*.fits
    done

    printf "\\n#Performing the FFTs at \$(date +"%Y-%m-%d_%H:%m:%S") -----------------------------------------------------\\n"
    printf "\\n#Performing the periodic search at \$(date +"%Y-%m-%d_%H:%m:%S") ------------------------------------------\\n"
    for i in \$(ls *.dat); do
        realfft \${i}
        if ${params.rednoise}; then
            rednoise \${i%.dat}.fft
            mv \${i%.dat}_red.fft \${i%.dat}.fft
            mv \${i%.dat}_red.inf \${i%.dat}.inf
        fi
        # Somtimes this has a 255 error code when data.pow == 0 so ignore it
        accelsearch -ncpus ${task.cpus} -zmax ${params.zmax} -flo ${min_f_harm} -fhi ${max_f_harm} -numharm ${params.nharm} \${i%.dat}.fft || true
    done

    printf "\\n#Performing the single pulse search at \$(date +"%Y-%m-%d_%H:%m:%S") ------------------------------------------\\n"
    ${params.presto_python_load}
    single_pulse_search.py -p -m 0.5 -b *.dat
    printf "\\n#Tar-ing up the data at \$(date +"%Y-%m-%d_%H:%m:%S") ------------------------------------------\\n"
    tar -cvf ${name}_DM\${dm_max}_ACCEL_${params.zmax}.tar --force-local *ACCEL_${params.zmax}
    tar -cvf ${name}_DM\${dm_max}_inf.tar --force-local *.inf
    tar -cvf ${name}_DM\${dm_max}_singlepulse.tar --force-local *.singlepulse
    tar -cvf ${name}_DM\${dm_max}_cand.tar --force-local *.cand

    if ${params.ffa}; then
        for f in `cat ${params.ffa_dms}`; do
            if [ -f ${name}_DM\${f}.inf ]; then
                echo ${name}_DM\${f}.dat >> files_to_tar.txt
                echo ${name}_DM\${f}.inf >> files_to_tar.txt
            fi
        done
        if [ -f files_to_tar.txt ]; then
            tar -cvf ${name}_DM\${dm_max}_ffa.tar --force-local -T files_to_tar.txt
        fi
    fi
    if [ ! -f ${name}_DM\${dm_max}_ffa.tar ]; then
        tar -cvf ${name}_DM\${dm_max}_ffa.tar --force-local -T /dev/null
    fi 
    printf "\\n#Finished at \$(date +"%Y-%m-%d_%H:%m:%S") ----------------------------------------------------------------\\n"
    """
}


process run_ffa {
    label 'ffa'
    label 'cpu'

    time '6h'
    memory '32 GB'
    publishDir params.out_dir, mode: 'copy'

    input:
    tuple val(name), path(ffa)

    output:
    tuple val(name), path("*peaks.csv"), path("*clusters.csv"), path("candidate_*.*")

    shell:
    '''
    for f in !{ffa}; do
        tar -xvf ${f} --force-local
    done
    rffa *.inf -c !{params.ffa_config}
    if [ ! -f peaks.csv ]; then
        touch peaks.csv
    fi
    if [ ! -f candidate_0000.json ]; then
        touch candidate_0000.json
    fi
    if [ ! -f clusters.csv ]; then
        touch clusters.csv
    fi
    '''
}


process accelsift {
    label 'cpu'
    label 'presto'

    time '2h'
    memory '4 GB'
    errorStrategy 'retry'
    maxRetries 1

    input:
    tuple val(name), path(accel), path(inf), path(single_pulse), path(cands)

    output:
    tuple val(name), path(accel), path(inf), path(single_pulse), path(cands), path("cands_*greped.txt"), path("*candidates.tar")

    shell:
    '''
    for f in !{accel}; do
        tar -xvf ${f} --force-local
    done
    for f in !{inf}; do
        tar -xvf ${f} --force-local
    done
    for f in !{cands}; do
        tar -xvf ${f} --force-local
    done
    # Remove incomplete or errored files
    for i in *ACCEL_!{params.zmax}; do
        if [ $(grep " Number of bins in the time series" $i | wc -l) == 0 ]; then
            rm ${i%%_ACCEL_}*
        fi
    done

    ACCEL_sift.py --file_name !{name}
    if [ -f cands_!{name}.txt ]; then
        # Get candidate lines and replace space with a comma
        grep !{name} cands_!{name}.txt | tr -s '[:space:]' | sed -e 's/ /,/g' > cands_!{name}_greped.txt
    else
        #No candidates so make an empty file
        touch cands_!{name}_greped.txt
    fi
    printf "tar -cvf !{name}_candidates.tar " > tar_candidates.sh
    cat cands_!{name}_greped.txt | awk -F"_ACCEL" '{printf $1"* "}' >> tar_candidates.sh
    echo "--force-local" >> tar_candidates.sh
    sh tar_candidates.sh
    '''
}


process prepfold {
    label 'cpu'
    label 'presto'

    publishDir params.out_dir, mode: 'copy', enabled: params.publish_all_prepfold
    time "${ (int) ( params.prepfold_scale * dur ) }s"
    memory '4 GB'
    errorStrategy 'retry'
    maxRetries 1

    input:
    tuple val(cand_lines), val(obsid), val(dur), path(cand_tar), path(fits_dir), path(rfifind_mask), path(rfifind_stats)

    output:
    tuple path("*pfd"), path("*bestprof"), path("*ps"), path("*png")//, optional: true) // some PRESTO installs don't make pngs

    //no mask command currently
    """
    # Loop over each candidate
    for cand_line in "${cand_lines.join('" "').replace(", ", ",")}"; do
        # cut off [ and ]
        cand_line=\$( echo "\${cand_line}" | cut -d "[" -f 2 | cut -d "]" -f 1 )
        echo \${cand_line}
        # Split into each value
        IFS=, read -r name dm c3 c4 nharm period c7 c8 c9 c10 c11 <<< \${cand_line}
        fits_name=\${name%_DM*}
        fits_name=\${fits_name#*_}
        tar -xvf ${cand_tar} --force-local --wildcards \${name%_ACCEL*}*.inf
        tar -xvf ${cand_tar} --force-local --wildcards \${name%_ACCEL*}*.cand

        # Set up the prepfold options to match the ML candidate profiler
        period=\$(echo "scale=8; \$period / 1000" | bc | awk '{printf "%.8f", \$0}')
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
        ddm=\$(echo "scale=10;0.000241*138.87^2*\${dmstep} / (1/\$period *\$nbins)" | bc)
        ndmfact=\$(echo "1 + 1/(\$ddm*\$nbins)" | bc)
        echo "ndmfact: \$ndmfact   ddm: \$ddm"

        if ${params.rfifind}; then
            rfifind_command="-mask *_rfifind.mask"
        else
            rfifind_command=""
        fi

        prepfold -ncpus ${task.cpus} -o \$name  -accelfile \${name%:*}.cand -accelcand \${name##*:} \
    -n \$nbins -dm \$dm -nosearch -noxwin -noclip -nsub 256 -npart \$ntimechunk -dmstep \$dmstep \
    -pstep 1 -pdstep 2 -npfact \$period_search_n -ndmfact \$ndmfact \${rfifind_command} ${dedisp_options} ${params.vcsdir}/${obsid}/pointings/${fits_dir}/\${fits_name}*.fits

    done
    """
}


process search_dd {
    label 'cpu'
    label 'presto_search'

    time { search_time_estimate(dur, params.max_work_function) }
    memory { "${task.attempt * 30} GB"}
    maxRetries 2
    errorStrategy 'retry'
    maxForks params.max_search_jobs

    input:
    tuple val(name), path(fits_files), val(freq), val(dur), val(ndms_job), val(ddplans)

    output:
    tuple val(name), path("*.inf"), path("*.singlepulse")

    """
    printf "\\n#Dedispersing the time series at \$(date +"%Y-%m-%d_%H:%m:%S") --------------------------------------------\\n"
    # Loop over ddplan lines
    for ddplan in "${ddplans.join('" "').replace(", ", ",")}"; do
        # cut off [ and ]
        ddplan=\$( echo "\${ddplan}" | cut -d "[" -f 2 | cut -d "]" -f 1 )
        echo \${ddplan}
        # Split into each value
        IFS=, read -r dm_min dm_max dm_step ndm timeres downsamp nsub wf <<< \${ddplan}
        # Calculate the number of output data points
        numout=\$(bc <<< "scale=0; ${dur} * 10000 + \${downsamp}")
        numout=\$(printf "%.0f\n" "\${numout}")
        if (( \$numout % 2 != 0 )) ; then
            numout=\$(expr \$numout + 1)
        fi
        echo "Performing dedispersion with"
        echo "    dm_min: \${dm_min}, dm_max: \${dm_max}, dm_step: \${dm_step}"
        echo "    ndm: \${ndm}, timeres: \${timeres}, downsamp: \${downsamp}, nsub: \${nsub},"
        prepsubband -ncpus ${task.cpus} -lodm \${dm_min} -dmstep \${dm_step} -numdms \${ndm} -zerodm -nsub \${nsub} \
-downsamp \${downsamp} -numout \${numout} -o ${name} *.fits
    done

    printf "\\n#Performing the single pulse search at \$(date +"%Y-%m-%d_%H:%m:%S") ------------------------------------------\\n"
    ${params.presto_python_load}
    single_pulse_search.py -p -m 0.5 -b *.dat
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
    tuple val(name), val(obsid), path(sps_tar), path(fits_dir)

    output:
    path "*pdf", optional: true
    path "*.SpS"

    """
    for f in ${sps_tar}; do
        tar -xvf \$f --force-local
    done
    cat *.singlepulse > ${name}.SpS
    #-SNR_min 4 -SNR_peak_min 4.5 -DM_cand 1.5 -N_min 3
    single_pulse_searcher.py -fits ${params.vcsdir}/${obsid}/pointings/${fits_dir}/*.fits -no_store -plot_name ${name}_sps.pdf ${name}.SpS
    """
}


workflow pulsar_search {
    take:
        name_fits_files // [val(obsid), path(fits_files)]
    main:
        // Grab meta data from the fits file
        get_freq_and_dur( name_fits_files ) // [ name, fits_file, freq, dur ]

        // Grab the meta data out of the CSV
        name_fits_freq_dur = get_freq_and_dur.out.map { obsid, fits, meta -> [ obsid, meta.splitCsv()[0][0], fits, meta.splitCsv()[0][1], meta.splitCsv()[0][2] ] }
        name_fits_freq_dur.view()
        ddplan( name_fits_freq_dur )
        // ddplan's output format is [ name, fits_file, centrefreq(MHz), duration(s), DDplan_file ]

        // Trasponse to get a DM plan file each row/job then split the csv to get the DDplan
        // Also using groupKey so that future groupTuple will have the size of the total number of DMs
        // The DDplan file has the name format DDplan_{i}_a{total_dm_steps}_n{local_dm_steps}.txt
        rfifind( name_fits_freq_dur )
        search_dd_fft_acc(
            ddplan.out.transpose()
            .map { obsid, name, fits, freq, dur, ddplan ->
                [ obsid, groupKey(name, ddplan.baseName.split("_n")[0].split("_a")[-1].toInteger() ), fits, freq, dur, ddplan.baseName.split("_n")[-1], ddplan.splitCsv() ]
            }.combine( rfifind.out.map{ [ it[-2], it[-1] ] } )
        )
        // Output format: [ name,  ACCEL_summary, presto_inf, single_pulse, periodic_candidates ]

        // Get all the inf, ACCEL and single pulse files and sort them into groups with the same name key
        // This uses the groupKey so it should output the channel as soon as it has all the DMs
        inf_accel_sp_cand = search_dd_fft_acc.out.transpose( remainder: true ).groupTuple( remainder: true ).map{ key, accel, inf, sp, cands, dat -> [ key.toString(), accel, inf, sp, cands ] }
        accelsift( inf_accel_sp_cand )
        if ( params.ffa ) {
            ffa_input = search_dd_fft_acc.out.transpose( remainder: true ).groupTuple( remainder: true ).map{ key, accel, inf, sp, cands, ffa -> [ key.toString(), ffa ] }
            run_ffa( ffa_input )
        }

        // For each line of each candidate file and treat it as a candidate
        accel_cands = accelsift.out.flatMap{ it[-2].splitCsv() }.map{ it -> [it[0].split("_ACCEL")[0], it ] }
        accel_tar = accelsift.out.map{ it[-1] } 
        // For each accel cand, pair it with its inf and cand files
        accel_inf_cands = accel_cands.combine( accel_tar )
        // Pair them with fits files and metadata so they are ready to fold
        cands_for_prepfold = name_fits_freq_dur.combine( accel_inf_cands ).map{it -> [it[6], it[0], Float.valueOf(it[4]), it[-1], it[2]] }
            // collate by several prepfold jobs together
            .collate( params.max_folds_per_job )
            // reformat them to be in lists for each data type
            .transpose().collate( 5 )
            .map { cand_lines, obsid, durs, cand_tar, fits_dir -> [ cand_lines, obsid.unique()[0], durs.sum(), cand_tar.unique()[0], fits_dir.unique()[0] ]}
            .combine( rfifind.out.map{ [ it[-2], it[-1] ] } )
            // [ name, fits_files, dur, cand_line, cand_inf, cand_file ]
        prepfold( cands_for_prepfold )

        // Combined the grouped single pulse files with the fits files
        //single_pulse_searcher(
        //    inf_accel_sp_cand.map{ [ it[0], it[3] ] }.combine( name_fits_freq_dur ).map{ [ it[3], it[2], it[1], it[4] ] }
        //)
    emit:
        // [ pfd, bestprof, ps, png ]
        prepfold.out
}

workflow single_pulse_search {
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
        search_dd(
            ddplan.out.transpose()
            .map { name, fits, freq, dur, ddplan ->
                [ groupKey(name, ddplan.baseName.split("_n")[0].split("_a")[-1].toInteger() ), fits, freq, dur, ddplan.baseName.split("_n")[-1], ddplan.splitCsv() ]
            }
        )
        // Output format: [ name,  presto_inf, single_pulse ]

        // Get all the inf and single pulse files and sort them into groups with the same name key
        inf_accel_sp_cand = search_dd.out.transpose( remainder: true ).groupTuple( remainder: true ).map{ key, accel, inf, sp, cands -> [ key.toString(), accel - null, sp, cands - null ] }
        // Combined the grouped single pulse files with the fits files
        single_pulse_searcher(
            inf_accel_sp_cand.map{ [ it[0], it[2] ] }.concat( name_fits_files ).groupTuple().map{ [ it[0], it[1][0], it[1][1] ] }
        )
    emit:
        single_pulse_searcher.out[0]
        single_pulse_searcher.out[1]
}
