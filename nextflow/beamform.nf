#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.help = false
if ( params.help ) {
    help = """beamform.nf: A pipeline that will beamform and splice on all input pointings.
             |Required argurments:
             |  --obsid     Observation ID you want to process [no default]
             |  --calid     Observation ID of calibrator you want to process [no default]
             |  --begin     First GPS time to process [no default]
             |  --end       Last GPS time to process [no default]
             |  --all       Use entire observation span. Use instead of -b & -e. [default: ${params.all}]
             |  --pointing_file
             |              A file containing pointings with the RA and Dec separated by _
             |              in the format HH:MM:SS_+DD:MM:SS on each line, e.g.
             |              "19:23:48.53_-20:31:52.95\\n19:23:40.00_-20:31:50.00" [default: None]
             |
             |Beamforming types arguments (optional):
             |  --summed   Sum the Stoke paramters [default: ${params.summed}]
             |  --incoh    Also produce an incoherent beam [default: ${params.incoh}]
             |  --ipfb     Also produce a high time resolution Inverse Polyphase Filter Bank beam
             |             [default: ${params.ipfb}]
             |  --offringa Use offringa calibration solution instead of RTS [default: ${params.offringa}]
             |
             |Optional arguments:
             |  --vcstools_version
             |              The vcstools module version to use [default: ${params.vcstools_version}]
             |  --no_combined_check
             |              Don't check if all the combined files are available [default: false]
             |  -w          The Nextflow work directory. Delete the directory once the processs
             |              is finished [default: ${workDir}]""".stripMargin()
    println(help)
    exit(0)
}

include { pre_beamform; beamform } from './beamform_module'

workflow {
    pre_beamform()
    beamform(
        pre_beamform.out.beg_end_dur,
        pre_beamform.out.channels[0],
        params.pointing_file
    )
}