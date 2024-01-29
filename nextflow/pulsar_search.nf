#!/usr/bin/env nextflow


params.help = false
if ( params.help ) {
    help = """pulsar_search.nf: A pipeline perform a pulsar search on a single input fits file.
             |                  The fits files must be in the format
             |                  <obsid>_<pointing>_ch<min_chan>-<max_chan>_00??.fits
             |Required argurments:
             |  --fits_dir The fits file to search [no default]
             |
             |Dedispersion arguments (optional):
             |  --dm_min    Minimum DM to search over [default: ${params.dm_min}]
             |  --dm_max    Maximum DM to search over [default: ${params.dm_max}]
             |  --dm_min_step
             |              Minimum DM step size (Delta DM) [default: ${params.dm_min_step}]
             |  --dm_max_step
             |              Maximum DM step size (Delta DM) [default: ${params.dm_max_step}]
             |  --max_dms_per_job
             |              Maximum number of DM steps a single job will process.
             |              Lowering this will reduce memory usage and increase parellelisation.
             |              [default: ${params.max_dms_per_job}]
             |
             |Pulsar search arguments (optional):
             |  --sp        Perform only a single pulse search [default: ${params.sp }]
             |  --cand      Label given to output files [default: ${params.cand }]
             |  --nharm     Number of harmonics to search [default: ${params.nharm }]
             |  --min_period
             |              Min period to search for in sec (ANTF min = 0.0013)
             |              [default: ${params.min_period }]
             |  --max_period
             |              Max period to search for in sec (ANTF max = 23.5)
             |              [default: ${params.max_period }]
             |  --zmax      Maximum acceleration to search (0 will do a simpler periodic search).
             |              I recomend you use 200 and set --max_dms_per_job 32
             |              [default: ${params.zmax }]
             |
             |Optional arguments:
             |  --cand      Candidate name to do a targeted search [default: Blind]
             |  --out_dir   Output directory for the candidates files
             |              [default: ${params.search_dir}/<obsid>_candidates]
             |  --mwa_search_version
             |              The mwa_search module bersion to use [default: master]
             |  -w          The Nextflow work directory. Delete the directory once the processs
             |              is finished [default: ${workDir}]""".stripMargin()
    println(help)
    exit(0)
}

workflow {
    if ( params.sp ) {
        single_pulse_search( params.fits_dir )
    }
    else {
        pulsar_search( params.fits_dir } )
        classifier( pulsar_search.out )
    }
}
