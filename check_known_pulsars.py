#! /usr/bin/env python3

import argparse
import find_pulsar_in_obs as fpio
import os
import glob
import subprocess
import numpy as np

import mwa_search_pipeline as search_pipe
from mwa_metadb_utils import get_common_obs_metadata as get_meta
import config
import file_maxmin

def beamform_and_fold(obsid, DI_dir, all_check, cal_obs, args, 
                      vdif_check=False, product_dir='/group/mwaops/vcs'):
    
    
    obsbeg, obsend, obsdur = file_maxmin.print_minmax(obsid)

    #wrapping for find_pulsar_in_obs.py
    names_ra_dec = np.array(fpio.grab_source_alog(max_dm=250))
    fpio.find_sources_in_obs([obsid], names_ra_dec, dt=100)
    known_pulsar_file = "{0}_analytic_beam.txt".format(obsid)

    meta_data = get_meta(obsid)
    channels = meta_data[-1]
            
    if all_check:
        #looks through the comined files to use the max and min
        #TODO have some sort of check to look for gaps
        if glob.glob("{0}/{1}/combined/{1}*_ics.dat".format(product_dir, obsid)):
            combined_files = glob.glob("{0}/{1}/combined/{1}*_ics.dat".format(product_dir, obsid))
        else:
            combined_files = glob.glob("{0}/{1}/combined/{1}*_ch{2}.dat".\
                                       format(product_dir, obsid, channels[-1]))
        comb_times = []
        for comb in combined_files:
            comb_times.append(int(comb.split("_")[1]))
        psrbeg = min(comb_times)
        psrend = max(comb_times)
    
    #looping through each puslar
    
    with open(known_pulsar_file, 'r') as kpf:
        pulsar_lines = []
        for pulsar_line in kpf:
            pulsar_lines.append(pulsar_line)
    
    for pulsar_line in pulsar_lines:
        if pulsar_line.startswith("J"):
            PSRJ = pulsar_line.split()[0]
            if len(PSRJ) < 11 or PSRJ[-1] == 'A' or PSRJ[-2:] == 'aa':
                inpc = float(pulsar_line.split()[1])
                otpc = float(pulsar_line.split()[2])
                if not all_check:
                    psrbeg = int(obsbeg+obsdur*inpc)
                    psrend = int(obsbeg+obsdur*otpc)
                temp = fpio.get_psrcat_ra_dec(pulsar_list=[PSRJ])
                temp = fpio.format_ra_dec(temp, ra_col = 1, dec_col = 2)
                jname, raj, decj = temp[0]
                #get pulsar period
                cmd = ['psrcat', '-c', 'p0', jname]
                output = subprocess.Popen(cmd,stdout=subprocess.PIPE).communicate()[0]
                period = output.split(b'\n')[4].split()[1] #in s
                print(PSRJ, raj, decj, period, psrbeg, psrend)
                fits_dir = '{0}/{1}/pointings/{2}_{3}/'.format(product_dir, obsid, raj, decj)
                if PSRJ[-1] == b'A' or PSRJ[-2:] == b'aa':
                    #Got to find all the pulsar J names with other letters
                    vdif_check = True
                    jname_list = []
                    for pulsar_l in pulsar_lines:
                        if pulsar_l.startswith(PSRJ[:-2]):
                            jname_list.append(pulsar_l.split()[0])
                else:
                    jname_list = [jname]
                    if b'*' in period:
                        continue
                    if float(period) < .05 :
                        vdif_check = True
                search_pipe.beamform(["{0} {1}".format(raj,decj)], obsid, psrbeg, psrend,
                                     DI_dir, vdif=vdif_check,
                                     args=args, pulsar_check=jname_list, cal_id=cal_obs,
                                     channels=channels)
    os.remove(known_pulsar_file)
    return



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Beamforms, folds on all known pulsars for an observation. If a pulsar is detected it's uploaded to the MWA Pulsar Database.

    Based on a script written by Mengyao Xue.
    """)
    parser.add_argument('-o','--obsid',type=str,help='The observation ID of the fits file to be searched')
    parser.add_argument("--DI_dir", default=None, help="Directory containing either Direction Independent Jones Matrices (as created by the RTS) or calibration_solution.bin as created by Andre Offringa's tools.[no default]")
    parser.add_argument('-O','--cal_obs', type=int, help="Observation ID of calibrator you want to process.", default=None)
    parser.add_argument("-a", "--all", action="store_true",  help="Uses all of the combined data available. If the options isn't used it'll only use the start and stops times that are recommened by find_pulsar_in_obs.py.")
    parser.add_argument("-v", "--vdif", action="store_true",  help="Create vdif files for all pulsars. Default is to only create vdif files for pulsar with a period shorter than 50 ms.")
    args=parser.parse_args()
    
    #option parsing
    if not args.obsid:
        print("Please input observation id by setting -o or --obsid. Exiting")
        quit()
    
    if not args.cal_obs:
        print("Please input calibration observation id by setting -O or --cal_obs. Exiting")
        quit()

    comp_config = config.load_config_file()
    if not args.DI_dir:
        args.DI_dir = "{0}/{1}/cal/{2}/rts/".format(comp_config['base_product_dir'],
                                                    args.obsid, args.cal_obs)
        print("No DI_dir given so assuming {0} is the directory".format(args.DI_dir))
    
    beamform_and_fold(args.obsid, args.DI_dir, args.all, args.cal_obs, args, 
                      vdif_check=args.vdif, product_dir=comp_config['base_product_dir'])

