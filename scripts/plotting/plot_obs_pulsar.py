#! /usr/bin/env python

import os
import math
import argparse
import numpy as np
import csv
from scipy.interpolate import UnivariateSpline
from math import radians, degrees

#vcstools
from vcstools.beam_calc import get_beam_power_over_time
from vcstools.catalogue_utils import get_psrcat_ra_dec
from vcstools.pointing_utils import sex2deg, deg2sex
from vcstools.metadb_utils import find_obsids_meta_pages, get_common_obs_metadata

#matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib import rcParams

rcParams['font.family'] = 'monospace'
plt.rcParams["font.family"] = "monospace"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    A ploting script tha can be used to plot MWA tile beams, pulsars and used to work out the SMART observations to best cover the southern sky.

    # SMART survey update example
    plot_obs_pulsar.py --smart --contour --fwhm --shade 1221832280 --pulsar J2241-5236
    # All pulsars detected update
    plot_obs_pulsar.py --lines --pulsar_detected --pulsar_all
    """)
    obs_group = parser.add_argument_group('Observation Options')
    obs_group.add_argument('--obsid_list', type=str, nargs='*',
                           help='Instead of calculating which positions to use the script will use the input obsids. eg: "1088850560 1090249472"')
    obs_group.add_argument('--all_obsids', action='store_true',
                           help='Uses all VCS obsids on the MWA metadatabase.')
    obs_group.add_argument('--incoh', action='store_true',
                           help='Calculates the sensitivity assuming thast all observations are incoherent')
    obs_group.add_argument('--smart', action='store_true',
                           help='Cover the Southern sky with observations for the SMART survey.')

    obs_plot_group = parser.add_argument_group('Observation Plot Types Options')
    obs_plot_group.add_argument('-s', '--sens', action='store_true',
                                help='Plots sensitivity of each observation')
    obs_plot_group.add_argument('-o', '--overlap', action='store_true',
                                help='Plots sensitivity by summing the power of overlapping observations.')
    obs_plot_group.add_argument('-c', '--contour', action='store_true',
                                help='Plots the contour of each observations power')
    obs_plot_group.add_argument('-l', '--lines', action='store_true',
                                help='Includes the min decs of other telescopes in plots')

    add_group = parser.add_argument_group('Extra Plot Layers Options')
    add_group.add_argument('--pulsar', type=str, nargs='+',
                           help='A list of pulsar to mark on the plot.')
    add_group.add_argument('--pulsar_detected', action='store_true',
                           help='Plots all pulsars detected by the MWA and uploaded to the pulsar database.')
    add_group.add_argument('--pulsar_all', action='store_true',
                           help='Plots all known pulsars.')
    add_group.add_argument('--pulsar_discovered', action='store_true',
                           help='Plots all pulsars discovered with the MWA.')
    add_group.add_argument('--fill', action='store_true',
                           help='Shades the area the MWA can view.')
    add_group.add_argument('--shade', type=str, nargs='+',
                           help="Shades the chosen SMART observations in their group's colour")
    add_group.add_argument('--shade_dark', type=str, nargs='+',
                           help="Shades the chosen SMART observations in their group's colour in a darker shade")

    plot_group = parser.add_argument_group('Plotting Options')
    plot_group.add_argument('-f', '--fwhm', action='store_true',
                            help='if this options is used the FWHM of each pointing is used. If it is not chosen the FWHM of a zenith pointing is used.')
    plot_group.add_argument('-r', '--resolution', type=int, default=3,
                            help='The resolution in degrees of the final plot (must be an integer). Default = 1')
    plot_group.add_argument('--square', action='store_true',
                            help='Plots a square grid instead of aitoff.')
    plot_group.add_argument('-p', '--plot_type', type=str,
                            help='Determines the output plot type, Default="png".',default='png')
    plot_group.add_argument('--ra_offset', action='store_true',
                            help='Offsets the RA by 180 so that 0h is in the centre')
    args=parser.parse_args()

    if args.shade is None:
        args.shade = []
    if args.shade_dark is None:
        args.shade_dark = []

    #Setting up some of the plots
    fig = plt.figure(figsize=(6, 4))
    plt.rc("font", size=8)
    if args.square:
        fig.add_subplot(111)
        ax = plt.axes()
    else:
        fig.add_subplot(111)
        ax = plt.axes(projection='mollweide')

    SMART_metadata = [[0,  "B01", 1221399680, 330.4, -55.0],
                      [1,  "B11", 1224859816, 26.8, -40.5],
                      [2,  "B13", 1225462936, 26.7, -13.0],
                      [3,  "B12", 1225118240, 26.7, 18.3],
                      [4,  "B08", 1255444104, 10.3, 1.6],
                      [5,  "B07", 1226062160, 10.3, -26.7],
                      [6,  "R04", 1253991112, 59.6, -40.5],
                      [7,  "R06", 1255197408, 59.5, -13.0],
                      [8,  "R05", 1254594264, 59.5, 18.3],
                      [9,  "R01", 1252177744, 43.1, 1.6],
                      [10, "B09", 1224252736, 10.3, -55.0],
                      [11, "R02", 1252780888, 43.2, -26.7],
                      [12, "R07", 1255803168, 70.7, -72.0],
                      [13, "R11", 1258221008, 92.4, -40.5],
                      [14, "R13", 1259427304, 92.4, -13.0],
                      [15, "R12", 1259685792, 92.3, 18.3],
                      [16, "R08", 1256407632, 75.9, 1.6],
                      [17, "R09", 1257010784, 76.0, -26.7],
                      [18, "R03", 1253471952, 50.5, -55.0],
                      [19, "G03", 1265983624, 125.2, -40.5],
                      [20, "G05", 1266155952, 125.2, -13.0],
                      [21, "G04", 1265725128, 125.1, 18.3],
                      [22, "G01", 1260638120, 108.8, 1.6],
                      [23, "G02", 1261241272, 108.8, -26.7],
                      [24, "G07", 1266932744, 130.8, -72.0],
                      [25, "R10", 1257617424, 90.6, -55.0],
                      [26, "G10", 1266680784, 158.0, -40.5],
                      [27, "G12", 1267283936, 158.0, -13.0],
                      [28, "G11", 1267111608, 158.0, 18.3],
                      [29, "G08", 1264867416, 141.6, 1.6],
                      [30, "G09", 1265470568, 141.6, -26.7],
                      [31, "G06", 1266329600, 130.7, -55.0],
                      [32, "P03", 1301240224, 189.4, -72.0],
                      [33, "P04", 1301412552, 189.7, -40.5],
                      [34, "P01", 1300809400, 189.8, -13.0],
                      [35, "P02", 1300981728, 189.8, 18.3],
                      [36, "G14", 1268063336, 174.4, 1.6],
                      [37, "G15", 1268321832, 174.4, -26.7],
                      [38, "G13", 1267459328, 170.8, -55.0],
                      [39, "P10", 1302282040, 222.6, -40.5],
                      [40, "P08", 1302712864, 222.6, -13.0],
                      [41, "P09", 1302540536, 222.6, 18.3],
                      [42, "P06", 1301847296, 206.2, 1.6],
                      [43, "P05", 1301674968, 206.2, -26.7],
                      [44, "P13", 1303408712, 249.8, -72.0],
                      [45, "O03", 45, 255.4, -40.5],
                      [46, "O01", 46, 255.4, -13.0],
                      [47, "O02", 47, 255.5,  18.3],
                      [48, "P12", 1303233776, 239.0, 1.6],
                      [49, "P11", 1303061448, 239.0, -26.7],
                      [50, "P07", 1302106648, 209.8, -55.0],
                      [51, "O08", 51, 288.2, -40.5],
                      [52, "O06", 52, 288.2, -13.0],
                      [53, "O07", 53, 288.3, 18.3],
                      [54, "O05", 54, 271.8, 1.6],
                      [55, "O04", 55, 271.8, -26.7],
                      [56, "P14", 1303581040, 249.9, -55.0],
                      [57, "O12", 57, 310.0, -72.0],
                      [58, "O15", 58, 321.0, -40.5],
                      [59, "O13", 59, 321.1, -13.0],
                      [60, "O14", 60, 321.1, 18.3],
                      [61, "O11", 61, 304.7, 1.6],
                      [62, "O10", 62, 304.6, -26.7],
                      [63, "O09", 63, 290.0, -55.0],
                      [64, "B06", 1225713560, 353.9, -40.5],
                      [65, "B04", 1222697776, 353.9, -13.0],
                      [66, "B05", 1223042480, 353.9, 18.3],
                      [67, "B02", 1221832280, 337.5, 1.6],
                      [68, "B03", 1222435400, 337.5, -26.7],
                      [69, "B10", 1227009976, 10.6, -72.0]]

    #Working out the observations required -----------------------------------------------
    if args.all_obsids:
        observations = find_obsids_meta_pages(params={'mode':'VOLTAGE_START'})#,'cenchan':145})
    elif args.obsid_list:
        observations = args.obsid_list
    elif args.smart:
        observations = np.array(SMART_metadata)[:,2]
        # Load SMART nz data from file
        from mwa_search import data_load
        smart_nz = []
        with open(data_load.SMART_POWER_FILE, 'rb') as f:
            for i in range(70):
                smart_nz.append(np.load(f))
        args.resolution = 1
    else:
        print("No observation options selected. No observations will be plotted")
        observations = []
    pointing_count = len(observations)

    #setting up RA Dec ranges for power calculations
    res = args.resolution
    #x, y = np.meshgrid(np.arange(radians(-220),radians(220),radians(res)), np.arange(radians(-90),radians(90),radians(res)))
    #x, y = np.meshgrid(np.arange(radians(-180.),radians(180.5),radians(res)), np.arange(radians(-90.),radians(90.5),radians(res)))
    x, y = np.meshgrid(np.flip(np.arange(radians(-180.),radians(180.5),radians(res)), 0), np.arange(radians(-90.),radians(90.5),radians(res)))
    nx = x.flatten()
    ny = y.flatten()
    #print("nx[0]: {}".format(nx[0]))
    #print(nx.shape, ny.shape)



    # Sense array initialisation
    nz_sens_overlap = np.zeros(len(nx))
    #nz_sens = np.full(len(nx), 50.)
    nz_sens = np.zeros(len(nx))
    nz_sens[:] = np.nan

    # Set up default colours
    colors= ['0.5' for _ in range(50)] ; colors[0]= 'blue'
    linewidths= [0.4 for _ in range(50)] ; linewidths[0]= 1.0
    alpha = 0.5
    smart_colours = {'B': {'light': 'skyblue', 'dark': 'blue'},
                     'R': {'light': 'lightcoral', 'dark': 'red'},
                     'G': {'light': 'lightgreen', 'dark': 'green'},
                     'P': {'light': 'orchid', 'dark': 'purple'},
                     'O': {'light': 'orange', 'dark': 'darkorange'}}
    smart_colours_nzs = {'B': {'light': np.zeros(len(nx)), 'dark': np.zeros(len(nx))},
                         'R': {'light': np.zeros(len(nx)), 'dark': np.zeros(len(nx))},
                         'G': {'light': np.zeros(len(nx)), 'dark': np.zeros(len(nx))},
                         'P': {'light': np.zeros(len(nx)), 'dark': np.zeros(len(nx))},
                         'O': {'light': np.zeros(len(nx)), 'dark': np.zeros(len(nx))}}

    # make extra nx ny and nz for the dec edges that mollweid projection fucks up
    x_1, y_1 = np.meshgrid(np.arange(radians(-220.),radians(-179.5),radians(res)), np.arange(radians(-90.),radians(90.5),radians(res)))
    x_2, y_2 = np.meshgrid(np.arange(radians(181.),radians(220.5),radians(res)), np.arange(radians(-90.),radians(90.5),radians(res)))
    #print(x_1.flatten(), x_2.flatten())
    nx_blue = np.concatenate((x_1.flatten(), x_2.flatten()))
    ny_blue = np.concatenate((y_1.flatten(), y_2.flatten()))
    nz_blue = np.zeros(len(nx_blue))
    for i in range(len(nx_blue)):
        if radians(-45) < ny_blue[i] < radians(28):
            nz_blue[i] = 0.8

    # a little hack to save metadata to speed up repeated calls
    if args.obsid_list or args.all_obsids:
        common_meta_list = []
        if not os.path.exists('obs_meta.csv'):
            os.mknod('obs_meta.csv')
        with open('obs_meta.csv', 'r') as csvfile:
            spamreader = csv.reader(csvfile)
            next(spamreader, None)
            obsid_meta_file = []
            for row in spamreader:
                obsid_meta_file.append(row)
        for i, ob in enumerate(observations):
            obs_foun_check = False
            for omf in obsid_meta_file:
                if int(ob) == int(omf[0]):
                    print("getting obs metadata from obs_meta.csv")
                    ob, ra, dec, time, delays,centrefreq, channels = omf
                    ob = int(ob)
                    time = int(time)
                    delaysx = list(map(int,delays[2:-2].split("], [")[0].split(",")))
                    delaysy = list(map(int,delays[2:-2].split("], [")[1].split(",")))
                    delays = [delaysx, delaysx]
                    centrefreq = float(centrefreq)
                    channels = list(map(int,channels[1:-1].split(",")))
                    obs_foun_check = True
            if not obs_foun_check:
                print("Getting metadata for {}".format(ob))
                ob, ra, dec, time, delays,centrefreq, channels =\
                    get_common_obs_metadata(ob)

                with open('obs_meta.csv', 'a') as csvfile:
                    spamwriter = csv.writer(csvfile)
                    spamwriter.writerow([ob, ra, dec, time, delays,centrefreq, channels])
            common_meta_list.append([ob, ra, dec, time, delays,centrefreq, channels])

    #Loop over observations and calc beam power
    for i, ob in enumerate(observations):
        # Calculate power over sky
        if args.smart:
            nz = np.array(smart_nz[i])
            mnzi = np.argmax(nz)
            #print("i: {}  ra: {:6.1f}  dec: {:6.1f}".format(i, degrees(nx[mnzi])+180, degrees(ny[mnzi])))
            if args.fwhm:
                levels = np.arange(0.5*max(nz), max(nz), 0.5/6.)
            else:
                levels = np.arange(0.5, 1., 0.05)
            colors[0]= smart_colours[SMART_metadata[i][1][0]]['dark']
            # Populate colour nzs
            for colour in smart_colours.keys():
                if colour == SMART_metadata[i][1][0]:
                    # Same colour
                    if ob in args.shade:
                        for zi in range(len(nz)):
                            if nz[zi] >= levels[0]:
                                smart_colours_nzs[colour]['light'][zi] = nz[zi]
                    if ob in args.shade_dark:
                        for zi in range(len(nz)):
                            if nz[zi] >= levels[0]:
                                smart_colours_nzs[colour]['dark'][zi] = nz[zi]
        else:
            print("Calculating obs {0}/{1}".format(i + 1, len(observations)))
            ob, ra, dec, time, delays, centrefreq, channels = common_meta_list[i]
            z = []; z_sens = []

            #print(max(Dec), min(RA), Dec.dtype)
            time_intervals = 600 # seconds
            names_ra_dec = np.column_stack((['source']*len(nx), np.degrees(nx), np.degrees(ny)))
            powout = get_beam_power_over_time(common_meta_list[i], names_ra_dec, dt=time_intervals, degrees = True)

            for c in range(len(nx)):
                temppower = 0.
                temppower_sense = 0.
                for t in range(powout.shape[1]):
                    power_ra = powout[c,t,0]
                    temppower_sense += power_ra #average power kinds
                    nz_sens_overlap[c] += power_ra * math.cos(ny[c])
                    if power_ra > temppower:
                        temppower = power_ra
                z_sens.append(temppower_sense)
                z.append(temppower)

            nz=np.array(z)

            #calculates sensitiviy and removes zeros -------------------------
            nz_sense_obs = []
            for zsi in range(len(z_sens)):
                if nz[zsi] < 0.001:
                    nz_sense_obs.append(np.nan)
                else:
                    nz_sense_obs.append(4.96/np.sqrt(z_sens[zsi]))

            for zi, zs in enumerate(nz_sense_obs):
                if math.isnan(nz_sens[zi]):
                    nz_sens[zi] = zs
                elif nz_sens[zi] > zs:
                    #append if larger
                    nz_sens[zi] = zs

        # plot contours ---------------------------------------
        if args.contour:
            #print("plotting colour {}".format(colors[0]))
            #print(nx.shape, ny.shape, nz.shape)
            plt.tricontour(nx, ny, nz, levels=[levels[0]], alpha = 0.6,
                           colors=colors,
                           linewidths=linewidths)
        # Label plots with id labels for debugging
        #ra_text = radians(180-(SMART_metadata[i][3]+10))
        #dec_text = radians(SMART_metadata[i][4])
        #ax.text(ra_text, dec_text, str(i), fontsize=12, ha='center', va='center')


    # plot sens -------------------------------------------------------
    if args.sens:
        if args.overlap:
            for zi in range(len(nz)):
                if nz_sens_overlap[zi] < 0.5:
                    nz_sens_overlap[zi] = np.nan
            nz = 1.5*4.96/np.sqrt(nz_sens_overlap)
            #nz = nz_sens_overlap
        else:
            if args.incoh:
                nz = nz_sens * 11.3 #(sqrt128)
            else:
                nz = nz_sens

        nx.shape = (len(map_dec_range),len(map_ra_range))
        ny.shape = (len(map_dec_range),len(map_ra_range))
        nz.shape = (len(map_dec_range),len(map_ra_range))
        if args.ra_offset:
            roll_by = len(map_ra_range)//2
            nx = np.roll(nx, roll_by)
            ny = np.roll(ny, roll_by)
            nz = np.roll(nz, roll_by)
        dec_limit_mask = ny > np.radians(63.3)
        nz[dec_limit_mask] = np.nan
        import matplotlib.colors as colors
        colour_map = 'plasma_r'
        if args.incoh:
            plt.pcolor(nx, ny, nz, cmap=colour_map, vmin=20, vmax=90)
        else:
            plt.pcolor(nx, ny, nz, cmap=colour_map, vmin=2., vmax=10.)
        plt.colorbar(spacing='uniform', shrink = 0.65, #ticks=[2., 10., 20., 30., 40., 50.],
                     label=r"Detection Sensitivity, 10$\sigma$ (mJy)")

    #Add extra plot layers ---------------------------------------

    # Shades only the selected colour
    if (args.shade or args.shade_dark) and args.smart:
        for colour in smart_colours.keys():
            if len(args.shade) > 1:
                nz = smart_colours_nzs[colour]['light']
                if colour == 'B':
                    print(max(nz))
                    cs = plt.tricontour(np.concatenate((nx, nx_blue)),
                                        np.concatenate((ny, ny_blue)),
                                        np.concatenate((nz, nz_blue)), levels=[levels[0]], alpha=0.0)
                else:
                    cs = plt.tricontour(nx, ny, nz, levels=[levels[0]], alpha=0.0)
                cs0 = cs.collections[0]
                cspaths = cs0.get_paths()
                for cspath in cspaths:
                    spch_0 = patches.PathPatch(cspath, facecolor=smart_colours[colour]['light'],
                                            edgecolor='gray',lw=0.5, alpha=0.6)
                    ax.add_patch(spch_0)

            if len(args.shade_dark) > 1:
                nz = smart_colours_nzs[colour]['dark']
                if colour == 'B':
                    cs = plt.tricontour(np.concatenate((nx, nx_blue)),
                                        np.concatenate((ny, ny_blue)),
                                        np.concatenate((nz, nz_blue)), levels=[levels[0]], alpha=0.0)
                else:
                    cs = plt.tricontour(nx, ny, nz, levels=[levels[0]], alpha=0.0)
                cs0 = cs.collections[0]
                cspaths = cs0.get_paths()
                for cspath in cspaths:
                    spch_0 = patches.PathPatch(cspath, facecolor=smart_colours[colour]['dark'],
                                            edgecolor='gray',lw=0.5, alpha=0.8)
                    ax.add_patch(spch_0)

    #add lines of other surveys
    if args.lines:
        x_line = np.arange(radians(-180.),radians(180.5),radians(res))
        plt.plot(x_line,
                 np.full(len(x_line),radians(30.)),
                 #'r',  label=r'MWA   ( 80 -    300 MHz)', zorder=130)
                 'r',  label=r'MWA', zorder=130)
        plt.plot(x_line,
                 np.full(len(x_line),radians(0.)),
                 #'--m',label=r'LOFAR ( 10 -    240 MHz)', zorder=130)
                 '--m',label=r'LOFAR', zorder=130)
        plt.plot(x_line,
                 np.full(len(x_line),radians(-40.)),
                 #'--g',label=r'GBT   (290 - 49,800 MHz)', zorder=130)
                 '--g',label=r'GBT', zorder=130)
        plt.plot(x_line,
                 np.full(len(x_line),radians(-55.)),
                 linestyle='--', color='orange',
                 #label=r'GMRT  ( 50 -  1,500 MHz)', zorder=130)
                 label=r'GMRT', zorder=130)

        #handles, labels = ax.get_legend_handles_labels()
                #plt.legend(bbox_to_anchor=(0.8, 0.85,0.5,0.2), loc='best', numpoints=1,
        #           ncol=1, mode="expand", borderaxespad=0., fontsize=8)
        plt.legend(loc='upper right', bbox_to_anchor=(1.05, 1.05),
                   fontsize=7, framealpha=.95)
        #plt.legend(loc='upper left', fontsize=8)

    if args.fill:
        import matplotlib.transforms as mtransforms
        trans = mtransforms.blended_transform_factory(ax.transData, ax.transAxes)
        map_ra_range = range(-80,481,res)
        ff = 30.
        ffa = 28.5
        ax.fill_between(np.array(map_ra_range)/180.*np.pi + -np.pi,
                        np.full(len(map_ra_range),np.radians((-100)/90.*ff+ffa)),
                        np.full(len(map_ra_range),np.radians((34.5)/90.*ff+ffa)),
                        facecolor='0.5', alpha=0.5, transform=trans)


    # Add pulsars to plot
    if args.pulsar_all:
        #add all pulsars on the antf catalogue
        ra_PCAT = []
        dec_PCAT = []
        pulsar_list = get_psrcat_ra_dec()
        for pulsar in pulsar_list:
            ra_temp, dec_temp = sex2deg(pulsar[1], pulsar[2])
            if args.ra_offset:
                if ra_temp > 180.:
                    ra_temp -= 180.
                else:
                    ra_temp += 180.
            if args.square:
                ra_PCAT.append(ra_temp)
                dec_PCAT.append(dec_temp)
            else:
                ra_PCAT.append(-(ra_temp-180.)/180.*np.pi)
                dec_PCAT.append(dec_temp/180.*np.pi)
        #print(min(ra_PCAT), max(ra_PCAT))
        ax.scatter(ra_PCAT, dec_PCAT, s=0.2, color ='b', zorder=1)

    if args.pulsar_detected:
        #add some pulsars
        ra_PCAT = []
        dec_PCAT = []
        from mwa_pulsar_client import client
        auth = (os.environ['MWA_PULSAR_DB_USER'],os.environ['MWA_PULSAR_DB_PASS'])
        pulsar_list_dict = client.pulsar_list('https://pulsar-cat.icrar.uwa.edu.au/', auth)
        pulsar_list = []
        for pulsar in pulsar_list_dict:
            pulsar_list.append(pulsar[u'name'])
        pulsar_pos_list = get_psrcat_ra_dec(pulsar_list=pulsar_list)
        for pulsar in pulsar_pos_list:
            ra_temp, dec_temp = sex2deg(pulsar[1], pulsar[2])
            if args.ra_offset:
                if ra_temp > 180:
                    ra_PCAT.append(-ra_temp/180.*np.pi+2*np.pi)
                else:
                    ra_PCAT.append(-ra_temp/180.*np.pi)
            else:
                ra_PCAT.append(-ra_temp/180.*np.pi+np.pi)
            dec_PCAT.append(dec_temp/180.*np.pi)
        ax.scatter(ra_PCAT, dec_PCAT, s=5, color ='r', zorder=100)

    if args.pulsar:
        #add some pulsars
        ra_PCAT = []
        dec_PCAT = []
        print("{} input pulsars".format(len(args.pulsar)))
        raw_pulsar_list = list(dict.fromkeys(args.pulsar))
        print("{} distinct pulsars".format(len(raw_pulsar_list)))
        pulsar_list = get_psrcat_ra_dec(pulsar_list=raw_pulsar_list)
        for pulsar in pulsar_list:
            ra_temp, dec_temp = sex2deg(pulsar[1], pulsar[2])
            if args.ra_offset:
                if ra_temp > 180:
                    ra_PCAT.append(-ra_temp/180.*np.pi+2*np.pi)
                else:
                    ra_PCAT.append(-ra_temp/180.*np.pi)
            else:
                #ra_PCAT.append(-ra_temp/180.*np.pi+np.pi)
                ra_PCAT.append(radians(180-ra_temp))
            dec_PCAT.append(radians(dec_temp))
        ax.scatter(ra_PCAT, dec_PCAT, s=5, color ='r', zorder=100)

    if args.pulsar_discovered:
        #add some pulsars
        ra_PCAT = []
        dec_PCAT = []
        pulsar_list = [["J0036-1033", "00:36:14.58", "-10:33:16.40"]]
        for pulsar in pulsar_list:
            ra_temp, dec_temp = sex2deg(pulsar[1], pulsar[2])
            if args.ra_offset:
                if ra_temp > 180:
                    ra_PCAT.append(-ra_temp/180.*np.pi+2*np.pi)
                else:
                    ra_PCAT.append(-ra_temp/180.*np.pi)
            else:
                ra_PCAT.append(-ra_temp/180.*np.pi+np.pi)
            dec_PCAT.append(dec_temp/180.*np.pi)
        ax.scatter(ra_PCAT, dec_PCAT, s=10, color ='r', zorder=100)

    plt.xlabel("Right Ascension")
    plt.ylabel("Declination")

    #xtick_labels = ['0h','2h','4h','6h','8h','10h','12h','14h','16h','18h','20h','22h']
    if args.ra_offset:
        xtick_labels = ['10h', '8h', '6h', '4h', '2h', '0h', '22h', '20h', '18h', '16h', '14h']
        xticks_moll = [150., 120., 90., 60., 30., 0., 330., 300., 270., 240., 210. ]
    else:
        xtick_labels = [ '22h', '20h', '18h', '16h', '14h','12h','10h', '8h', '6h', '4h', '2h']
        xticks_moll = [330., 300., 270., 240., 210., 180., 150., 120., 90., 60., 30.]
    xticks_square = np.radians(180 - np.array(xticks_moll))

    if args.square:
        plt.xticks(xticks_square, tuple(xtick_labels))
        ytick_labels = [-75, -60, -45, -30, -15, 0, 15, 30, 45, 60, 75]
        plt.yticks(np.radians(np.array(ytick_labels)), tuple(ytick_labels))
    else:
        ax.set_xticklabels(xtick_labels, zorder=150)
    print("plotting grid")
    plt.grid(True, color='gray', lw=0.5, linestyle='dotted')


    # Creates a plot name --------------------------
    plot_name = 'mwa_obs_n{}_res{}'.format(pointing_count, res)

    if args.sens:
        plot_name += '_sens'
    if args.contour:
        plot_name += '_contour'
    if args.lines:
        plot_name += '_minlines'
    if args.obsid_list:
        plot_name += '_obslist'
    if args.incoh:
        plot_name += '_incoh'
    if args.fill:
        plot_name += '_fill'
    if args.pulsar:
        plot_name += '_pulsar_n{}'.format(len(raw_pulsar_list))
    if args.pulsar_detected:
        plot_name += '_pulsar_detected'
    if args.pulsar_discovered:
        plot_name += '_pulsar_discovered'
    plot_type = args.plot_type
    #plt.title(plot_name)
    print("saving {}.{}".format(plot_name, plot_type))
    fig.savefig(plot_name + '.' + plot_type, format=plot_type, dpi=1000, bbox_inches='tight')
    #plt.show()

