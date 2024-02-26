import math
import numpy as np
from matplotlib import use
use('Agg')
import matplotlib.pyplot as plt

def plot_sensitivity(DD_plan_array, time, centrefreq, freqres, bandwidth):
    base_sensitivity = 3 #mJy. This could be done properly but this will for now
    # adjust for time
    base_sensitivity = base_sensitivity * math.sqrt(4800) / math.sqrt(time)
    # adjust for unscattered pulsar
    #base_sensitivity = base_sensitivity / math.sqrt( ( 1. - 0.05) / 0.05 )

    # period to work with
    periods = np.array([ 1., 0.1, 0.01, 0.001 ])*1000.
    widths = periods*0.05

    #fig, ax = plt.subplots(1, 1)
    plt.subplots(1, 1)
    #ax.set_ylim(0, 100)
    #plt.ylim(0, 100)

    for period, width in zip(periods, widths):
        sensitivties = []
        DMs = []
        for dm_row in DD_plan_array:
            DM_start, D_DM, DM_step, _, timeres, _, _ = dm_row
            for DM in np.arange(DM_start, D_DM, DM_step):
                # For each DM you're going to search do a sensitivy cal based on how
                # a pulse will get

                #Dm smear over a frequency channel
                dm_smear = DM * freqres * 8.3 * 10.**6 / centrefreq**3
                #Dm smear due to maximum incorrect DM
                dm_step_smear = 8.3 * 10.**6 * DM_step / 2. * bandwidth / centrefreq**3

                effective_width = math.sqrt(width**2 + dm_smear**2 + dm_step_smear**2 + timeres**2)
                #print(effective_width)
                #sensitivity given new effectiv width
                if effective_width >= period:
                    sensitivties.append(1000.)
                else:
                    sensitivties.append(base_sensitivity /
                                    math.sqrt( ( period - effective_width) / effective_width ) *
                                    math.sqrt( ( period - width) / width ))
                DMs.append(DM)
        plt.plot(DMs, sensitivties, label="P={0} ms".format(period))
    #plt.yscale('log')
    plt.legend()
    plt.yscale('log')
    plt.xscale('log')
    plt.ylabel(r"Detection Sensitivity, 10$\sigma$ (mJy)")
    plt.xlabel(r"Dispersion measure (pc cm$^{-3}$ ")
    plt.title("Sensitivy using a minimum DM step size of {0}".format(DD_plan_array[0][2]))
    #plt.show()
    plt.savefig("DM_step_sens_mDMs_{0}.png".format(DD_plan_array[0][2]))


def calc_nsub(centrefreq, bandwidth, nfreqchan, ref_freq_frac, lo_dm, hi_dm, timeres, smear_fact, start_nsub=3,
              max_nsub=3072):
    ref_freq = (centrefreq + 0.5 * bandwidth) - (ref_freq_frac * bandwidth)
    # work out how many subbands to use based on the dm smear over a subband
    nsub = start_nsub
    # find subband number at ref_freq
    freqs = np.arange(centrefreq + 0.5 * bandwidth, centrefreq - 0.5 * bandwidth, -bandwidth / nsub)[:-1]
    subband_no = np.argmin(np.abs(freqs - ref_freq))
    subband_top = freqs[subband_no]
    if subband_no + 1 == nsub:
        subband_bottom = centrefreq - 0.5 * bandwidth
    else:
        subband_bottom = freqs[subband_no + 1]
    dm_smear = 0.5 * (hi_dm - lo_dm) * 4.15 * 10. ** 6 * np.abs(subband_top ** -2 - subband_bottom ** -2)
    while dm_smear > timeres * smear_fact:
        nsub *= 2.
        freqs = np.arange(centrefreq + 0.5 * bandwidth, centrefreq - 0.5 * bandwidth, -bandwidth / nsub)[:-1]
        subband_no = np.argmin(np.abs(freqs - ref_freq))
        subband_top = freqs[subband_no]
        if subband_no + 1 == nsub:
            subband_bottom = centrefreq - 0.5 * bandwidth
        else:
            subband_bottom = freqs[subband_no + 1]
        dm_smear = 0.5 * (hi_dm - lo_dm) * 4.15 * 10. ** 6 * np.abs(subband_top ** -2 - subband_bottom ** -2)
        if nsub > max_nsub:
            break
    if nsub > max_nsub:
        nsub = max_nsub
    return int(nsub)


def dd_plan(
        centrefreq=154.24,
        bandwidth=30.72,
        nfreqchan=3072,
        start_timeres=0.1,
        lowdm=0,
        highdm=500,
        smear_ref_freq_frac=0.2,
        nsub_ref_freq_frac=0.8,
        smear_fact=2,
        nsub_smear_fact=2,
        min_dm_step=0.01,
        max_dm_step=500.0,
        max_dms_per_job=288,
        max_nsub=3072,
):
    """
    Work out the dedisperion plan

    Parameters
    ----------
    centrefreq: float
        The center frequency of the observation in MHz
    bandwidth: float
        The bandwidth of the observation in MHz
    nfreqchan: int
        The number of frequency channels
    start_timeres: float
        The time resolution of the observation in ms
    lowdm: float
        The lowest dispersion measure
    highdm: float
        The highest dispersion measure
    smear_ref_freq_frac
        The reference frequency as a fraction of the bandwidth to
        calculate the amount of smearing. 0 is the top of the band
        and 1 is the bottom of the band.
    nsub_ref_freq_frac
        The reference frequency as a fraction of the bandwidth to
        calculate the smearing caused by the difference in the
        central DM of a prepsubband step and the low/high DM of the
        step. 0 is the top of the band and 1 is the bottom of the band.
    smear_fact: int
        Number of time samples smeared over before moving to next DM step
    nsub_smear_fact: int
        Acceptable number of time samples smeared out when computing the
        the acceptable lowest nsub value.
    min_dm_step: float
        Will overwrite the minimum DM step with this value
    max_dm_step: float
        Will overwrite the maximum DM step with this value
    max_dms_per_job: int
        If Nsteps is greater than this value split it into multiple lines

    Returns
    -------
    DD_plan_array: list list
        dedispersion plan format:
        [[low_DM, high_DM, DM_step, nDM_step, timeres, downsample, nsub ]]
    """

    print(centrefreq)
    DD_plan_array = []
    freqres = bandwidth / float(nfreqchan)
    previous_DM = lowdm

    # number of time samples smeared over before moving to next D_dm

    # Loop until you've made a hit your range max
    D_DM = 0.
    timeres = start_timeres
    downsample = 1
    while D_DM < round(highdm, 2):
        change_ds = True
        ref_freq = (centrefreq + 0.5 * bandwidth) - (smear_ref_freq_frac * bandwidth)
        # calculate the DM where the current time resolution equals the
        # dispersion in a frequency channel (a bit of an overkill)

        # Dm smear over a frequency channel
        dm_smear = previous_DM * freqres * 8.3 * 10. ** 6 / ref_freq ** 3
        total_smear = math.sqrt(timeres ** 2 + dm_smear ** 2)

        D_DM = smear_fact * timeres * ref_freq ** 3 / \
               (8.3 * 10. ** 6 * freqres)
        print(D_DM)

        # difference in DM that will double the effective width (eq 6.4 of pulsar handbook)
        # TODO make this more robust
        # DM_step = math.sqrt( (2.*timeres)**2 - timeres**2 )/\
        #          (8.3 * 10**6 * bandwidth / centrefreq**3)
        DM_step = smear_fact * total_smear / \
                  (4.15 * 10. ** 6 * np.abs(
                      (centrefreq + 0.5 * bandwidth) ** -2 - (centrefreq - 0.5 * bandwidth) ** -2))

        # round to nearest 0.01
        DM_step = round(DM_step, 2)
        if DM_step < min_dm_step:
            # set DM to 0.01 as a zero DM doesn't make sense
            DM_step = min_dm_step
        if DM_step > max_dm_step:
            DM_step = max_dm_step
        # check useful timeres for the new DM step
        new_total_smear = DM_step / smear_fact * \
                          (4.15 * 10. ** 6 * np.abs(
                              (centrefreq + 0.5 * bandwidth) ** -2 - (centrefreq - 0.5 * bandwidth) ** -2))
        new_timeres = np.sqrt(new_total_smear ** 2 - dm_smear ** 2)
        while new_timeres // timeres > 1:
            timeres *= 2
            downsample *= 2
            change_ds = False

        if D_DM > highdm:
            # last one so range from to max
            D_DM = highdm
        # range from last to new
        D_DM = round(D_DM, 2)
        nDM_step = int((D_DM - previous_DM) / DM_step)
        if nDM_step % max_dms_per_job != 0:
            extra_steps = max_dms_per_job - (nDM_step % max_dms_per_job)
            nDM_step += extra_steps
            D_DM += extra_steps * DM_step
            D_DM = round(D_DM, 2)
        if nDM_step % 2 != 0:
            nDM_step -= 1
        total_work_factor = nDM_step / downsample
        if D_DM > lowdm:
            nsub = calc_nsub(
                centrefreq,
                bandwidth,
                nfreqchan,
                nsub_ref_freq_frac,
                previous_DM,
                D_DM,
                timeres * downsample,
                nsub_smear_fact,
                max_nsub=max_nsub
            )
            # if downsample > 16:
            #    DD_plan_array.append([ previous_DM, D_DM, DM_step, nDM_step, timeres, 16, nsub, total_work_factor ])
            # else:
            DD_plan_array.append([previous_DM, D_DM, DM_step, nDM_step, timeres, downsample, nsub, total_work_factor])

            previous_DM = D_DM

        # Double time res to account for incoherent dedispersion
        if change_ds:
            timeres *= 2.
            downsample *= 2
            while 10000 % downsample != 0:
                downsample += 1
                timeres += start_timeres

    # Check no lines have more Nsteps than max_dms_per_job
    new_DD_plan_array = []
    for dd_line in DD_plan_array:
        new_dd_lines = []
        dm_min, dm_max, dm_step, ndm, timeres, downsamp, nsub, total_work_factor = dd_line
        n_jobs_left = ndm
        new_dm_min = round(dm_min, 2)
        while n_jobs_left > 0:
            n_jobs = np.min([n_jobs_left, max_dms_per_job])
            # previous_DM, D_DM, DM_step, nDM_step, timeres, downsample, nsub
            new_nsub = calc_nsub(
                centrefreq,
                bandwidth,
                nfreqchan,
                nsub_ref_freq_frac,
                new_dm_min,
                new_dm_min + dm_step * n_jobs,
                timeres,
                nsub_smear_fact,
                max_nsub=max_nsub
            )
            new_dd_lines.append([
                new_dm_min,
                round(new_dm_min + dm_step * n_jobs, 2),
                dm_step,
                n_jobs,
                int(timeres),
                downsamp,
                new_nsub,
                total_work_factor
            ])
            new_dm_min = new_dm_min + dm_step * n_jobs
            new_dm_min = round(new_dm_min, 2)
            # dd_line = [
            #    dm_min + dm_step * n_jobs,
            #    dm_max,
            #    dm_step,
            #    ndm - n_jobs,
            #    timeres,
            #    downsamp,
            #    nsub,
            #    total_work_factor
            # ]
            n_jobs_left -= max_dms_per_job
        # new_dd_lines.append(dd_line)
        for n_line in new_dd_lines:
            new_DD_plan_array.append(n_line)

    return new_DD_plan_array
