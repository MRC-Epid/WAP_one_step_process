# WAVEFORM ACCELERATION PROCESSING STANDALONE SCRIPT (STANDALONE):
# Copyright (C) 2019  MRC Epidemiology Unit, University of Cambridge
#   
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.
#   
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#   
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

version = "WAP_one_step_process v.1.0 04/10/2019"

###############################################################################################################

import numpy as np
from datetime import datetime, timedelta
import time
import sys, os
from pampro import data_loading, diagnostics, Time_Series, Channel, channel_inference, Bout, Bout_Collection, batch_processing, triaxial_calibration, time_utilities, pampro_utilities, pampro_fourier
from collections import OrderedDict
import collections, re, copy
import pandas as pd

################################################################################################################
# SETTINGS
################################################################################################################

# FOLDER & FILE SETTINGS
monitor_type = "Axivity"        # monitor type ("GeneActiv" or "Axivity")
job_file = ""                   # filepath (including filename) of job file
results_folder = ""             # location for results, plots and metadata files

# PROCESSING SETTINGS
epoch_minutes = [1, 60]     # Give a list of epochs required - in minutes
epoch_plot = [1]            # a list of epochs(in minutes) to plot (EACH MUST BE CONTAINED IN LIST ABOVE)
                            # if no plots required leave empty '[]'

processing_epoch = 5            # processing epoch, in seconds
noise_cutoff_mg = 13            # noise threshold in mg

###########################################################################################
# STATISTICS & THRESHOLDS
# This section may be altered if different intensity thresholds or statistics are required.
###########################################################################################

# Threshold definition - This will be used with ENMO & HPFVM to create intensity categories
# The numbers in the brackets stand for: (start point, end point, increments to increase for next variable)
list1 = list(range(0, 5, 1))                   # [0,99999],[1,99999]
list2 = list(range(5, 150, 5))                 # [5,99999],[10,99999]
list3 = list(range(150, 300, 10))              # [100,99999],[110,99999]
list4 = list(range(300, 1000, 100))            # [300,99999],[400,99999]
list5 = list(range(1000, 5000, 1000))          # [1000,99999],[2000,99999]
vals = list1 + list2 + list3 + list4 + list5   # Collection of all the cutpoints above

# Define the desired statistics.
stats = OrderedDict()
stats["ENMO"] = [("generic", ["mean", "n", "missing", "sum"]), ("cutpoints", [[l,99999] for l in vals])]
stats["HPFVM"] = [("generic", ["mean", "n", "missing", "sum"]), ("cutpoints", [[l,99999] for l in vals])]
stats["PITCH"] = [("generic", ["mean", "std", "min", "max"]), ("cutpoints", [[p,p+5] for p in range(-90,90,5)])]
stats["ROLL"] = [("generic", ["mean", "std", "min", "max"]), ("cutpoints", [[p,p+5] for p in range(-90,90,5)])]
stats["Temperature"] = [("generic", ["mean"])]
stats["Battery"] = [("generic", ["mean"])]
stats["Integrity"] = [("generic", ["sum"])]

anomaly_types = ["A", "B", "C", "D", "E", "F", "G"]     # A list of known anomaly types identified by pampro

GA_battery_max = 4.3            # maximum value of GeneActiv battery, used to find percentage charged
AX_battery_max = 210            # maximum value of Axivity battery, used to find percentage charged

plotting_dict = {"ENMO_sum": "{}_{}_ENMO_sum.png",
                "HPFVM_sum": "{}_{}_HPFVM_sum.png",
                "PITCH_mean": "{}_{}_PITCH_mean.png",
                "ROLL_mean": "{}_{}_ROLL_mean.png",
                "Temperature_mean": "{}_{}_Temperature_mean.png",
                "Battery_mean": "{}_{}_Battery_mean.png"}

##################################################################################################################
# SCRIPT BEGINS BELOW
##################################################################################################################

job_num = int(sys.argv[1])
num_jobs = int(sys.argv[2])

def process_file(job_details):

    id_num = str(job_details["pid"])
    filename = job_details["filename"]

    filename_short = os.path.basename(filename).split('.')[0]

    meta = os.path.join(results_folder, "metadata_{}.csv".format(filename_short))
    # check if analysis_meta already exists...
    if os.path.isfile(meta):
        os.remove(meta)

    battery_max = 0
    if monitor_type == "GeneActiv":
        battery_max = GA_battery_max
    elif monitor_type == "Axivity":
        battery_max = AX_battery_max
    
    epochs = [timedelta(minutes=n) for n in epoch_minutes]
    # Use 'epochs_minutes' variable to create the corresponding names to the epochs defined
    names = []
    plots_list = []
    for n in epoch_minutes:
        name = ""
        if n % 60 == 0:                     # If the epoch is a multiple of 60, it will be named in hours, e.g. '1h'
            name = "{}h".format(int(n/60))
        elif n% 60 != 0:                    # If the epoch is NOT a multiple of 60, it will be named in seconds, e.g. '15m'
            name = "{}m".format(n)
        names.append(name)
        if n in epoch_plot:
            plots_list.append(name)
    
    # fast-load the data to identify any anomalies:
    qc_ts, qc_header = data_loading.fast_load(filename, monitor_type)

    qc_channels = qc_ts.get_channels(["X", "Y", "Z"])
    
    anomalies = diagnostics.diagnose_fix_anomalies(qc_channels, discrepancy_threshold=2)
    
    # Load the data
    ts, header = data_loading.load(filename, monitor_type, compress=False)
    header["processed_file"] = os.path.basename(filename)

    # some monitors have manufacturers parameters applied to them, let's preserve these but rename them:
    var_list = ["x_gain", "x_offset", "y_gain", "y_offset", "z_gain", "z_offset", "calibration_date"] 
    for var in var_list:
        if var in header.keys():
            header[("manufacturers_%s" % var)] = header[var]
            header.pop(var)

    x, y, z, battery, temperature, integrity = ts.get_channels(["X", "Y", "Z", "Battery", "Temperature", "Integrity"])
    initial_channels = [x, y, z, battery, temperature, integrity]
    
    # create dictionary of anomalies total and types
    anomalies_dict = {"QC_anomalies_total": len(anomalies)}
    
    # check whether any anomalies have been found:
    if len(anomalies) > 0:
        anomalies_file = os.path.join(results_folder, "{}_anomalies.csv".format(filename_short))
        df = pd.DataFrame(anomalies)
        
        for type in anomaly_types:
            anomalies_dict["QC_anomaly_{}".format(type)] = (df.anomaly_type.values == type).sum()
        
        df = df.set_index("anomaly_type")
        # print record of anomalies to anomalies_file
        df.to_csv(anomalies_file)
        
        # if anomalies have been found, fix these anomalies
        channels = diagnostics.fix_anomalies(anomalies, initial_channels)

    else:
        for type in anomaly_types:
            anomalies_dict["QC_anomaly_{}".format(type)] = 0
        # if no anomalies    
        channels = initial_channels

    first_channel = channels[0]
    # Convert timestamps to offsets from the first timestamp
    start, offsets = Channel.timestamps_to_offsets(first_channel.timestamps)

    # As timestamps are sparse, expand them to 1 per observation
    offsets = Channel.interpolate_offsets(offsets, len(first_channel.data))
    
    # For each channel, convert to offset timestamps
    for c in channels:
        c.start = start
        c.set_contents(c.data, offsets, timestamp_policy="offset")
    
    # find approximate first and last battery percentage values
    first_battery_pct = round((battery.data[1] / battery_max) * 100, 2)
    last_battery_pct = round((battery.data[-1] / battery_max) * 100, 2)

    # Calculate the time frame to use
    start = time_utilities.start_of_day(x.timeframe[0])
    end = time_utilities.end_of_day(x.timeframe[-1])
    tp = (start, end)

    # if the sampling frequency is greater than 40Hz
    if x.frequency > 40:    
        # apply a low pass filter
        x = pampro_fourier.low_pass_filter(x, 20, frequency=x.frequency, order=4)
        x.name = "X"  # because LPF^ changes the name, we want to override that
        y = pampro_fourier.low_pass_filter(y, 20, frequency=y.frequency, order=4)
        y.name = "Y"
        z = pampro_fourier.low_pass_filter(z, 20, frequency=z.frequency, order=4)
        z.name = "Z"

    # find any bouts where data is "missing" BEFORE calibration
    missing_bouts = []
    if -111 in x.data:
        # extract the bouts of the data channels where the data == -111 (the missing value)
        missing = x.bouts(-111, -111)
        
        # add a buffer of 2 minutes (120 seconds) to the beginning and end of each bout 
        for item in missing:
            
            bout_start = max(item.start_timestamp - timedelta(seconds=120), x.timeframe[0])
            bout_end = min(item.end_timestamp + timedelta(seconds=120), x.timeframe[1])
            
            new_bout = Bout.Bout(start_timestamp=bout_start, end_timestamp=bout_end)
            missing_bouts.append(new_bout)
    
    else:
        pass
    
    x.delete_windows(missing_bouts)
    y.delete_windows(missing_bouts)
    z.delete_windows(missing_bouts)
    integrity.fill_windows(missing_bouts, fill_value=1)
    
    ################ CALIBRATION #######################
    
    # extract still bouts
    calibration_ts, calibration_header = triaxial_calibration.calibrate_stepone(x, y, z, noise_cutoff_mg=noise_cutoff_mg)
    # Calibrate the acceleration to local gravity
    cal_diagnostics = triaxial_calibration.calibrate_steptwo(calibration_ts, calibration_header, calibration_statistics=False)

    # calibrate data
    triaxial_calibration.do_calibration(x, y, z, temperature=None, cp=cal_diagnostics)

    x.delete_windows(missing_bouts)
    y.delete_windows(missing_bouts)
    z.delete_windows(missing_bouts)
    temperature.delete_windows(missing_bouts)
    battery.delete_windows(missing_bouts)
    
    # Derive some signal features
    vm = channel_inference.infer_vector_magnitude(x, y, z)
    vm.delete_windows(missing_bouts)
    
    if "HPFVM" in stats:
        vm_hpf = channel_inference.infer_vm_hpf(vm)
    else:
        vm_hpf = None

    if "ENMO" in stats:
        enmo = channel_inference.infer_enmo(vm)
    else:
        enmo = None

    if "PITCH" and "ROLL" in stats:
        pitch, roll = channel_inference.infer_pitch_roll(x, y, z)
    else:
        pitch = roll = None

    # Infer nonwear and mask those data points in the signal
    nonwear_bouts = channel_inference.infer_nonwear_triaxial(x, y, z, noise_cutoff_mg=noise_cutoff_mg)
    for bout in nonwear_bouts:
        # Show non-wear bouts in purple
        bout.draw_properties = {'lw': 0, 'alpha': 0.75, 'facecolor': '#764af9'}
    
    for channel, channel_name in zip([enmo, vm_hpf, pitch, roll, temperature, battery], ["ENMO", "HPFVM", "PITCH", "ROLL", "Temperature", "Battery"]):
        if channel_name in stats:
        # Collapse the sample data to a processing epoch (in seconds) so data is summarised
            epoch_level_channel = channel.piecewise_statistics(timedelta(seconds=processing_epoch), time_period=tp)[0]
            epoch_level_channel.name = channel_name
            if channel_name in ["Temperature", "Battery"]:
                pass
            else:
                epoch_level_channel.delete_windows(nonwear_bouts)
            epoch_level_channel.delete_windows(missing_bouts)    
            ts.add_channel(epoch_level_channel)
    
        # collapse binary integrity channel
        epoch_level_channel = integrity.piecewise_statistics(timedelta(seconds=int(processing_epoch)), statistics=[("binary", ["flag"])], time_period=tp)[0]
        epoch_level_channel.name = "Integrity"
        epoch_level_channel.fill_windows(missing_bouts, fill_value=1)
        ts.add_channel(epoch_level_channel)   
    
    # create and open results files
    results_files = [os.path.join(results_folder, "{}_{}.csv".format(name, filename_short)) for name in names]
    files = [open(file, "w") for file in results_files]

    # Write the column headers to the created files
    for f in files:
        f.write(pampro_utilities.design_file_header(stats) + "\n")
    
    # writing out and plotting results
    for epoch, name, f in zip(epochs, names, files):
        results_ts = ts.piecewise_statistics(epoch, statistics=stats, time_period=tp, name=id_num)
        results_ts.write_channels_to_file(file_target=f)
        f.flush()
        if name in plots_list:
            # for each statistic in the plotting dictionary, produce a plot in the results folder
            for stat, plot in plotting_dict.items():
                try:
                    results_ts[stat].add_annotations(nonwear_bouts)
                    results_ts.draw([[stat]], file_target=os.path.join(results_folder, plot.format(filename_short, name)))
                except KeyError:
                    pass

    header["processing_script"] = version
    header["analysis_resolutions"] = names
    header["noise_cutoff_mg"] = noise_cutoff_mg
    header["processing_epoch"] = processing_epoch
    header["QC_first_battery_pct"] = first_battery_pct
    header["QC_last_battery_pct"] = last_battery_pct
    
    metadata = {**header, **anomalies_dict, **cal_diagnostics}
    
    # write metadata to file
    pampro_utilities.dict_write(meta, id_num, metadata)

    for c in ts:
        del c.data
        del c.timestamps
        del c.indices
        del c.cached_indices

batch_processing.batch_process(process_file, job_file, job_num, num_jobs, task="pampro_processing")