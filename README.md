## WAP_one_step_process

### Introduction
A standalone script for the processing of waveform accelerometry data from binary AX3 or GENEActiv files to give intensity and/or orientation of monitor statistics at specified time resolution(s).

**PLEASE NOTE:** This is currently a development process, the RAM memory requirements are currently being investigated.  
In the meantime we suggest a minimum RAM capacity of 8GB for a 7-day file (e.g a 250Mb Ax3 file).

### Features
*  Read the binary file
*  Detect and "fix" any anomalies in the time series, writing details of any anomalies to an "anomalies" csv file
*  Calculate the beginning and end battery percentages
*  If the sampling frequency is less than or equal to 40Hz, apply a Butterworth 4th-order low pass filter to the x, y and z data
*  Use bouts of "still" data within the file to derive custom calibration factors for the file and apply to the data
*  Derive ENMO, HPFVM, pitch and roll
*  Infer periods of non-wear
*  Collapse ENMO, HPFVM, pitch and roll, as well as temperature and battery recordings, to a processing epoch (5 seconds by default)
*  Summarise the results for the time resolutions specified (e.g. 1 minute, 1 hour) and write to result files
*  Plot the results for the required time resolutions (e.g. 1 minute)
*  Write the metadata from the file and process to an individual csv file

### Prerequisites
*  Binary files from either an AX3 or GENEActiv device (see below)
*  Python 3.6 or higher ([https://www.python.org/downloads/](url) - choose a download suitable for your system)
*  Pampro Python module installed ([https://github.com/MRC-Epid/pampro](url))
*  Batch-processing capacity (recommended)

The process currently only supports .cwa (AX3) or .bin (GENEActiv) binary files.

NOTE: This process has been developed on a Linux operating system, but is also compatible with Windows.  It has NOT been tested for any other operating system type, e.g. macOS.  The process is initiated using the command line, therefore some familiarity of this is assumed.

### Downloading and preparing the environment
There are two options available for downloading the code, depending on whether you wish to use Git.  Option 1 requires Git to be installed in your environment ([https://git-scm.com/](url)).
1.  EITHER use the command line to navigate to your desired folder location and execute the following command:
`git clone https://github.com/MRC-Epid/WAP_one_step_process/`

2.  OR select the 'Repository' option from the lefthand sidebar, and select the download icon on the top-right of the Repository page.  You can select from different formats of download.
3.  Regardless of whether you used step 1 or 2 above, you should now have a folder that contains the required files.  Also included is a folder named "_logs", this is where log files will be created by the process.
4.  Make a job file. Included in the downloaded files is an example job file with the required column headings "pid" and "filename". Each pid in the column must be a unique identifier and the filename column must contain the complete filepath (including filename) of each file requiring processing.
5.  Make a results folder where you would like the results to be created.  Ensure any relevant permissions are correct.

### Editing the script
As this is a self-contained process, all the settings are found at the top of the processing script WAP_one_step_process_v1.0.py.  Open this file using your preferred text editor.

The settings are commented to explain their usage, and, as a minimum, the ‘monitor_type’ setting should be checked and the locations of the job file and 'results' folder that you have created must be provided.

'epoch_minutes' gives the summarised time resolutions in minutes (by default these 1 minute and 1 hour), and 'epoch_plot' defines the time resolutions which are to be plotted (1 minute, by default).  

'Processing_epoch' and 'noise_cutoff_mg' are set to standard default values, but can be altered if required.

### Executing the script
The processing script takes a 'job number' and 'number of jobs' from the command line as arguments.  These are used in the script to split the job list into sections.  Submitting the job can be done in a number of ways, depending on your environment.

1.  If you do not have the capacity to submit multiple jobs then the simplest way to run the script is to give these both as "1" when submitting the script. Use the command line to navigate to the folder containing the script and issue the following command: `python WAP_one_step_process_v1.0.py 1 1` 
This will run the script as one process.

2.  If, however, you do have multiple-process capability you could submit the script in batches in this way: `python WAP_one_step_process_v1.0.py 1 3 & python WAP_one_step_process_v1.0.py 2 3 & python WAP_one_step_process_v1.0.py 3 3` 
This would execute the python script three times, each process using one third of the job list.

3.  Another batch processing option would be to use a scheduling engine, such as Sun Grid Engine. The shell script 'batch_sge.sh' has been written to take the processing script's relative path, and the number of batches required. It then uses the python environment (in this case provided by Anaconda3) in order to automatically submit the required number of jobs. In order to submit three jobs, it would be executed from the command line thus: `./batch_sge.sh WAP_one_step_process_v1.0.py 3`

### Output
The process produces metadata output for each raw file, as a wide-format 'metadata' .csv file. The variables come from both the metadata contained in the file itself (which varies between the AX3 and GENEActiv files) and the derived output from the process. These files can be consolidated and reviewed accordingly. If a metadata file already exists for the current raw file being processed it will be overwritten with the results from the current process. In addition, if there are any “anomalies” detected in the raw files processed, an ‘anomalies’ .csv file will also be created in the results folder.  

The process produces time-resolution statistics files, labelled accordingly (e.g. "1m" for 1 minute resolution.)  It will also produce .png plot files for the specified time resolution(s) given in the 'epoch_plot' variable.

For an explanation of the variables produced in both the results files and the metadata file, see the data dictionary contained in this repository. NB: As different types of raw file contain slightly different metadata, there is a separate tab on the data dictionary file outlining the relevant metadata for each raw file type. 
