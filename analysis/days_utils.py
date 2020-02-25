##########################################################################################
### Functions for manipulating time slices ###
##########################################################################################
import csv
import os

# Define the possible time slices
def get_month(timestamp):
    return "-".join(timestamp.split("-")[:-1]+['01'])

def get_day(timestamp):
    return timestamp.split(' ')[0]

def get_hour(timestamp):
    return timestamp[0:13]+':00:00'

#######################################################################################################
# Define the timeslice_tuple generator function
def gen_timeslices(wflow_filename,get_time_slice,subsets=None):
    wflow_dir = wflow_filename.split(".csv")[0]
    os.makedirs(wflow_dir)
    with open(wflow_filename,'r') as wflow_file:
        records = csv.DictReader(wflow_file,delimiter=",",quotechar='"')
        timeslice_files = {}
        for record in records:
            timeslice = get_time_slice(record['root_timestamp'])
            if timeslice not in timeslice_files:
                if timeslice_files: timeslice_files[prev_timeslice]["file"].close()
                timeslice_files[timeslice] = {"timeslice":timeslice, "filename":os.path.join(wflow_dir,timeslice+".tmp"), "subsets":subsets}
                timeslice_files[timeslice]["file"] = open(timeslice_files[timeslice]["filename"],"w")
                timeslice_files[timeslice]["writer"] = csv.DictWriter(timeslice_files[timeslice]["file"],records.fieldnames,delimiter=",",quotechar='"')
                timeslice_files[timeslice]["writer"].writeheader()
            timeslice_files[timeslice]["writer"].writerow(record)
            timeslice_files[timeslice]["file"].flush()
            prev_timeslice = timeslice
        timeslice_files[prev_timeslice]["file"].close()
    return [(get_time_slice,timeslice_files[timeslice]["timeslice"],timeslice_files[timeslice]["filename"],timeslice_files[timeslice]["subsets"]) for timeslice in timeslice_files]

# Define the timeslice_tuple getter function for when they're already generated
def get_timeslices(wflow_filename,get_time_slice,subsets=None):
    wflow_dir = wflow_filename.split(".csv")[0]
    filenames = [filename for filename in os.listdir(wflow_dir) if ".tmp" in filename]
    timeslices = [filename.split(".tmp")[0] for filename in filenames]
    timeslice_files = {}
    for timeslice in timeslices:
        timeslice_files[timeslice] = {"timeslice":timeslice, "filename":os.path.join(wflow_dir,timeslice+".tmp"), "subsets":subsets}
    return [(get_time_slice,timeslice_files[timeslice]["timeslice"],timeslice_files[timeslice]["filename"],timeslice_files[timeslice]["subsets"]) for timeslice in timeslice_files]

#######################################################################################################
# Read in the subsets that we want to aggregate over (called within each time slices)
def load_subsets(subsets):
    # Load the subsets that we want to also aggregate over
    for subset in subsets:
        subsets[subset]['set'] = set(account_ID.strip() for account_ID in open(subsets[subset]['filename']))
    return subsets

#######################################################################################################
# Read in the list of flows that we want to aggregate together
def load_time_slice(wflow_filename):
    # Load the file, and filter out the timeslice we want
    with open(wflow_filename,'r') as wflow_file:
        wflow_reader  = csv.DictReader(wflow_file,delimiter=",",quotechar='"')
        for wflow in wflow_reader:
            yield wflow

#######################################################################################################
# Parse a single flow
def parse(wflow,get_time_slice):
    from datetime import datetime, timedelta
    wflow['flow_categs']    = tuple(wflow['flow_categs'].strip('()').split(','))
    wflow['flow_acct_IDs']  = wflow['flow_acct_IDs'].strip('[]').split(',')
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_txns']      = [float(txn) for txn in wflow['flow_txns'].strip('[]').split(',')]
    wflow['flow_amts']      = [float(amt) for amt in wflow['flow_amts'].strip('[]').split(',')]
    wflow['flow_revs']      = [float(rev) for rev in wflow['flow_revs'].strip('[]').split(',')]
    wflow['flow_durs']      = [] if wflow['flow_durs'] == "[]" else [float(dur) for dur in wflow['flow_durs'].strip('[]').split(',')]
    timestamps              = [datetime.strptime(wflow['root_timestamp'],"%Y-%m-%d %H:%M:%S")]
    for dur in wflow['flow_durs']:
        timestamps.append(timestamps[-1]+timedelta(hours=dur))
    wflow['timeslices']     = [get_time_slice(datetime.strftime(timestamp,"%Y-%m-%d %H:%M:%S")) for timestamp in timestamps]
    return wflow

###########################################################################################
# Define a function to get all the exit types
def get_exit_types(system_dict):
    exit_types = set()
    for timeslice in system_dict:
        for subset in system_dict[timeslice]:
            exit_types.update(system_dict[timeslice][subset].keys())
            exit_types.remove('TOTAL')
    return exit_types
