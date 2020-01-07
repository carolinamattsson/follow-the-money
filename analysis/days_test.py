##########################################################################################
### Get stats and system maturity for each day ###
##########################################################################################
from collections import defaultdict
from multiprocessing import Pool
import traceback
import csv

#######################################################################################################
# Decine the possible time slices
def get_month(timestamp):
    return "-".join(txn['timestamp'].split("-")[:-1])

def get_day(timestamp):
    return timestamp.split(' ')[0]

def get_hour(timestamp):
    return timestamp[0:13]+':00:00'

#######################################################################################################
# Define the time slice generator function
def gen_time_slices(records,get_time_slice,sort_field='timestamp'):
    slice = []
    first = True
    for record in records:
        if first:
            old_time = get_time_slice(record[sort_field])
            first = False
        if old_time != get_time_slice(record[sort_field]):
            yield slice
            del slice[:]
            old_time = get_time_slice(record[sort_field])
        slice.append(record)
    yield slice

#######################################################################################################
# Read in the subsets that we want to aggregate over (called within each time slices)
def load_subsets(subsets):
    # Load the subsets that we want to also aggregate over
    for subset in subsets:
        subsets[subset]['set'] = set(account_ID.strip() for account_ID in open(subsets[subset]['filename']))
    return subsets

###########################################################################################
# Define the function that opens the files, runs aggregating functions, and writes the results
def maturity_by_timeslice(wflow_filenames,timeslices_filename,issues_filename,timeslice='day',subsets={},processes=1):
    #################################################################
    # We'll need the user/agent subsets and time slicing function to be accessible by every time slice
    global base_dict, subset_files, load_subsets_files, get_time_slice, issues_file, issues_writer
    load_subsets_files = load_subsets
    subset_files = subsets
    get_time_slice = get_month if timeslice=='month' else (get_day if timeslice=='day' else get_hour)
    base_dict = {'prv_amt':0,'obs_dep':0,'obs_amt':0,'TUE_dep':0,'TUE_amt':0,'DUE_dep':0,'DUE_amt':0,'PRIN-2st_dep':0,'PRIN-2st_amt':0,'PRIN-72hr_dep':0,'PRIN-72hr_amt':0}
    #################################################################
    # Define the time slice maturity summary -- timeslice_summary[TIMESLICE][SUBSET][EXIT]
    timeslice_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: base_dict.copy())))
    exit_types = set()
    #################################################################
    for wflow_filename in wflow_filenames:
        with open(wflow_filename,'r') as wflow_file, open(issues_filename,'a') as issues_file:
            wflow_reader  = csv.DictReader(wflow_file,delimiter=",",quotechar='"')
            issues_writer = csv.writer(issues_file,delimiter=",",quotechar='"')
            #############################################################
            timeslices = gen_time_slices(wflow_reader,get_time_slice,sort_field='root_timestamp')
            for timeslice in timeslices:
                print("New timeslice length: "+str(len(timeslice)))
    #################################################################

if __name__ == '__main__':
    import argparse
    import sys
    import csv
    import os

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input weighted flow file (created by follow_the_money.py)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output files')
    parser.add_argument('--file', action='append', default=[], help='Additional weighted flow/trajectory files.')
    parser.add_argument('--subset_file', action='append', default=[], help='File with a set of subsets to aggregate over.')
    parser.add_argument('--subset_type', action='append', default=[], help='Whether a subset corresponds to the initial "user" or the "entry" point (ex. agents).')
    parser.add_argument('--subset_name', action='append', default=[], help='Name of this subset, used as file extension.')
    parser.add_argument('--timeslice', default='day', help='What time segmentation to use: "month","day","hour".')
    parser.add_argument('--processes', default=1, help='Integer number of parallel processes to use.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filenames     = [args.input_file]
    subset_filenames    = []
    timeslices_filename = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_maturity.csv")
    issues_filename     = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_maturity_issues.txt")

    for additional_file in args.file:
        if not os.path.isfile(additional_file):
            raise OSError("Could not find the additional input file",additional_file)
        else:
            wflow_filenames.append(additional_file)

    if len(args.subset_file) == len(args.subset_type) and len(args.subset_file) == len(args.subset_name):
        subsets = {subset[0]:{'type':subset[1],'filename':subset[2]} for subset in zip(args.subset_name,args.subset_type,args.subset_file)}
    else:
        raise IndexError("Please provide a type and name for each subset file:",args.subset_file,args.subset_type,args.subset_name)
    for subset in subsets:
        if not os.path.isfile(subsets[0]):
            raise OSError("Could not find the subset file.",subsets[0])
        if subsets[1] not in ["user","entry"]:
            raise ValueError("Please use 'user' or 'entry' as the subset type.",subsets[1])

    if args.timeslice not in ["month","day","hour"]:
        raise ValueError("Please use 'month','day', or 'hour' as the time slicing interval.",subsets[1])

    ######### Creates weighted flow file #################
    maturity_by_timeslice(wflow_filenames,timeslices_filename,issues_filename,timeslice=args.timeslice,subsets=subsets,processes=args.processes)
    #################################################
