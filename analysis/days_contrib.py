##########################################################################################
### Get stats and system contrib for each day ###
##########################################################################################
from multiprocessing import Pool
import traceback
import csv

#######################################################################################################
# Define the possible time slices
def get_month(timestamp):
    return "-".join(timestamp.split("-")[:-1])

def get_day(timestamp):
    return timestamp.split(' ')[0]

def get_hour(timestamp):
    return timestamp[0:13]+':00:00'

#######################################################################################################
# Define the timeslice_tuple generator function
def gen_timeslices(wflow_filename,tmp_dir,get_time_slice,subsets):
    with open(wflow_filename,'r') as wflow_file:
        records = csv.DictReader(wflow_file,delimiter=",",quotechar='"')
        timeslice_files = {}
        for record in records:
            timeslice = get_time_slice(record['root_timestamp'])
            if timeslice not in timeslice_files:
                if timeslice_files: timeslice_files[prev_timeslice]["file"].close()
                timeslice_files[timeslice] = {"timeslice":timeslice, "filename":tmp_dir+timeslice+".tmp"}
                timeslice_files[timeslice]["file"] = open(timeslice_files[timeslice]["filename"],"w")
                timeslice_files[timeslice]["writer"] = csv.DictWriter(timeslice_files[timeslice]["file"],records.fieldnames,delimiter=",",quotechar='"')
                timeslice_files[timeslice]["writer"].writeheader()
            timeslice_files[timeslice]["writer"].writerow(record)
            timeslice_files[timeslice]["file"].flush()
            prev_timeslice = timeslice
        timeslice_files[prev_timeslice]["file"].close()
    return [(timeslice_files[timeslice]["timeslice"],timeslice_files[timeslice]["filename"],subsets) for timeslice in timeslice_files]

#######################################################################################################
# Read in the subsets that we want to aggregate over (called within each time slices)
global load_subsets
def load_subsets(subsets):
    # Load the subsets that we want to also aggregate over
    for subset in subsets:
        subsets[subset]['set'] = set(account_ID.strip() for account_ID in open(subsets[subset]['filename']))
    return subsets

#######################################################################################################
# Read in the list of flows that we want to aggregate together
global load_time_slice
def load_time_slice(wflow_filename):
    # Load the file, and filter out the timeslice we want
    with open(wflow_filename,'r') as wflow_file:
        wflow_reader  = csv.DictReader(wflow_file,delimiter=",",quotechar='"')
        for wflow in wflow_reader:
            yield wflow

###########################################################################################
# Define the function that opens the files, runs aggregating functions, and writes the results
def contrib_by_time_slice(wflow_filename,timeslices_filename,issues_filename,timeslice='day',subsets={},processes=1,tmp_dir=None):
    #################################################################
    # Define the function that turns a timestamp into the correpsonding timeslice
    get_time_slice = get_month if timeslice=='month' else (get_day if timeslice=='day' else get_hour)
    #################################################################
    # Define the time slice contribution to maturity summary -- timeslice_summary[TIMESLICE][subset][EXIT]
    timeslice_summary = {}
    global  base_dict
    base_dict = {'prv_amt':0,'obs_txn':0,'obs_amt':0,'dur_txn':0,'dur_amt':0}
    #################################################################
    timeslice_list = gen_timeslices(wflow_filename,tmp_dir,get_time_slice,subsets)
    #############################################################
    global issues_file, issues_writer # we want the multiprocessing jobs to be able to catch errors too
    with open(issues_filename,'w') as issues_file:
        issues_writer = csv.writer(issues_file,delimiter=",",quotechar='"')
        #############################################################
        pool = Pool(processes=processes)
        #############################################################
        timeslices = pool.imap_unordered(timeslice_contrib, timeslice_list)
        for timeslice, summary in timeslices:
            timeslice_summary[timeslice] = summary
        pool.close()
        pool.join()
    #################################################################
    # When done...
    # Write the overall total numbers to file
    with open(timeslices_filename, 'w') as timeslices_file:
        write_timeslice_file(timeslices_filename, timeslices_file, timeslice_summary)
    # Write the by-ubset numbers to file
    for subset in subsets:
        subset_filename = timeslices_filename.split('.csv')[0]+'_'+subset+'.csv'
        with open(subset_filename, 'w') as subset_file:
            write_timeslice_file(subset_filename, subset_file, timeslice_summary, subset = subset)

###########################################################################################
# Define the (parallel) function that loads the file, the subsets, and runs the contrib computations
def timeslice_contrib(timeslice_tuple):
    import traceback
    import csv
    # Note globally accessible function: load_time_slice, load_subsets
    # Note globally accessible variables: base_dict, issues_file, issues_writer
    timeslice, filename, subsets = timeslice_tuple
    # Then, load the subsets!
    subsets = load_subsets(subsets)
    subsets['ALL'] = None
    # Then, get the timeslice!
    flows = load_time_slice(filename)
    # Now, define the dictionary summarizing this timeslice --- summary[subset][EXIT]
    summary = {}
    for subset in subsets:
        summary[subset] = {'TOTAL':base_dict.copy(),\
                           'REVENUE':base_dict.copy(),\
                           'SAVINGS':base_dict.copy()}
    # Finally, populate it!
    for flow in flows:
        try:
            # Parse the lists of numbers that define the flow
            flow['flow_acct_IDs']  = flow['flow_acct_IDs'].strip('[]').split(',')
            flow['flow_txn_types'] = flow['flow_txn_types'].strip('[]').split(',')
            flow['flow_txns'] = [float(txn) for txn in flow['flow_txns'].strip('[]').split(',')]
            flow['flow_amts'] = [float(amt) for amt in flow['flow_amts'].strip('[]').split(',')]
            flow['flow_revs'] = [float(rev) for rev in flow['flow_revs'].strip('[]').split(',')]
            flow['flow_durs'] = [] if flow['flow_durs']=="[]" else [float(dur) for dur in flow['flow_durs'].strip('[]').split(',')]
            # Note the boundary conditions of this flow
            enter_categ, exit_categ = flow['flow_categs'].strip('()').split(',')
            # Note the entry/exit_ID for this flow
            entry_ID = flow['flow_acct_IDs'].pop(0)
            exit_ID  = flow['flow_acct_IDs'].pop() if exit_categ == 'withdraw' else None
            # Treat savings (money held in account beyond time cutoff) as exit transaction
            if exit_categ == 'savings':
                flow['flow_txn_types'].append('SAVINGS')
                flow['flow_amts'].append(flow['flow_amts'][-1])
                flow['flow_revs'].append(0)
            # Note amounts that were in the system prior to the start of the data collection
            if flow['flow_txn_types'][0] == 'inferred': flow['flow_durs'][0] = None
            # Sum up the contribution of this flow to user-centric measures and totals
            summary = update_users_summary(summary,flow,subsets)
        except:
            issues_writer.writerow(['FAILED: CALCULATION:',flow['flow_txn_IDs'],traceback.format_exc()])
            issues_file.flush()
    # Normalize by the total number of observations to get the proper weighted average
    for subset in summary:
        for out_type in summary[subset]:
            if summary[subset][out_type]['obs_amt'] > 0:
                summary[subset][out_type]['dur_amt'] = summary[subset][out_type]['dur_amt']/summary[subset][out_type]['obs_amt']
                summary[subset][out_type]['dur_txn'] = summary[subset][out_type]['dur_txn']/summary[subset][out_type]['obs_txn']
    issues_writer.writerow(['Processed: '+timeslice])
    issues_file.flush()
    return timeslice, summary

###########################################################################################
# Process the contribution of this flow to user-centric measures and totals
global update_user_summary
def update_users_summary(summary,flow,subsets):
    # Loop through the user_IDs along this flow
    for subset in subsets:
        step = 0
        for user_ID in flow['flow_acct_IDs']:
            # Skip if this user_ID is not in the relevant subset
            if subset != 'ALL' and user_ID not in subsets[subset]['set']:
                continue
            # Retrieve the duration in this account
            dur = flow['flow_durs'][step]
            # If this step began before the data was collected, note this and move on
            if dur is None:
                # Get and initialize the particular type of the out transaction, if needed
                out_type = flow['flow_txn_types'][step]
                summary[subset].setdefault(out_type,base_dict.copy())
                summary[subset]['TOTAL']['prv_amt']   += flow['flow_amts'][step]+flow['flow_revs'][step]
                summary[subset][out_type]['prv_amt']  += flow['flow_amts'][step]
                summary[subset]['REVENUE']['prv_amt'] += flow['flow_revs'][step]
                continue
            # Get and initialize the particular type of the out transaction, if needed
            out_type = flow['flow_txn_types'][step+1]
            summary[subset].setdefault(out_type,base_dict.copy())
            # Normalization values
            amt_in  = flow['flow_amts'][step]
            txn_in  = flow['flow_txns'][step]
            amt_out = flow['flow_amts'][step+1]
            txn_out = txn_in*amt_out/amt_in
            amt_rev = flow['flow_revs'][step+1]
            txn_rev = txn_in*amt_rev/amt_in
            # Total observations
            summary[subset]['TOTAL']['obs_amt']   += amt_in
            summary[subset]['TOTAL']['obs_txn']   += txn_in
            summary[subset][out_type]['obs_amt']  += amt_out
            summary[subset][out_type]['obs_txn']  += txn_out
            summary[subset]['REVENUE']['obs_amt'] += amt_rev
            summary[subset]['REVENUE']['obs_txn'] += txn_rev
            # Get the total contribution to DUE
            summary[subset]['TOTAL']['dur_amt']   += dur*amt_in
            summary[subset]['TOTAL']['dur_txn']   += dur*txn_in
            summary[subset][out_type]['dur_amt']  += dur*amt_out
            summary[subset][out_type]['dur_txn']  += dur*txn_out
            summary[subset]['REVENUE']['dur_amt'] += dur*amt_rev
            summary[subset]['REVENUE']['dur_txn'] += dur*txn_rev
            step += 1
    return summary

###########################################################################################
# Define a function to get all the exit types
def get_exit_types(timeslice_summary):
    exit_types = set()
    for timeslice in timeslice_summary:
        for subset in timeslice_summary[timeslice]:
            exit_types.update(timeslice_summary[timeslice][subset].keys())
            exit_types.remove('TOTAL')
    return exit_types

###########################################################################################
# Define the function that writes the dictionary of contrib computations to output files
def write_timeslice_file(output_filename, output_file, timeslice_summary, subset = 'ALL'):
    # Find the exit types
    exit_types = get_exit_types(timeslice_summary)
    # Create the header
    header = ['timeslice']
    for exit_type in ['TOTAL']+list(exit_types):
        header = header + [exit_type+'_'+term for term in base_dict.keys()]
    # Now write the files
    w = csv.DictWriter(output_file,header,delimiter=",",quotechar='"',escapechar="%")
    w.writeheader()
    for timeslice in timeslice_summary:
        try:
            record = {term:0 for term in header}
            record['timeslice'] = timeslice
            if subset in timeslice_summary[timeslice]:
                for exit_type in timeslice_summary[timeslice][subset]:
                    for term in base_dict.keys():
                        record[exit_type+'_'+term] = timeslice_summary[timeslice][subset][exit_type][term]
            w.writerow(record)
        except:
            print("Issue writing to file: "+timeslice+traceback.format_exc())
    return

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
    parser.add_argument('--subset_file', action='append', default=[], help='File with a set of subsets to aggregate over.')
    parser.add_argument('--subset_name', action='append', default=[], help='File with a set of subsets of users to aggregate over.')
    parser.add_argument('--timeslice', default='day', help='What time segmentation to use: "month","day","hour".')
    parser.add_argument('--processes', default=1, help='Integer number of parallel processes to use.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    if not args.prefix: args.prefix = os.path.basename(args.input_file).split("wflows")[0]

    subset_filenames    = []
    timeslices_filename = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_contrib.csv")
    issues_filename     = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_contrib_issues.txt")

    timeslices_tmp_dir  = os.path.join(args.output_directory,args.prefix+args.timeslice+"_tmp","")
    os.makedirs(timeslices_tmp_dir,exist_ok=True)

    if len(args.subset_file) == len(args.subset_name):
        subsets = {subset[0]:{'filename':subset[1]} for subset in zip(args.subset_name,args.subset_file)}
    else:
        raise IndexError("Please provide a name for each subset file:",args.subset_file,args.subset_name)
    for subset in subsets:
        if not os.path.isfile(subsets[subset]['filename']):
            raise OSError("Could not find the subset file.",subsets[subset]['filename'])

    if args.timeslice not in ["month","day","hour"]:
        raise ValueError("Please use 'month','day', or 'hour' as the time slicing interval.",args.timeslice)

    args.processes = int(args.processes)

    ######### Creates weighted flow file #################
    contrib_by_timeslice(args.input_file,timeslices_filename,issues_filename,timeslice=args.timeslice,subsets=subsets,processes=args.processes,tmp_dir=timeslices_tmp_dir)
    #################################################
