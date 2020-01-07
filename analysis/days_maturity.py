##########################################################################################
### Get stats and system maturity for each day ###
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
                timeslice_files[timeslice] = {"timeslice":timeslice, "filename":tmp_dir+timeslice+".tmp", "subsets":subsets}
                timeslice_files[timeslice]["file"] = open(timeslice_files[timeslice]["filename"],"w")
                timeslice_files[timeslice]["writer"] = csv.DictWriter(timeslice_files[timeslice]["file"],records.fieldnames,delimiter=",",quotechar='"')
                timeslice_files[timeslice]["writer"].writeheader()
            timeslice_files[timeslice]["writer"].writerow(record)
            timeslice_files[timeslice]["file"].flush()
            prev_timeslice = timeslice
        timeslice_files[prev_timeslice]["file"].close()
    return [(timeslice_files[timeslice]["timeslice"],timeslice_files[timeslice]["filename"],timeslice_files[timeslice]["subsets"]) for timeslice in timeslice_files]

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
def maturity_by_time_slice(wflow_filename,timeslices_filename,issues_filename,timeslice='day',subsets={},processes=1,tmp_dir=None):
    #################################################################
    # Define the function that turns a timestamp into the correpsonding timeslice
    get_time_slice = get_month if timeslice=='month' else (get_day if timeslice=='day' else get_hour)
    #################################################################
    # Define the time slice maturity summary -- timeslice_summary[TIMESLICE][SUBSET][EXIT]
    timeslice_summary = {}
    global  base_dict
    base_dict = {'prv_amt':0,'obs_dep':0,'obs_amt':0,'TUE_dep':0,'TUE_amt':0,'DUE_dep':0,'DUE_amt':0,'PRIN-2st_dep':0,'PRIN-2st_amt':0,'PRIN-72hr_dep':0,'PRIN-72hr_amt':0}
    #################################################################
    timeslice_list = gen_timeslices(wflow_filename,tmp_dir,get_time_slice,subsets)
    #############################################################
    global issues_file, issues_writer # we want the multiprocessing jobs to be able to catch errors too
    with open(issues_filename,'w') as issues_file:
        issues_writer = csv.writer(issues_file,delimiter=",",quotechar='"')
        #############################################################
        pool = Pool(processes=processes)
        #############################################################
        timeslices = pool.imap_unordered(timeslice_maturity, timeslice_list)
        for timeslice, summary in timeslices:
            timeslice_summary[timeslice] = summary
        pool.close()
        pool.join()
    #################################################################
    # When done...
    # Write the overall total numbers to file
    with open(timeslices_filename, 'w') as timeslices_file:
        write_timeslice_file(timeslices_filename, timeslices_file, timeslice_summary)
    # Write the by-subset numbers to file
    for subset in subsets:
        subset_filename = timeslices_filename.split('.csv')[0]+'_'+subset+'.csv'
        with open(subset_filename, 'w') as subset_file:
            write_timeslice_file(subset_filename, subset_file, timeslice_summary, subset = subset)

###########################################################################################
# Define the (parallel) function that loads the file, the subsets, and runs the maturity computations
def timeslice_maturity(timeslice_tuple):
    import traceback
    import csv
    # Note globally accessible function: load_time_slice, load_subsets
    # Note globally accessible variables: base_dict, issues_file, issues_writer
    timeslice, filename, subsets = timeslice_tuple
    # Then, load the subsets!
    subsets = load_subsets(subsets)
    # Then, get the timeslice!
    flows = load_time_slice(filename)
    # Now, define the dictionary summarizing this timeslice --- summary[SUBSET][EXIT]
    summary = {}
    # Finally, populate it!
    for flow in flows:
        try:
            # Parse the lists of numbers that define the flow
            flow['flow_txns'] = [float(txn) for txn in flow['flow_txns'].strip('[]').split(',')]
            flow['flow_amts'] = [float(amt) for amt in flow['flow_amts'].strip('[]').split(',')]
            flow['flow_revs'] = [float(rev) for rev in flow['flow_revs'].strip('[]').split(',')]
            flow['flow_durs'] = [] if flow['flow_durs']=="[]" else [float(dur) for dur in flow['flow_durs'].strip('[]').split(',')]
            # Determine the boundary conditions of this flow
            enter_categ, exit_categ = flow['flow_categs'].strip('()').split(',')
            # Find the user_ID and the entry_ID for this flow; note if it was not a depoist
            if enter_categ == 'deposit':
                entry_ID, user_ID = flow['flow_acct_IDs'].strip('[]').split(',')[:2]
                prv_amt = None
            else:
                entry_ID, user_ID = None, flow['flow_acct_IDs'].strip('[]').split(',')[0]
                prv_amt = flow['flow_amts'][0]+flow['flow_revs'][0]
            # Find the type of exit for this flow; note if it was not a withdraw
            if exit_categ == 'withdraw':
                exit_type = flow['flow_txn_types'].strip('[]').split(',')[-1]
                exit_txn  = flow['flow_txns'].pop()
                exit_amt  = flow['flow_amts'].pop()
                exit_rev  = flow['flow_revs'].pop()
            else:
                exit_type = 'STAY'
                exit_txn  = 1.0
                exit_amt  = flow['flow_amts'][-1]
                exit_rev  = 0.0
            # Determine the subsets towards which this flow should be counted
            flow_subsets = ['ALL']
            flow_subsets = flow_subsets + [subset for subset in subsets if (subsets[subset]['type']=='user' and user_ID in subsets[subset]['set'])]
            if entry_ID:
                flow_subsets = flow_subsets + [subset for subset in subsets if (subsets[subset]['type']=='entry' and entry_ID in subsets[subset]['set'])]
            # Sum up the contribution of this flow to measures and totals
            for subset in flow_subsets:
                # Initialize the dictionary
                summary.setdefault(subset,{})
                summary[subset].setdefault('TOTAL',base_dict.copy())
                summary[subset].setdefault(exit_type,base_dict.copy())
                summary[subset].setdefault('REVENUE',base_dict.copy())
                # If this flow began before the data was collected, note this and move on
                if prv_amt:
                    summary[subset]['TOTAL']['prv_amt']   += prv_amt
                    summary[subset][exit_type]['prv_amt'] += exit_amt
                    summary[subset]['REVENUE']['prv_amt'] += prv_amt-exit_amt
                    continue
                # Normalization values
                obs_amt      = flow['flow_amts'][0]+flow['flow_revs'][0]
                obs_dep      = flow['flow_txns'][0]
                exit_obs_amt = flow['flow_amts'][-1]
                exit_obs_dep = flow['flow_txns'][0]*exit_amt/obs_amt
                rev_obs_amt  = obs_amt-exit_obs_amt
                rev_obs_dep  = obs_dep-exit_obs_dep
                # Total observations
                summary[subset]['TOTAL']['obs_amt']   += obs_amt
                summary[subset]['TOTAL']['obs_dep']   += obs_dep
                summary[subset][exit_type]['obs_amt'] += exit_obs_amt
                summary[subset][exit_type]['obs_dep'] += exit_obs_dep
                summary[subset]['REVENUE']['obs_amt'] += rev_obs_amt
                summary[subset]['REVENUE']['obs_dep'] += rev_obs_dep
                # Sum up the contribution to TUE
                TUE = sum([1.0*amt for amt in flow['flow_amts']])/obs_amt
                summary[subset]['TOTAL']['TUE_amt']   += TUE*obs_amt
                summary[subset]['TOTAL']['TUE_dep']   += TUE*obs_dep
                summary[subset][exit_type]['TUE_amt'] += TUE*exit_obs_amt
                summary[subset][exit_type]['TUE_dep'] += TUE*exit_obs_dep
                summary[subset]['REVENUE']['TUE_amt'] += TUE*rev_obs_amt
                summary[subset]['REVENUE']['TUE_dep'] += TUE*rev_obs_dep
                # Sum up the contribution to DUE
                DUE = sum([dur*amt for dur,amt in zip(flow['flow_durs'],flow['flow_amts'])])/obs_amt
                summary[subset]['TOTAL']['DUE_amt']   += DUE*obs_amt
                summary[subset]['TOTAL']['DUE_dep']   += DUE*obs_dep
                summary[subset][exit_type]['DUE_amt'] += DUE*exit_obs_amt
                summary[subset][exit_type]['DUE_dep'] += DUE*exit_obs_dep
                summary[subset]['REVENUE']['DUE_amt'] += DUE*rev_obs_amt
                summary[subset]['REVENUE']['DUE_dep'] += DUE*rev_obs_dep
                # Sum up the contribution to the proportion remaining (steps)
                PRIN_2st = 1 if len(flow['flow_durs'])>1 else 0
                summary[subset]['TOTAL']['PRIN-2st_amt']   += PRIN_2st*obs_amt
                summary[subset]['TOTAL']['PRIN-2st_dep']   += PRIN_2st*obs_dep
                summary[subset][exit_type]['PRIN-2st_amt'] += PRIN_2st*exit_obs_amt
                summary[subset][exit_type]['PRIN-2st_dep'] += PRIN_2st*exit_obs_dep
                summary[subset]['REVENUE']['PRIN-2st_amt'] += PRIN_2st*rev_obs_amt
                summary[subset]['REVENUE']['PRIN-2st_dep'] += PRIN_2st*rev_obs_dep
                # Sum up the contribution to the proportion remaining (duration)
                PRIN_72hr = 1 if sum(flow['flow_durs'])>72 else 0
                summary[subset]['TOTAL']['PRIN-72hr_amt']   += PRIN_72hr*obs_amt
                summary[subset]['TOTAL']['PRIN-72hr_dep']   += PRIN_72hr*obs_dep
                summary[subset][exit_type]['PRIN-72hr_amt'] += PRIN_72hr*exit_obs_amt
                summary[subset][exit_type]['PRIN-72hr_dep'] += PRIN_72hr*exit_obs_dep
                summary[subset]['REVENUE']['PRIN-72hr_amt'] += PRIN_72hr*rev_obs_amt
                summary[subset]['REVENUE']['PRIN-72hr_dep'] += PRIN_72hr*rev_obs_dep
        except:
            issues_writer.writerow(['could not calculate measures:',flow['flow_txn_IDs'],traceback.format_exc()])
            issues_file.flush()
    # Normalize by the total number of observations to get the proper weighted average
    for subset in summary:
        for exit_type in summary[subset]:
            if summary[subset][exit_type]['obs_amt'] > 0:
                for term in ['TUE_amt','DUE_amt','PRIN-2st_amt','PRIN-72hr_amt']:
                    summary[subset][exit_type][term] = summary[subset][exit_type][term]/summary[subset][exit_type]['obs_amt']
                for term in ['TUE_dep','DUE_dep','PRIN-2st_dep','PRIN-72hr_dep']:
                    summary[subset][exit_type][term] = summary[subset][exit_type][term]/summary[subset][exit_type]['obs_dep']
    issues_writer.writerow(['Processed: '+timeslice])
    issues_file.flush()
    return timeslice, summary

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
# Define the function that writes the dictionary of maturity computations to output files
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
    parser.add_argument('--subset_type', action='append', default=[], help='Whether a subset corresponds to the initial "user" or the "entry" point (ex. agents).')
    parser.add_argument('--subset_name', action='append', default=[], help='Name of this subset, used as file extension.')
    parser.add_argument('--timeslice', default='day', help='What time segmentation to use: "month","day","hour".')
    parser.add_argument('--processes', default=1, help='Integer number of parallel processes to use.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    subset_filenames    = []
    timeslices_filename = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_maturity.csv")
    issues_filename     = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_maturity_issues.txt")

    timeslices_tmp_dir  = os.path.join(args.output_directory,args.prefix+args.timeslice+"_tmp","")
    os.makedirs(timeslices_tmp_dir,exist_ok=True)

    if len(args.subset_file) == len(args.subset_type) and len(args.subset_file) == len(args.subset_name):
        subsets = {subset[0]:{'type':subset[1],'filename':subset[2]} for subset in zip(args.subset_name,args.subset_type,args.subset_file)}
    else:
        raise IndexError("Please provide a type and name for each subset file:",args.subset_file,args.subset_type,args.subset_name)
    for subset in subsets:
        if not os.path.isfile(subsets[subset]['filename']):
            raise OSError("Could not find the subset file.",subsets[subset]['filename'])
        if subsets[subset]['type'] not in ["user","entry"]:
            raise ValueError("Please use 'user' or 'entry' as the subset type.",subsets[subset]['filename'])

    if args.timeslice not in ["month","day","hour"]:
        raise ValueError("Please use 'month','day', or 'hour' as the time slicing interval.",args.timeslice)

    args.processes = int(args.processes)

    ######### Creates weighted flow file #################
    maturity_by_time_slice(args.input_file,timeslices_filename,issues_filename,timeslice=args.timeslice,subsets=subsets,processes=args.processes,tmp_dir=timeslices_tmp_dir)
    #################################################
