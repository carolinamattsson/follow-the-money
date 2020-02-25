##########################################################################################
### Get stats and system contrib for each day ###
##########################################################################################
global util
import days_utils as util

###########################################################################################
# Define the function that opens the files, runs aggregating functions, and writes the results
def contrib_by_timeslice(wflow_filename,contrib_filename,timeslice='day',subsets={},processes=1,premade=False):
    from multiprocessing import Pool
    from collections import defaultdict
    from collections import Counter
    import traceback
    import csv
    #################################################################
    # Define the function that turns a timestamp into the correpsonding timeslice
    get_time_slice = util.get_month if timeslice=='month' else (util.get_day if timeslice=='day' else util.get_hour)
    #################################################################
    # Define the time slice contribution to contrib contrib -- system_contrib[TIMESLICE][subset][EXIT]
    system_contrib = {}
    #################################################################
    timeslice_list = util.get_timeslices(wflow_filename,get_time_slice,subsets) if premade else util.gen_timeslices(wflow_filename,get_time_slice,subsets)
    #############################################################
    global issues_file, issues_writer # we want the multiprocessing jobs to be able to catch errors too
    with open(contrib_filename+".err",'w') as issues_file:
        issues_writer = csv.writer(issues_file,delimiter=",",quotechar='"')
        #############################################################
        pool = Pool(processes=processes)
        #############################################################
        timeslices = pool.imap_unordered(timeslice_contrib, timeslice_list)
        for timeslice, contrib in timeslices:
            system_contrib[timeslice] = contrib
        pool.close()
        pool.join()
    #################################################################
    # Write the overall total numbers to file
    with open(contrib_filename+".csv", 'w') as contrib_file:
        write_contrib_file(contrib_file, system_contrib)
    # Write the by-ubset numbers to file
    for subset in subsets:
        subset_filename = contrib_filename+'_'+subset+'.csv'
        with open(subset_filename, 'w') as subset_file:
            write_contrib_file(subset_file, system_contrib, subset = subset)

###########################################################################################
# Define the (parallel) function that loads the file, the subsets, and runs the contrib computations
def timeslice_contrib(timeslice_tuple):
    from collections import defaultdict
    import traceback
    # Note globally accessible function: load_time_slice, load_subsets
    # Note globally accessible variables: base_dict, issues_file, issues_writer
    get_timeslice, timeslice, filename, subsets = timeslice_tuple
    # Load the subsets
    subsets = util.load_subsets(subsets)
    # Get the timeslice
    flows = util.load_time_slice(filename)
    # Now, define the dictionary summarizing this timeslice --- contrib[subset][EXIT]
    contrib = {}
    base_dict = {'obs_amt':0,'obs_dep':0}
    for subset in ['ALL']+[subset for subset in subsets]:
        contrib[subset] = {'TOTAL':base_dict.copy(),\
                           'REVENUE':base_dict.copy(),\
                           'SAVINGS':base_dict.copy()}
        contrib[subset]['TOTAL'].update({'inf_amt':0,'RET_amt':0,'DUR_amt':0,'RET_dep':0,'DUR_dep':0})
    # Finally, populate it!
    for flow in flows:
        try:
            # Parse the lists of numbers that define the flow
            flow = util.parse(flow,get_timeslice)
            # Sum up the contribution of this flow to system-wide measures and totals
            contrib = update_users_contrib(contrib,flow,subsets,base_dict)
        except:
            issues_writer.writerow(['could not calculate measures:',flow['flow_txn_IDs'],traceback.format_exc()])
            issues_file.flush()
    # Normalize by the total number of observations to get the proper weighted average
    for subset in contrib:
        for metric, suffix in [(metric, suffix) for metric in ['RET','DUR'] for suffix in ['amt','dep']]:
            if contrib[subset]['TOTAL']['obs_'+suffix] > 0:
                term = '_'.join([metric,suffix])
                contrib[subset]['TOTAL'][term] = contrib[subset]['TOTAL'][term]/contrib[subset]['TOTAL']['obs_'+suffix]
    # Print progress
    issues_writer.writerow(['Processed: '+timeslice])
    issues_file.flush()
    # Return!
    return timeslice, contrib

def update_users_contrib(contrib,flow,subsets,base_dict):
    '''
    '''
    # Establish the kind of flow we're dealing with
    enter_categ, exit_categ = flow['flow_categs']
    # If this flow begins with something other than a deposit --- ignore completely
    if enter_categ != 'deposit':
        return contrib
    # If this flow began before the data was collected, note this and move on
    if flow['flow_txn_types'][0] == 'inferred':
        contrib['ALL']['TOTAL']['inf_amt'] += flow['flow_amts'][0]+flow['flow_revs'][0]
        return contrib
    # Determine the entry details for this flow
    entry_amt = flow['flow_amts'][0]+flow['flow_revs'][0]
    entry_dep = flow['flow_txns'][0]
    # Determine the exit details for this flow
    if exit_categ == 'withdraw':
        exit_ID   = flow['flow_acct_IDs'].pop()
        exit_type = flow['flow_txn_types'].pop()
        exit_amt  = flow['flow_amts'].pop()
        exit_rev  = flow['flow_revs'].pop()
    else:
        exit_type = 'SAVINGS' if exit_categ == 'savings' else flow['flow_txn_types'][-1]
        exit_amt  = flow['flow_amts'][-1]
        exit_rev  = flow['flow_revs'][-1]
    # The exit contributes to DUE but not TUE
    flow['flow_amts'] = flow['flow_amts']+[0]
    flow['flow_revs'] = flow['flow_revs']+[0]
    # Get the contribution to TUE and DUE by each user_ID along this flow
    for step, user_ID in enumerate(flow['flow_acct_IDs'][1:]):
        for subset in ['ALL']+[subset for subset in subsets if user_ID in subsets[subset]]:
            # Total observations & measures
            contrib[subset]['TOTAL']['obs_amt']   += flow['flow_amts'][step]
            contrib[subset]['TOTAL']['DUR_amt']   += flow['flow_amts'][step]*flow['flow_durs'][step]
            contrib[subset]['TOTAL']['RET_amt']   += flow['flow_amts'][step+1]
            contrib[subset]['REVENUE']['obs_amt'] += flow['flow_revs'][step+1]
            norm = entry_dep/entry_amt
            contrib[subset]['TOTAL']['obs_dep']   += norm*flow['flow_amts'][step]
            contrib[subset]['TOTAL']['DUR_dep']   += norm*flow['flow_amts'][step]*flow['flow_durs'][step]
            contrib[subset]['TOTAL']['RET_dep']   += norm*flow['flow_amts'][step+1]
            contrib[subset]['REVENUE']['obs_dep'] += norm*flow['flow_revs'][step+1]
    # Now get the exit
    for subset in ['ALL']+[subset for subset in subsets if user_ID in subsets[subset]]:
        contrib[subset].setdefault(exit_type,{'obs_amt':0,'obs_dep':0})
        contrib[subset][exit_type]['obs_amt'] += exit_amt
        contrib[subset]['REVENUE']['obs_amt'] += exit_rev
        norm = entry_dep/entry_amt
        contrib[subset][exit_type]['obs_dep'] += exit_amt*norm
        contrib[subset]['REVENUE']['obs_dep'] += exit_rev*norm
    return contrib

###########################################################################################
# Define the function that writes the dictionary of contrib computations to output files
def write_contrib_file(contrib_file, system_contrib, subset = 'ALL'):
    import traceback
    # Find the exit types
    exit_types = util.get_exit_types(system_contrib)
    # Create the header
    header = ['timeslice']+['inf_amt','RET_amt','DUR_amt','RET_dep','DUR_dep']
    for exit_type in ['TOTAL']+list(exit_types):
        header = header + [exit_type+'_'+term for term in ['obs_amt','obs_dep']]
    # Now write the files
    w = csv.DictWriter(contrib_file,header,delimiter=",",quotechar='"',escapechar="%")
    w.writeheader()
    for timeslice in system_contrib:
        try:
            record = {term:0 for term in header}
            record['timeslice'] = timeslice
            if subset in system_contrib[timeslice]:
                for term in ['inf_amt','RET_amt','DUR_amt','RET_dep','DUR_dep']:
                    record[term] = system_contrib[timeslice][subset]['TOTAL'][term]
                for exit_type in system_contrib[timeslice][subset]:
                    for term in ['obs_amt','obs_dep']:
                        record[exit_type+'_'+term] = system_contrib[timeslice][subset][exit_type][term]
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
    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input weighted flow file (or premade directory if --premade is checked)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--timeslice', default='day', help='What time segmentation to use: "month","day","hour".')
    parser.add_argument('--subset_file', action='append', default=[], help='File with a set of subsets of users to aggregate over.')
    parser.add_argument('--subset_name', action='append', default=[], help='File with a set of subsets of users to aggregate over.')
    parser.add_argument('--prefix', default="", help='Overwrite the default prefix prepended to output files')
    parser.add_argument('--processes', default=1, help='Integer number of parallel processes to use.')
    parser.add_argument('--premade', action="store_true", default=False, help='Use premade directory of wflow files by timeslice.')

    args = parser.parse_args()

    if args.premade:
        if not os.path.isdir(args.input_file):
            raise OSError("Could not find the input directory",args.input_file)
    else:
        if not os.path.isfile(args.input_file):
            raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    if not args.prefix: args.prefix = os.path.basename(args.input_file).replace("wflows_","").split(".csv")[0]+"_"

    subset_filenames = []
    contrib_filename = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_contrib")

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
    contrib_by_timeslice(args.input_file,contrib_filename,timeslice=args.timeslice,subsets=subsets,processes=args.processes,premade=args.premade)
    #################################################
