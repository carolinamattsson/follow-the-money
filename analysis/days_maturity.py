##########################################################################################
### Get stats and system maturity for each day ###
##########################################################################################
global util
import days_utils as util

###########################################################################################
# Define the function that opens the files, runs aggregating functions, and writes the results
def maturity_by_timeslice(wflow_filename,maturity_filename,timeslice='day',subsets={},processes=1,premade=False):
    from multiprocessing import Pool
    from collections import defaultdict
    from collections import Counter
    import traceback
    import csv
    #################################################################
    # Define the function that turns a timestamp into the correpsonding timeslice
    get_time_slice = util.get_month if timeslice=='month' else (util.get_day if timeslice=='day' else util.get_hour)
    #################################################################
    # Define the time slice maturity summary -- system_maturity[TIMESLICE][SUBSET][EXIT]
    system_maturity = {}
    #################################################################
    timeslice_list = util.get_timeslices(wflow_filename,get_time_slice,subsets) if premade else util.gen_timeslices(wflow_filename,get_time_slice,subsets)
    #############################################################
    global issues_file, issues_writer # we want the multiprocessing jobs to be able to catch errors too
    with open(maturity_filename+".err",'w') as issues_file:
        issues_writer = csv.writer(issues_file,delimiter=",",quotechar='"')
        #############################################################
        pool = Pool(processes=processes)
        #############################################################
        timeslices = pool.imap_unordered(timeslice_maturity, timeslice_list)
        for timeslice, maturity in timeslices:
            system_maturity[timeslice] = maturity
        pool.close()
        pool.join()
    #################################################################
    # Write the overall total numbers to file
    with open(maturity_filename+".csv", 'w') as maturity_file:
        write_maturity_file(maturity_file, system_maturity)
    # Write the by-subset numbers to file
    for subset in subsets:
        subset_filename = maturity_filename+'_'+subset+'.csv'
        with open(subset_filename, 'w') as maturity_file:
            write_timeslice_file(maturity_file, system_maturity, subset = subset)

###########################################################################################
# Define the (parallel) function that loads the file, the subsets, and runs the maturity computations
def timeslice_maturity(timeslice_tuple):
    from collections import defaultdict
    import traceback
    # Note globally accessible function: load_time_slice, system_maturityets
    # Note globally accessible variables: issues_file, issues_writer
    get_timeslice, timeslice, filename, subsets = timeslice_tuple
    # Load the subsets
    subsets = util.load_subsets(subsets)
    # Get the timeslice
    flows = util.load_time_slice(filename)
    # Define the dictionary summarizing this timeslice --- maturity[subset][EXIT]
    maturity = {}
    base_dict = {'obs_amt':0,'obs_dep':0,'obs_svg':0,'obs_txn':0,'obs_ext':0}
    for subset in ['ALL']+[subset for subset in subsets]:
        maturity[subset] = {'TOTAL':base_dict.copy(),\
                           'REVENUE':base_dict.copy(),\
                           'SAVINGS':base_dict.copy()}
        maturity[subset]['TOTAL'].update({'inf_amt':0,'TUE_amt':0,'DUE_amt':0,'TUE_dep':0,'DUE_dep':0,'TUE_svg':0,'DUE_svg':0,'TUE_txn':0,'DUE_txn':0})
    # Populate them!
    for flow in flows:
        try:
            # Parse the lists of numbers that define the flow
            flow = util.parse(flow,get_timeslice)
            # Sum up the contribution of this flow to system-wide measures and totals
            maturity = update_system_maturity(maturity,flow,subsets)
        except:
            issues_writer.writerow(['could not calculate measures:',flow['flow_txn_IDs'],traceback.format_exc()])
            issues_file.flush()
    # Normalize by the total number of observations to get the proper weighted average
    for subset in maturity:
        for metric, suffix in [(metric, suffix) for metric in ['TUE','DUE'] for suffix in ['amt','dep','svg','txn']]:
            if maturity[subset]['TOTAL']['obs_'+suffix] > 0:
                term = '_'.join([metric,suffix])
                maturity[subset]['TOTAL'][term] = maturity[subset]['TOTAL'][term]/maturity[subset]['TOTAL']['obs_'+suffix]
    # Print progress
    issues_writer.writerow(['Processed: '+timeslice])
    issues_file.flush()
    # Return!
    return timeslice, maturity

def update_system_maturity(maturity,flow,subsets):
    '''
    '''
    # Establish the kind of flow we're dealing with
    enter_categ, exit_categ = flow['flow_categs']
    # If this flow began before the data was collected, note this and move on
    if enter_categ == 'deposit' and flow['flow_txn_types'][0] == 'inferred':
        maturity['ALL']['TOTAL']['inf_amt'] += flow['flow_amts'][0]+flow['flow_revs'][0]
        return maturity
    # Determine the entry details for this flow
    if enter_categ == 'deposit':
        entry_ID  = flow['flow_acct_IDs'][0]
        entry_amt = flow['flow_amts'][0]+flow['flow_revs'][0]
        entry_dep = flow['flow_txns'][0]
    else:
        entry_ID  = None
        entry_amt = flow['flow_amts'][0]+flow['flow_revs'][0]
        entry_txn = flow['flow_txns'][0]
        flow['flow_txn_types'] = ['SAVINGS']+flow['flow_txn_types']
        flow['flow_amts'] = [0]+flow['flow_amts']
        flow['flow_revs'] = [0]+flow['flow_revs']
        flow['flow_txns'] = [0]+flow['flow_txns']
        flow['flow_durs'] = [0]+flow['flow_durs']
    # Determine the exit details for this flow
    if exit_categ == 'withdraw':
        exit_type = flow['flow_txn_types'].pop()
        exit_amt  = flow['flow_amts'].pop()
        exit_rev  = sum(flow['flow_revs'])
    else:
        exit_type = 'SAVINGS' if exit_categ == 'savings' else flow['flow_txn_types'][-1]
        exit_amt  = flow['flow_amts'][-1]
        exit_rev  = sum(flow['flow_revs'])
    # Find the contribution of this flow to system measures
    TUE = sum([1.0*amt for amt in flow['flow_amts'][1:]])
    DUE = sum([dur*amt for dur,amt in zip(flow['flow_durs'],flow['flow_amts'])])
    # Add the contribution to the total
    for subset in ['ALL']+[subset for subset in subsets]:
        # Skip if this entry_ID is not in the relevant subset
        if subset != 'ALL' and entry_ID not in subsets[subset]['set']:
            continue
        # Initialize the relevant dictionary
        maturity[subset].setdefault(exit_type,{'obs_amt':0,'obs_dep':0,'obs_svg':0,'obs_txn':0,'obs_ext':0})
        # Total observations & measures
        if enter_categ == 'deposit':
            maturity[subset]['TOTAL']['obs_amt']   += entry_amt
            maturity[subset][exit_type]['obs_amt'] += exit_amt
            maturity[subset]['REVENUE']['obs_amt'] += exit_rev
            maturity[subset]['TOTAL']['TUE_amt']   += TUE
            maturity[subset]['TOTAL']['DUE_amt']   += DUE
            norm = entry_dep/entry_amt
            maturity[subset]['TOTAL']['obs_dep']   += entry_dep
            maturity[subset][exit_type]['obs_dep'] += exit_amt*norm
            maturity[subset]['REVENUE']['obs_dep'] += exit_rev*norm
            maturity[subset]['TOTAL']['TUE_dep']   += TUE*norm
            maturity[subset]['TOTAL']['DUE_dep']   += DUE*norm
        else:
            maturity[subset]['TOTAL']['obs_svg']   += entry_amt
            maturity[subset][exit_type]['obs_svg'] += exit_amt
            maturity[subset]['REVENUE']['obs_svg'] += exit_rev
            maturity[subset]['TOTAL']['TUE_svg']   += TUE
            maturity[subset]['TOTAL']['DUE_svg']   += DUE
            if TUE or DUE:
                norm = entry_txn/entry_amt
                maturity[subset]['TOTAL']['obs_txn']   += entry_txn
                maturity[subset][exit_type]['obs_txn'] += exit_amt*norm
                maturity[subset]['REVENUE']['obs_txn'] += exit_rev*norm
                maturity[subset]['TOTAL']['TUE_txn']   += TUE*norm
                maturity[subset]['TOTAL']['DUE_txn']   += DUE*norm
            else:
                maturity[subset]['TOTAL']['obs_ext']   += entry_amt
                maturity[subset][exit_type]['obs_ext'] += exit_amt
                maturity[subset]['REVENUE']['obs_ext'] += exit_rev
    return maturity

###########################################################################################
# Define the function that writes the dictionary of maturity computations to output files
def write_maturity_file(maturity_file, system_maturity, subset = 'ALL'):
    import traceback
    # Find the exit types
    exit_types = util.get_exit_types(system_maturity)
    # Create the header
    header = ['timeslice']+['TUE_amt','DUE_amt','TUE_dep','DUE_dep','TUE_svg','DUE_svg','TUE_txn','DUE_txn','inf_amt']
    for exit_type in ['TOTAL']+list(exit_types):
        header = header + [exit_type+'_'+term for term in ['obs_amt','obs_dep','obs_svg','obs_txn','obs_ext']]
    # Now write the files
    w = csv.DictWriter(maturity_file,header,delimiter=",",quotechar='"',escapechar="%")
    w.writeheader()
    for timeslice in system_maturity:
        try:
            record = {term:0 for term in header}
            record['timeslice'] = timeslice
            if subset in system_maturity[timeslice]:
                for term in ['TUE_amt','DUE_amt','TUE_dep','DUE_dep','TUE_svg','DUE_svg','TUE_txn','DUE_txn','inf_amt']:
                    record[term] = system_maturity[timeslice][subset]['TOTAL'][term]
                for exit_type in system_maturity[timeslice][subset]:
                    for term in ['obs_amt','obs_dep','obs_svg','obs_txn','obs_ext']:
                        record[exit_type+'_'+term] = system_maturity[timeslice][subset][exit_type][term]
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
    parser.add_argument('--timeslice', default='day', help='What time segmentation to use: "month","day","hour".')
    parser.add_argument('--subset_file', action='append', default=[], help='File with a set of subsets of "entry" points (ex. agents) to aggregate over.')
    parser.add_argument('--subset_name', action='append', default=[], help='Name of this subset, used as file extension.')
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

    subset_filenames    = []
    maturity_filename = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_maturity")

    if len(args.subset_file) == len(args.subset_name):
        subsets = {subset[0]:{'filename':subset[1],'set':set()} for subset in zip(args.subset_name,args.subset_file)}
    else:
        raise IndexError("Please provide a name for each subset file:",args.subset_file,args.subset_name)
    for subset in subsets:
        if not os.path.isfile(subsets[subset]['filename']):
            raise OSError("Could not find the subset file.",subsets[subset]['filename'])

    if args.timeslice not in ["month","day","hour"]:
        raise ValueError("Please use 'month','day', or 'hour' as the time slicing interval.",args.timeslice)

    args.processes = int(args.processes)

    ######### Creates weighted flow file #################
    maturity_by_timeslice(args.input_file,maturity_filename,timeslice=args.timeslice,subsets=subsets,processes=args.processes,premade=args.premade)
    #################################################
