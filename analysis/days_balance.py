##########################################################################################
### Get system balance and interevent distribution ###
##########################################################################################
global util
import days_utils as util

###########################################################################################
# Define the function that opens the files, runs aggregating functions, and writes the results
def balance_by_timeslice(wflow_filename,balance_filename,timeslice='day',subsets={},interevents=False,processes=1,premade=False):
    from multiprocessing import Pool
    from collections import defaultdict
    from collections import Counter
    import traceback
    import csv
    #################################################################
    # Define the function that turns a timestamp into the correpsonding timeslice
    get_timeslice = util.get_month if timeslice=='month' else (util.get_day if timeslice=='day' else util.get_hour)
    #################################################################
    # Define the time slice balance summary -- system_balance[SUBSET][BAL_TYPE][TIMESLICE]
    system_balance = {subset:{'tracked_inc':Counter(),'tracked_dec':Counter(),'savings_inc':Counter(),'savings_dec':Counter()} for subset in ['ALL']+[subset for subset in subsets]}
    # Define the inter-event summary -- system_interevents[SUBSET][NORM][TIMESLICE][HOURS]
    global interevent; interevent = interevents
    system_interevents = {subset:{'amt': defaultdict(Counter),'dep': defaultdict(Counter),'txn':defaultdict(Counter)} for subset in ['ALL']+[subset for subset in subsets]}
    #################################################################
    timeslice_list = util.get_timeslices(wflow_filename,get_timeslice,subsets) if premade else util.gen_timeslices(wflow_filename,get_timeslice,subsets)
    #############################################################
    global issues_file, issues_writer # we want the multiprocessing jobs to be able to catch errors too
    with open(balance_filename+".err",'w') as issues_file:
        issues_writer = csv.writer(issues_file,delimiter=",",quotechar='"')
        #############################################################
        pool = Pool(processes=processes)
        #############################################################
        timeslices = pool.imap_unordered(timeslice_balance, timeslice_list)
        for timeslice, balance, interevents in timeslices:
            for subset in system_balance:
                system_balance[subset]['tracked_inc'].update(balance[subset]['tracked_inc'])
                system_balance[subset]['tracked_dec'].update(balance[subset]['tracked_dec'])
                system_balance[subset]['savings_inc'].update(balance[subset]['savings_inc'])
                system_balance[subset]['savings_dec'].update(balance[subset]['savings_dec'])
                for timeslice in interevents[subset]['amt']:
                    system_interevents[subset]['amt'][timeslice].update(interevents[subset]['amt'][timeslice])
                    system_interevents[subset]['dep'][timeslice].update(interevents[subset]['dep'][timeslice])
                    system_interevents[subset]['txn'][timeslice].update(interevents[subset]['txn'][timeslice])
        pool.close()
        pool.join()
    #################################################################
    # Convert the balance dictionary from net change to end-of-day balance
    timeslices, system_balance = gen_absolute_balance(system_balance)
    # Write the overall total numbers to file
    with open(balance_filename+'.csv', 'w') as balance_file:
        write_balance_file(balance_file, timeslices, system_balance)
    if interevent:
        iets_filename = balance_filename.replace("balance","iets")
        with open(iets_filename+'_amt.csv', 'w') as amt_file, \
             open(iets_filename+'_dep.csv', 'w') as dep_file, \
             open(iets_filename+'_txn.csv', 'w') as txn_file:
            write_interevents_files((amt_file, dep_file, txn_file), timeslices, system_interevents)
    # Write the by-subset numbers to file
    for subset in subsets:
        subset_filename = '_'.join([balance_filename,subset])
        with open(subset_filename+'.csv', 'w') as balance_file:
            write_balance_file(balance_file, timeslices, system_balance, subset = subset)
        if interevent:
            with open(subset_filename+'_iets_amt.csv', 'w') as amt_file, \
                 open(subset_filename+'_iets_dep.csv', 'w') as dep_file, \
                 open(subset_filename+'_iets_txn.csv', 'w') as txn_file:
                write_interevents_files(system_interevents, amt_file, dep_file, txn_file, subset = subset)

###########################################################################################
# Define the (parallel) function that loads the file, the subsets, and runs the maturity computations
def timeslice_balance(timeslice_tuple):
    from collections import defaultdict
    from collections import Counter
    import traceback
    import csv
    # Note globally accessible function: load_time_slice, load_subsets
    # Note globally accessible variables: issues_file, issues_writer, interevent
    get_timeslice, timeslice, filename, subsets = timeslice_tuple
    # Load the subsets and timeslice
    subsets = util.load_subsets(subsets)
    flows   = util.load_time_slice(filename)
    # Define Counters for the changes in balance contributed by this timeslice --- balance[subset][BAL_TYPE]
    balance = {}
    for subset in ['ALL']+[subset for subset in subsets]:
        balance[subset] = {'tracked_inc':Counter(),'tracked_dec':Counter(),'savings_inc':Counter(),'savings_dec':defaultdict(float)}
    # Define Counters for the interevent times contributed by this timeslice's flows --- interevents[subset][TIMESLICE][HOURS]
    interevents = {}
    for subset in ['ALL']+[subset for subset in subsets]:
        interevents[subset] = {'amt':defaultdict(Counter),'dep':defaultdict(Counter),'txn':defaultdict(Counter)}
    # Populate them!
    for flow in flows:
        try:
            # Parse the lists of numbers that define the flow
            flow = util.parse(flow,get_timeslice)
            # Note the balance changes due to this flow
            balance = update_balance(balance,flow,subsets)
            # Note the inter-event distribution
            if interevent: interevents = update_interevents(interevents,flow,subsets)
        except:
            issues_writer.writerow(['could not calculate measures:',flow['flow_txn_IDs'],traceback.format_exc()])
            issues_file.flush()
    # Print progress
    issues_writer.writerow(['Processed: '+timeslice])
    issues_file.flush()
    # Return!
    return timeslice, balance, interevents

def update_interevents(interevents,flow,subsets):
    '''
    '''
    import math
    # Establish the kind of flow we're dealing with
    enter_categ, exit_categ = flow['flow_categs']
    enter_amt = flow['flow_amts'][0]+flow['flow_revs'][0]
    # Get the durations, binned by hour
    for timeslice, dur, amt, txn, user_ID in zip(flow['timeslices'],flow['flow_durs'],flow['flow_amts'],flow['flow_txns'],flow['flow_acct_IDs'][1:]):
        dur = math.ceil(dur)
        for subset in ['ALL']+[subset for subset in subsets if user_ID in subsets[subset]]:
            interevents[subset]['amt'][timeslice][dur] += amt
            interevents[subset]['txn'][timeslice][dur] += txn
            if enter_categ == 'deposit':
                interevents[subset]['dep'][timeslice][dur] += amt/enter_amt
    # Return the updated dictionary of interevent times
    return interevents

def update_balance(balance,flow,subsets):
    '''
    '''
    # Establish the kind of flow we're dealing with
    enter_categ, exit_categ = flow['flow_categs']
    # Get the start/end timestamps/timeslices
    timeslices = flow['timeslices']
    # Note the balance change at start
    balance['ALL']['tracked_inc'][timeslices[0]] += flow['flow_amts'][0]+flow['flow_revs'][0]
    # Note the balance changes along flow
    for amount, revenue, timeslice, src_ID, tgt_ID in zip(flow['flow_amts'],flow['flow_revs'],timeslices,flow['flow_acct_IDs'],flow['flow_acct_IDs'][1:]):
        balance['ALL']['tracked_dec'][timeslice] -= revenue
        for subset in [subset for subset in subsets if src_ID in subsets[subset]]:
            balance[subset]['tracked_dec'][timeslice] -= amount
            balance[subset]['tracked_dec'][timeslice] -= revenue
        for subset in [subset for subset in subsets if tgt_ID in subsets[subset]]:
            balance[subset]['tracked_inc'][timeslice] += amount
    # Note the balance change at end
    balance['ALL']['tracked_dec'][timeslices[-1]] -= flow['flow_amts'][-1]
    # Note the contribution to the savings balance change
    if enter_categ != 'deposit':
        for subset in ['ALL']+[subset for subset in subsets if flow['flow_acct_IDs'][0] in subsets[subset]]:
            balance[subset]['savings_dec'][timeslices[0]] -= flow['flow_amts'][0]+flow['flow_revs'][0]
    if exit_categ == 'savings':
        for subset in ['ALL']+[subset for subset in subsets if flow['flow_acct_IDs'][-1] in subsets[subset]]:
            balance[subset]['savings_inc'][timeslices[-1]] += flow['flow_amts'][-1]
    # Return the updated dictionary of net change in balance over that day
    return balance

###########################################################################################
# Converts the balance dictionary from net change to end-of-day balance
def gen_absolute_balance(system_balance):
    # Get the list of timeslices
    timeslices = sorted(set(system_balance['ALL']['tracked_inc'].keys()).union(\
                            system_balance['ALL']['tracked_dec'].keys()).union(\
                            system_balance['ALL']['savings_inc'].keys()).union(\
                            system_balance['ALL']['savings_dec'].keys())
                            )
    # Define the time slice balance summary -- timeslice_summary[SUBSET][BAL_TYPE][TIMESLICE]
    timeslice = timeslices[0]
    system_balance['ALL']['savings_bal'] = {timeslice:0}
    for subset in system_balance.keys():
        system_balance[subset]['tracked_bal'] = {timeslice:0}
    for prev, timeslice in zip(timeslices[:-1], timeslices[1:]):
        net_balance_change = (system_balance['ALL']['savings_inc'][timeslice] if timeslice in system_balance['ALL']['savings_inc'] else 0) + \
                             (system_balance['ALL']['savings_dec'][timeslice] if timeslice in system_balance['ALL']['savings_dec'] else 0)
        system_balance['ALL']['savings_bal'][timeslice] = system_balance['ALL']['savings_bal'][prev] + net_balance_change
    for subset in system_balance:
        for prev, timeslice in zip(timeslices[:-1], timeslices[1:]):
            net_balance_change = (system_balance[subset]['tracked_inc'][timeslice] if timeslice in system_balance[subset]['tracked_inc'] else 0) + \
                                 (system_balance[subset]['tracked_dec'][timeslice] if timeslice in system_balance[subset]['tracked_dec'] else 0)
            system_balance[subset]['tracked_bal'][timeslice] = system_balance[subset]['tracked_bal'][prev] + net_balance_change
    return timeslices, system_balance

###########################################################################################
# Define the function that writes the dictionary of maturity computations to output files
def write_balance_file(output_file, timeslices, system_balance, subset = 'ALL'):
    # Create the header
    header = ['timeslice']+['tracked_bal','tracked_inc','tracked_dec','savings_bal','savings_inc','savings_dec']
    # Now write the files
    w = csv.DictWriter(output_file,header,delimiter=",",quotechar='"',escapechar="%")
    w.writeheader()
    for timeslice in timeslices:
        try:
            record = {term:0 for term in header}
            record['timeslice'] = timeslice
            if subset in system_balance:
                for term in ['tracked_bal','tracked_inc','tracked_dec','savings_bal','savings_inc','savings_dec']:
                    record[term] = system_balance[subset][term][timeslice]
            w.writerow(record)
        except:
            print("Issue writing to file: "+timeslice+traceback.format_exc())
    return

###########################################################################################
# Define a function to get all the durations
def get_durations(system_interevents):
    durations = set()
    for subset in system_interevents:
        for norm in system_interevents[subset]:
            for timeslice in system_interevents[subset][norm]:
                durations.update(system_interevents[subset][norm][timeslice].keys())
    return min(durations),max(durations)

# Define the function that writes the dictionary of maturity computations to output files
def write_interevents_files(interevent_files, timeslices, system_interevents, subset = 'ALL'):
    # Unpack the files
    amt_file, dep_file, txn_file = interevent_files
    # Get the range of durations
    min_dur, max_dur = get_durations(system_interevents)
    # Create the header
    header = ['timeslice']+[str(hours) for hours in range(min_dur,max_dur+1)]
    # Now write the files
    for norm, file in zip(['amt','dep','txn'],[amt_file, dep_file, txn_file]):
        w = csv.DictWriter(file,header,delimiter=",",quotechar='"',escapechar="%")
        w.writeheader()
        for timeslice in timeslices:
            try:
                record = {term:0 for term in header}
                record['timeslice'] = timeslice
                for hour in system_interevents[subset][norm][timeslice]:
                    record[str(hour)] = system_interevents[subset][norm][timeslice][hour]
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
    parser.add_argument('input_file', help='The input weighted flow file (or premade directory if --premade is checked)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--timeslice', default='day', help='What time segmentation to use: "month","day","hour".')
    parser.add_argument('--interevents', action="store_true", default=False, help='Produce the distribution of interevent times from each timeslice.')
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
    balance_filename = os.path.join(args.output_directory,args.prefix+args.timeslice+"s_balance")

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
    balance_by_timeslice(args.input_file,balance_filename,timeslice=args.timeslice,subsets=subsets,interevents=args.interevents,processes=args.processes,premade=args.premade)
    #################################################
