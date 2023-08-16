from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import math

from utils import parse, timewindow_trajectories, partial_trajectories, bin_duration
from utils import consolidate_txn_types, finalize_summary, write_summary

#######################################################################################################
#######################################################################################################

def get_categ(wflow):
    # Return the category-combo
    return "~".join(wflow['trj_categ'])

def get_length(wflow,max_transfers=None):
    # Handle the max length (defined as # of transfers)
    transfers = wflow["trj_len"]
    if max_transfers is not None and wflow["trj_len"] >= max_transfers:
        transfers = str(max_transfers)+"+"
    # Return the trajectory length
    return transfers

def raw_duration(wflow):
    '''
    duration of this trajectory, with an indication of whether this value
    corresponds to a complete observation
    '''
    # Flag the ambiguous values
    exact = True
    #   durations extending outside the timewindow are ambiguous
    if wflow['acct_IDs'][0]=='inferred': exact = False
    if wflow['acct_IDs'][-1]=='inferred': exact = False
    #   trajectory ending as untracked funds are ambiguous
    if wflow['trj_categ'][1]=='untracked': exact = False
    # Handle instantaneous trajectories
    if wflow["trj_dur"] is None:
        duration = float("nan")
    else:
        duration = wflow["trj_dur"]
    # Report the values (unambiguous or upper bounds)
    return duration, exact

def get_duration(wflow,upper=False,bound=float("inf")):
    # retrieve the duration
    duration, exact = raw_duration(wflow)
    # adjust to the given bound
    if upper and not exact: duration = bound
    # return
    return duration

def get_motif(wflow,consolidate=None,max_transfers=None):
    from utils import consolidate_txn_types
    ############################################################
    txn_types = wflow['txn_types'].copy()
    # consolidate transaction types
    if consolidate is not None:
        txn_types = consolidate_txn_types(txn_types,consolidate)
    # Handle the start of trajectories
    if wflow['trj_categ'][0]=='deposit':
        enter = txn_types.pop(0)
    elif wflow['trj_categ'][0]=='existing':
        enter = ""
    else:
        raise ValueError("Bad trj_categ:",wflow['trj_categ'][0])
    # Handle the end of trajectories
    if wflow['trj_categ'][1] in ['withdraw','transfer']:
        exit = txn_types.pop()
    elif wflow['trj_categ'][1] in ['untracked','cut']:
        exit = ""
    else:
        raise ValueError("Bad trj_categ:",wflow['trj_categ'][1])
    # Handle the middle of trajectories
    circ  = "~".join(txn_types)
    if max_transfers is not None and len(txn_types) >= max_transfers:
        circ = str(max_transfers)+"+_transfers"
    # Return the motif
    return "~".join([enter]+[circ]+[exit]) if circ else "~".join([enter]+[exit])

def get_deposit(wflow,labels=None):
    try:
        return labels[wflow['txn_IDs'][0]]
    except:
        return 'other'

def get_account(wflow,labels=None):
    # no account for withdraws from existing funds; it's an ~untracked earlier
    if wflow['trj_len'] == 0 and wflow['trj_categ'] == ('existing','withdraw'):
        return 'none'
    if wflow['trj_len'] == 1 and wflow['trj_categ'] == ('existing','transfer'):
        return 'none'
    # otherwise, use the second in the list
    else:
        try:
            return labels[wflow['acct_IDs'][1]]
        except:
            return 'other'

def get_timestamp(wflow,timeformat="%Y-%m-%d %H:%M:%S",group_timeformat=None):
    if group_timeformat is None:
        return wflow['trj_timestamp']
    else:
        return datetime.strftime(datetime.strptime(wflow['trj_timestamp'],timeformat),group_timeformat)

def get_presence(wflow,timestamp):
    # no presence for withdraws from existing funds; it's an ~untracked earlier
    if wflow['trj_len'] == 0 and wflow['trj_categ'] == ('existing','withdraw'):
        return float('nan')
    if wflow['trj_len'] == 1 and wflow['trj_categ'] == ('existing','transfer'):
        return float('nan')
    # use the raw duration to get the ending timestamp
    duration, exact = raw_duration(wflow)
    end_timestamp = wflow['trj_timestamp'] + timedelta(hours=duration)
    return int(wflow['trj_timestamp'] <= timestamp < end_timestamp)

#######################################################################################################

def define_splits(max_transfers=None,cutoffs=None,consolidate=None,upper=False,bound=float("inf"),deposits=None,accounts=None,timestamps=[],group_timeformat=None,timeformat="%Y-%m-%d %H:%M:%S"):
    '''
    define the functions used to get the split given a single wflow
    '''
    # available splits
    get_split = {'categ':get_categ,
                 'motif':get_motif,
                 'length':get_length,
                 'interval': lambda x: bin_duration(get_duration(x,upper=upper,bound=bound)),
                 'deposit':get_deposit,
                 'account':get_account,
                 'timestamp': lambda x: get_timestamp(x,timeformat=timeformat)}
    # re-define using split-getters, if needed
    if upper: get_split.update({'interval':lambda x: bin_duration(get_duration(x,upper=upper,bound=bound),bound=bound)})
    if cutoffs is not None: get_split.update({'interval':lambda x: bin_duration(get_duration(x,upper=upper,bound=bound),cutoffs=cutoffs,bound=bound)})
    if consolidate is not None: get_split.update({'motif':lambda x: get_motif(x,consolidate=consolidate)})
    if max_transfers is not None: get_split.update({'motif':lambda x: get_motif(x,max_transfers=max_transfers),'length':lambda x: get_length(x,max_transfers=max_transfers)})
    if max_transfers is not None and consolidate is not None: get_split.update({'motif':lambda x: get_motif(x,consolidate=consolidate,max_transfers=max_transfers)})
    if deposits is not None: get_split.update({'deposit':lambda x: get_deposit(x,labels=deposits)})
    if accounts is not None: get_split.update({'account':lambda x: get_account(x,labels=accounts)})
    if group_timeformat is not None: get_split.update({'timestamp':lambda x: get_timestamp(x,group_timeformat=group_timeformat,timeformat=timeformat)})
    if timestamps:
        for timestamp in timestamps:
            time = datetime.strptime(timestamp,timeformat)
            get_split[timestamp] = lambda x, time=time: get_presence(x,time)
    # send it off
    return get_split

#######################################################################################################
#######################################################################################################

def trj_aggregate(wflow_file,output_file,timewindow=(None,None),timeformat="%Y-%m-%d %H:%M:%S",partials=False,split_bys=[],max_transfers=None,cutoffs=None,consolidate=None,deposits=None,accounts=None,upper=False,bound=float("inf"),timestamps=[],group_timeformat=None):
    #############################################################
    summary_header = split_bys+["flows","deposits","amount","entrys","exits","users","avg_dur_f","avg_dur_d","avg_dur_a","frc_cpl_f","frc_cpl_d","frc_cpl_a"]
    # motifs is a nested dictionary: split-tuple -> property -> value
    get_split = define_splits(max_transfers=max_transfers,consolidate=consolidate,cutoffs=cutoffs,deposits=deposits,accounts=accounts,upper=upper,bound=bound,timestamps=timestamps,timeformat=timeformat,group_timeformat=group_timeformat)
    summary = defaultdict(lambda: {"flows":0,"amount":0,"deposits":0,"avg_dur_f":0,"avg_dur_a":0,"avg_dur_d":0,"frc_cpl_f":0,"frc_cpl_d":0,"frc_cpl_a":0,"entrys":set(),"exits":set(),"users":set()})
    ##########################################################################################
    wflow_header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
    with open(wflow_file,'r') as wflow_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        # populate the summary dictionary
        for wflow in partial_trajectories(timewindow_trajectories(reader_wflows,timewindow,timeformat),fees=partials):
            try:
                wflow = parse(wflow,timeformat)
                split = tuple(get_split[term](wflow) for term in split_bys+timestamps) if split_bys+timestamps else 'all'
                summary = update_summary(summary,split,wflow,upper,bound)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())
        # finalize the records
        summary = finalize_summary(summary,split_bys,sets=['entrys','exits','users'],flows=True)
        # write the results
        write_summary(summary,output_file,summary_header)

def update_summary(summary,split,wflow,upper,bound):
    '''
    update the summary dictionary at this split with this trajectory
    '''
    # straightforward sums
    summary[split]["flows"]    += 1
    summary[split]["amount"]   += wflow['trj_amt']
    summary[split]["deposits"] += wflow['trj_txn']
    # sets of entry points, exit points, and users
    if wflow['trj_categ'][0]=='deposit': summary[split]["entrys"].add(wflow['acct_IDs'].pop(0))
    if wflow['trj_categ'][1]=='withdraw': summary[split]["exits"].add(wflow['acct_IDs'].pop())
    summary[split]["users"].update(wflow['acct_IDs'])
    # weighted average duration

    summary[split]["avg_dur_f"] += duration
    summary[split]["avg_dur_a"] += wflow['trj_amt']*duration
    summary[split]["avg_dur_d"] += wflow['trj_txn']*duration
    if complete:
        summary[split]["frc_cpl_f"] += 1
        summary[split]["frc_cpl_a"] += wflow['trj_amt']
        summary[split]["frc_cpl_d"] += wflow['trj_txn']
    return summary

#######################################################################################################

def trj_durations(wflow_file,output_file,timewindow=(None,None),timeformat="%Y-%m-%d %H:%M:%S",partials=False,split_bys=[],max_transfers=None,cutoffs=None,consolidate=None,deposits=None,accounts=None,upper=False,bound=float("inf"),timestamps=[],group_timeformat=None):
    #############################################################
    dists_header = ["duration","exact","deposits","amount"]+split_bys+timestamps
    get_split = define_splits(max_transfers=max_transfers,consolidate=consolidate,cutoffs=cutoffs,deposits=deposits,accounts=accounts,upper=upper,bound=bound,timestamps=timestamps,timeformat=timeformat,group_timeformat=group_timeformat)
    ##########################################################################################
    with open(wflow_file,'r') as wflow_file, open(output_file,'w') as output_file:
        #############################################################
        wflow_header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
        reader_wflows = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        # the durations are continuous-valued; we will be streaming them out
        writer_dists  = csv.writer(output_file,delimiter=",",quotechar="'",escapechar="%")
        writer_dists.writerow(dists_header)
        # loop to grab the durations
        for wflow in partial_trajectories(timewindow_trajectories(reader_wflows,timewindow,timeformat),fees=partials):
            try:
                wflow = parse(wflow,timeformat)
                value = make_value(wflow)
                split = [get_split[term](wflow) for term in split_bys+timestamps]
                writer_dists.writerow(value+split)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())

def make_value(wflow):
    # Get the duration and an indication of whether it's just a lower bound
    duration, exact = raw_duration(wflow)
    # Duration, minimum duration, amount, and deposit-normalized amount
    return [duration,int(exact),wflow['trj_txn'],wflow['trj_amt']]

#######################################################################################################
#######################################################################################################

if __name__ == '__main__':
    import argparse
    import json
    import sys
    import csv
    import os

    available_splits = define_splits()
    available_splits = available_splits.keys()

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input weighted flow file (created by follow_the_money.py)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output filenames')
    parser.add_argument('--suffix', default="", help='Suffix appended to output filenames')
    parser.add_argument('--partials', action="store_true", default=False, help='TODO') # Consider partial trajectories that end in fees as own trajectories
    parser.add_argument('--split_by', action='append', default=[], help="Split aggregation by any number of these options: "+str(available_splits))
    parser.add_argument('--duration', action="store_true", default=False, help="Output the full, unbinned, raw, distribution of trajectory durations.'")
    parser.add_argument('--timeseries', default=None, help="Split aggregation by presence/absence at the given timestamp or in the given file.")
    parser.add_argument('--max_transfers', type=int, default=None, help='[length,motif] Aggregate separately only up to this number of consecutive transfers, as an integer.')
    parser.add_argument('--consolidate', action='append', default=[], help="[motif] Transaction types to consolidate, as 'name:[type1,type2,...]'. Feel free to call multiple times.")
    parser.add_argument('--cutoffs', default=None, help="[interval] Duration cutoffs, in hours. Takes a list of integers as '[cutoff1,cutoff2,...]'.'")
    parser.add_argument('--deposits', default=None, help="[deposit] Mapping for the first of the txn_IDs. Takes a .json file with a dictionary of txn_ID:label pairs.'")
    parser.add_argument('--accounts', default=None, help="[account] Mapping for the first recipient acct_IDs. Takes a .json file with a dictionary of acct_ID:label pairs.'")
    parser.add_argument('--group_timeformat', default=None, help='[timestamp] Format used for timestamps binning and aggregation, as a string.')
    parser.add_argument('--upper', action="store_true", default=False, help="Use upper bound for unknown durations in binning and aggregation (never for --duration).'")
    parser.add_argument('--bound', metavar='hours', type=float, default=float("inf"), help='Consider trajectories with the given upper-bound duration "exact".')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format used for timestamps in trajectory file & timewindow, as a string.')
    parser.add_argument('--timewindow', default="(,)", help='Include trajectories that begin within this time window, as a tuple.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file

    if not all([option in available_splits for option in args.split_by]):
        raise IndexError("Please ensure all --split_by are among the available options "+str(available_splits)+"):",args.split_by)

    if args.timeseries is not None:
        try:
            datetime.strptime(args.timeseries,args.timeformat)
            args.timeseries = [args.timeseries]
        except:
            if not os.path.isfile(args.timeseries):
                raise OSError("Could not find the --timestamps file",args.timeseries,"or single timestamp not in proper format:",args.timeformat)
            with open(args.timeseries,'r') as timestamps_file:
                args.timeseries = [timestamp.strip() for timestamp in timestamps_file]
            for timestamp in args.timeseries:
                try:
                    datetime.strptime(timestamp,args.timeformat)
                except:
                    raise ValueError("Please ensure all timestamps in",args.timeseries,"are in the proper timeformat:",args.timeformat)
    else:
        args.timeseries = []

    if args.deposits is not None:
        with open(args.deposits, 'r') as txn_IDs_file:
            args.deposits = json.load(txn_IDs_file)

    if args.accounts is not None:
        with open(args.accounts, 'r') as acct_IDs_file:
            args.accounts = json.load(acct_IDs_file)

    if args.cutoffs is not None:
        try:
            args.cutoffs = sorted([int(cutoff) for cutoff in args.cutoffs.strip('()[]').split(',')])
        except:
            raise ValueError("Please make sure the format of your --cutoffs argument is '[cutoff1,cutoff2,...]':",args.cutoffs)

    name = "trj_agg" if not args.duration else "trj_duration"
    output_filename = os.path.join(args.output_directory,args.prefix+name+("_"+"_".join(args.split_by) if args.split_by else "")+("_up" if args.upper else "")+args.suffix+".csv")

    try:
        joins = [join.split(':') for join in args.consolidate]
        args.consolidate = {join[0]:set(join[1].strip('()[]').split(',')) for join in joins}
    except:
        raise IndexError("Please make sure the format of your --consolidate argument(s) is 'name:[type1,type2,...]'",args.consolidate)

    all_joins_list = []
    for join in args.consolidate:
        all_joins_list.extend(args.consolidate[join])
    if len(all_joins_list) != len(set(all_joins_list)):
        raise ValueError("Please do not duplicate consolidated transaction types:",args.consolidate)

    args.timewindow = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow.strip('()').strip('[]').split(',')])

    ######### Creates weighted flow file #################
    if not args.duration: trj_aggregate(wflow_filename,output_filename,partials=args.partials,split_bys=args.split_by,max_transfers=args.max_transfers,consolidate=args.consolidate,cutoffs=args.cutoffs,deposits=args.deposits,accounts=args.accounts,upper=args.upper,bound=args.bound,timestamps=args.timeseries,group_timeformat=args.group_timeformat,timeformat=args.timeformat,timewindow=args.timewindow)
    #################################################
    if args.duration: trj_durations(wflow_filename,output_filename,partials=args.partials,split_bys=args.split_by,max_transfers=args.max_transfers,consolidate=args.consolidate,cutoffs=args.cutoffs,deposits=args.deposits,accounts=args.accounts,upper=args.upper,bound=args.bound,timestamps=args.timeseries,group_timeformat=args.group_timeformat,timeformat=args.timeformat,timewindow=args.timewindow)
    #################################################
