from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import math

from utils import parse, consolidate_txn_types

#######################################################################################################
#######################################################################################################

def get_categ(pair):
    '''
    Return the category-combo
    '''
    return "~".join(pair['trj_categ'])

def get_motif(pair,consolidate=None):
    '''
    Return the pair of transaction types, consolidated if requested.
    '''
    txn_types = pair['txn_types'].copy()
    # consolidate transaction types
    if consolidate is not None:
        txn_types = consolidate_txn_types(txn_types,consolidate)
    # Return the transaction pairs
    if pair['trj_categ'][0] == 'existing':
        # There is no in-transaction
        return "~".join([""]+txn_types)
    elif pair['trj_categ'][1] == 'untracked':
        # There is no out-transaction
        return "~".join(txn_types+[""])
    else:
        # Otherwise just return the pair of transaction types
        return "~".join(txn_types)

def get_duration(pair):
    '''
    Exact or lower bound duration of this trajectory, in hours.
    '''
    if pair['trj_categ'][0] == 'existing':
        # There is no duration, this is an instantanous one for pairwise flows from existing funds
        return float("nan")
    else:
        # Otherwise just return the raw duration
        return pair["trj_dur"]

def get_account(pair):
    '''
    Retrieve the account where this duration took place.
    '''
    if pair['trj_categ'][0] == 'existing':
        # initial account for pairwise flows from existing funds; they ended previously as ~untracked funds
        return pair['acct_IDs'][0]
    else:
        # otherwise, use the second in the list
        return pair['acct_IDs'][1]

def get_timestamps(pair):
    '''
    Retrieve the timestamp from the pairwise flow and generate the corresponding one.
    '''
    # Initialize with the given timestamp
    if pair['trj_categ'][0] == 'existing':
        # the given timestamp is the ending one for pairwise flows from existing funds
        timestamps = (None,pair['trj_timestamp'])
    elif pair['trj_categ'][1] == 'untracked':
        # the given timestamp is the starting one, but we don't know the ending one
        timestamps = (pair['trj_timestamp'],None)
    else:
        # the given timestamp is the starting one, and we know the ending one
        timestamps = (pair['trj_timestamp'],pair['trj_timestamp']+timedelta(hours=pair['trj_dur']))
    # Now return the timestamps in the given format
    return timestamps

#######################################################################################################

def get_timestamp_beg(value):
    '''
    Retrieve the starting timestamp from the timestamps tuple and pass through the duration.
    '''
    return value["duration"], value['timestamps'][0]

def get_timestamp_end(value):
    '''
    Retrieve the ending timestamp from the timestamps tuple and pass through the duration,
    unless this is a pair from existing funds in which case use the min duration
    '''
    if value['timestamps'][1] is None:
        # For untracked funds, compute the end timestamp from the lower bound duration
        value['timestamps'] = (value['timestamps'][0],value['timestamps'][0]+timedelta(hours=value['duration']))
    return value["duration"], value['timestamps'][1]

def impose_timestamp_beg(value,existing_beg=None):
    '''
    Retrieve the starting timestamp from the timestamps tuple and pass through the duration,
    unless this is a pair from existing funds in which case impose a max duration.
    '''
    if  value['timestamps'][0] is None:
        # Impose the starting timestamp and compute the resulting duration
        value["duration"] = (value['timestamps'][1]-existing_beg).total_seconds()/3600
        return value["duration"], existing_beg
    else:
        # the given timestamp is the starting one, and we know the ending one
        return value["duration"], value['timestamps'][0]

def impose_timestamp_end(value,untracked_end=None):
    '''
    Retrieve the ending timestamp from the timestamps tuple and pass through the duration,
    unless this is a pair ending in untracked funds in which case impose a max duration.
    '''
    if value['timestamps'][1] is None:
        if untracked_end is None:
            # The duration is unknown, there is an indefinite upper bound for the duration
            return float("inf"), None
        else:
            # Impose the ending timestamp and compute the resulting duration
            return (untracked_end-value['timestamps'][0]).total_seconds()/3600, untracked_end
    else:
        # The duration in known and we know the ending timestamp
        return value["duration"], value['timestamps'][1]

def filter_unobserveds(value,timewindow_end=None):
    '''
    Filter out durations that are infinitessimal, indefinite, and end at or after the given timewindow_end.
    '''
    if value["duration"] == float("inf"):
        return False
    elif not value["duration"] == value["duration"]: # only valid check for float("nan"), nans are weird
        return False
    elif timewindow_end is not None and value['timestamp_end'] >= timewindow_end:
        return False
    else:
        return True

def trj_durations(pair_file,output_file,columns=[],consolidate=None,unobserveds=False,timewindow_beg=None,timewindow_end=None,timeformat="%Y-%m-%d %H:%M:%S"):
    #############################################################
    # Create the dictionary of functions to get the values for each column
    get_value = {'duration':get_duration,
                 'amount':lambda x: x['trj_amt'],
                 'fraction':lambda x: x['trj_txn'],
                 'categ':get_categ,
                 'motif':get_motif,
                 'account':get_account,
                 'timestamps':get_timestamps,
                 'timestamp_beg':lambda x: get_timestamp_beg(x),
                 'timestamp_end':lambda x: get_timestamp_end(x),
                 }
    # Adjust the motif getter function if asked to consolidate transaction types
    if consolidate is not None: 
        get_value.update({'motif':lambda x: get_motif(x,consolidate=consolidate)})
    # Adjust the timewindow_beg and timewindow_end getter functions if asked to impose a timewindow
    if timewindow_beg is not None:
        get_value.update({'timestamp_beg':lambda x: impose_timestamp_beg(x,existing_beg=timewindow_beg)})
        get_value.update({'timestamp_end':lambda x: impose_timestamp_end(x,untracked_end=None)})
        if timewindow_end is not None:
            get_value.update({'timestamp_end':lambda x: impose_timestamp_end(x,untracked_end=timewindow_end)})
    ##########################################################################################
    # Create the header for the durations output file
    columns = ["duration","amount"]+columns
    durs_header = columns.copy()
    if "timestamps" in durs_header:
        durs_header = durs_header+["timestamp_beg","timestamp_end"]
        durs_header.remove("timestamps")
    # Create the file, write the header, and loop over the durations
    with open(pair_file,'r') as pair_file, open(output_file,'w') as output_file:
        #############################################################
        reader_pairs = csv.DictReader(pair_file,delimiter=",",quotechar='"',escapechar="%")
        # the durations are continuous-valued and we will be streaming them out
        writer_dists  = csv.writer(output_file,delimiter=",",quotechar="'",escapechar="%")
        writer_dists.writerow(durs_header)
        # loop to grab the durations
        for pair in reader_pairs:
            try:
                pair = parse(pair,timeformat)
                value = {term:get_value[term](pair) for term in columns}
                # Update the duration and timestamps, if necessary
                if "timestamps" in columns:
                    value["duration"], value["timestamp_beg"] = get_value["timestamp_beg"](value)
                    value["duration"], value["timestamp_end"] = get_value["timestamp_end"](value)
                # Filter out infinitessimal, infinite, or unobserved durations
                if unobserveds or filter_unobserveds(value,timewindow_end=timewindow_end):
                    # Format the timestamps
                    if "timestamps" in columns:
                        value["timestamp_beg"] = datetime.strftime(value["timestamp_beg"],timeformat) if value["timestamp_beg"] is not None else None
                        value["timestamp_end"] = datetime.strftime(value["timestamp_end"],timeformat) if value["timestamp_end"] is not None else None
                    # Print the duration and the other columns
                    writer_dists.writerow([value[term] for term in durs_header])
            except:
                print(str([pair[term] for term in pair])+"\n"+traceback.format_exc())

#######################################################################################################
#######################################################################################################

if __name__ == '__main__':
    import argparse
    import json
    import sys
    import csv
    import os

    available_columns = ["fraction","categ","motif","account","timestamps"]    #TODO: add fraction of balance?

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input pairwise weighted flow file (created by follow_the_money.py --pairwise)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output filenames')
    parser.add_argument('--suffix', default="", help='Suffix appended to output filenames')
    parser.add_argument('--column', action='append', default=[], help="Use 'all' or manually include any number of these columns: "+str(available_columns)+".")
    parser.add_argument('--consolidate', action='append', default=[], help="[motif] Transaction types to change/consolidate, as 'name:[type1,type2,...]'. Feel free to call multiple times.")
    parser.add_argument('--timewindow_beg', default=None, help='Impose max durations for existing funds, leaving untracked funds to be of indefinite duration (these are filtered out by default; use --unobserveds to keep them). Do not give a timestamp after `timewindow_beg` in the trajectory file.')
    parser.add_argument('--timewindow_end', default=None, help='Impose max durations for untracked funds. These and any other durations ending at timewindow_end are filtered out by default; use --unobserveds to keep them. Do not use without --timewindow_beg. Do not give a timestamp before `timewindow_end` in the trajectory file.')
    parser.add_argument('--unobserveds', action="store_true", default=False, help="Keep durations that are infinitessimal, indefinite, and end at or after the given timewindow_end.'")
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format used for timestamps in trajectory file & timewindow, as a string.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    pair_filename = args.input_file

    if "all" in args.column:
        args.column = available_columns

    if not all([option in available_columns for option in args.column]):
        raise IndexError("Please ensure all --column are among the available options "+str(available_columns)+"):",args.column)

    output_filename = os.path.join(args.output_directory,args.prefix+"durations"+args.suffix+".csv")

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

    if args.timewindow_beg is not None:
        args.timewindow_beg = datetime.strptime(args.timewindow_beg.strip(),args.timeformat)
    if args.timewindow_end is not None:
        args.timewindow_end = datetime.strptime(args.timewindow_end.strip(),args.timeformat)

    #################################################
    trj_durations(pair_filename,output_filename,columns=args.column,consolidate=args.consolidate,timeformat=args.timeformat,timewindow_beg=args.timewindow_beg,timewindow_end=args.timewindow_end,unobserveds=args.unobserveds)
    #################################################
