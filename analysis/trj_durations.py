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
    Exact or upper bound duration of this trajectory, in hours.
    '''
    if pair['trj_categ'][0] == 'existing':
        # There is no duration, this is an instantanous one for pairwise flows from existing funds
        return float("nan")
    elif pair['trj_categ'][1] == 'untracked':
        # The duration is unknown, this is a lower bound for pairwise flows to untracked funds
        return float("inf")
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

def get_timestamp_start(value,timeformat="%Y-%m-%d %H:%M:%S"):
    '''
    Retrieve the starting timestamp from the timestamps tuple and pass through the duration.
    '''
    return value["duration"], datetime.strftime(value['timestamps'][0],timeformat) if value['timestamps'][0] is not None else None

def get_timestamp_end(value,timeformat="%Y-%m-%d %H:%M:%S"):
    '''
    Retrieve the ending timestamp from the timestamps tuple and pass through the duration.
    '''
    return value["duration"], datetime.strftime(value['timestamps'][1],timeformat) if value['timestamps'][1] is not None else None

def impose_timestamp_start(value,existing_start=None,timeformat="%Y-%m-%d %H:%M:%S"):
    '''
    Retrieve the starting timestamp from the timestamps tuple and pass through the duration,
    unless this is a pair from existing funds in which case impose a max duration.
    '''
    if  value['timestamps'][0] is None:
        # Impose the starting timestamp and compute the resulting duration
        value["duration"] = (value['timestamps'][1]-existing_start).total_seconds()/3600
        return value["duration"], datetime.strftime(existing_start,timeformat)
    else:
        # the given timestamp is the starting one, and we know the ending one
        return value["duration"], datetime.strftime(value['timestamps'][0],timeformat) if value['timestamps'][0] is not None else None

def impose_timestamp_end(value,untracked_end=None,timeformat="%Y-%m-%d %H:%M:%S"):
    '''
    Retrieve the ending timestamp from the timestamps tuple and pass through the duration,
    unless this is a pair ending in untracked funds in which case impose a max duration.
    '''
    if  value['timestamps'][1] is None:
        # Impose the ending timestamp and compute the resulting duration
        value["duration"] = (untracked_end-value['timestamps'][0]).total_seconds()/3600
        return value["duration"], datetime.strftime(untracked_end,timeformat)
    else:
        # the given timestamp is the starting one, and we know the ending one
        return value["duration"], datetime.strftime(value['timestamps'][1],timeformat) if value['timestamps'][1] is not None else None

def trj_durations(pair_file,output_file,columns=[],consolidate=None,timewindow=(None,None),timeformat="%Y-%m-%d %H:%M:%S"):
    #############################################################
    # Create the dictionary of functions to get the values for each column
    get_value = {'duration':get_duration,
                 'amount':lambda x: x['trj_amt'],
                 'fraction':lambda x: x['trj_txn'],
                 'categ':get_categ,
                 'motif':get_motif,
                 'account':get_account,
                 'timestamps':get_timestamps,
                 'timestamp_start':lambda x: get_timestamp_start(x,timeformat=timeformat),
                 'timestamp_end':lambda x: get_timestamp_end(x,timeformat=timeformat)}
    # Adjust the motif getter function if asked to consolidate transaction types
    if consolidate is not None: 
        get_value.update({'motif':lambda x: get_motif(x,consolidate=consolidate)})
    # Adjust the timestamp_start and timestamp_end getter functions if asked to impose a timewindow
    if timewindow[0] is not None:
        get_value.update({'timestamp_start':lambda x: impose_timestamp_start(x,existing_start=timewindow[0],timeformat=timeformat)})
    if timewindow[1] is not None:
        get_value.update({'timestamp_end':lambda x: impose_timestamp_end(x,untracked_end=timewindow[1],timeformat=timeformat)})
    ##########################################################################################
    # Create the header for the durations output file
    columns = ["duration","amount"]+columns
    durs_header = columns.copy()
    if "timestamps" in durs_header:
        durs_header = durs_header+["timestamp_start","timestamp_end"]
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
                    value["duration"], value["timestamp_start"] = get_value["timestamp_start"](value)
                    value["duration"], value["timestamp_end"] = get_value["timestamp_end"](value)
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
    parser.add_argument('--timewindow', default="(,)", help='Timewindow for imposing max durations for untracked/existing funds, as a tuple. Default "(,)".')
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

    args.timewindow = tuple([(datetime.strptime(timestamp.strip(),args.timeformat) if timestamp != "" else None) for timestamp in args.timewindow.strip('()').strip('[]').split(',')])

    #################################################
    trj_durations(pair_filename,output_filename,columns=args.column,consolidate=args.consolidate,timeformat=args.timeformat,timewindow=args.timewindow)
    #################################################
