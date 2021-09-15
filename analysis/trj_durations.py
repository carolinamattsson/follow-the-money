from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import math

from utils import parse, timewindow_trajectories, partial_trajectories
from utils import get_categ, get_motif, get_length, get_duration, cumsum

get_split = {'categ':get_categ,'motif':get_motif,'length':get_length,'duration':get_duration}

#######################################################################################################
def trj_durations(wflow_file,output_file,timewindow=(None,None),timeformat="%Y-%m-%d %H:%M:%S",partials=False,split_bys=[],max_transfers=None,cutoffs=None,consolidate=None,lower=False):
    #############################################################
    dists_header = ["duration","complete","amount","deposits"]+split_bys
    # re-define the global split-getters, if needed
    global get_split
    if lower: get_split.update({'duration':lambda x: get_duration(x,lower=lower)})
    if cutoffs is not None: get_split.update({'duration':lambda x: get_duration(x,cutoffs=cutoffs,lower=lower)})
    if consolidate is not None: get_split.update({'motif':lambda x: get_motif(x,consolidate=consolidate)})
    if max_transfers is not None: get_split.update({'motif':lambda x: get_motif(x,max_transfers=max_transfers),'length':lambda x: get_length(x,max_transfers=max_transfers)})
    if max_transfers is not None and consolidate is not None: get_split.update({'motif':lambda x: get_motif(x,consolidate=consolidate,max_transfers=max_transfers)})
    ##########################################################################################
    wflow_header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
    with open(wflow_file,'r') as wflow_file, open(output_file,'w') as output_file:
        reader_wflows = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # the durations are continuous-valued; we will be streaming them out
        writer_dists  = csv.writer(output_file,delimiter=",",quotechar="'",escapechar="%")
        writer_dists.writerow(dists_header)
        # populate the summary dictionary
        for wflow in partial_trajectories(timewindow_trajectories(reader_wflows,timewindow,timeformat),fees=partials):
            try:
                wflow = parse(wflow,timeformat)
                value = make_value(wflow)
                split = [get_split[term](wflow) for term in split_bys] if split_bys else []
                writer_dists.writerow(value+split)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())

def make_value(wflow):
    # Duration, minimum duration, amount, and deposit-normalized amount
    value = [wflow["trj_dur"],True,wflow['trj_amt'],wflow['trj_txn']]
    # Handle instantaneous trajectories
    if wflow["trj_dur"] is None: value[0] = 0
    # Handle the case where this is just a lower bound
    if wflow['trj_categ'][1]=='untracked': value[1] = False
    if wflow['txn_types'][-1]=='final': value[1] = False
    # Now, return
    return value

if __name__ == '__main__':
    import argparse
    import sys
    import csv
    import os

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input weighted flow file (created by follow_the_money.py)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output filenames')
    parser.add_argument('--suffix', default="", help='Suffix appended to output filenames')
    parser.add_argument('--timewindow', default="(,)", help='Include trajectories that begin within this time window, as a tuple.')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format used for timestamps in trajectory file & timewindow, as a string.')
    parser.add_argument('--partials', action="store_true", default=False, help='TODO') # Consider partial trajectories that end in fees as own trajectories
    parser.add_argument('--split_by', action='append', default=[], help="Split aggregation by any number of these options: "+str(get_split.keys()))
    parser.add_argument('--max_transfers', type=int, default=None, help='Aggregate separately only up to this number of consecutive transfers, as an integer.')
    parser.add_argument('--consolidate', action='append', default=[], help="Transaction types to consolidate, as 'name:[type1,type2,...]'. Feel free to call multiple times.")
    parser.add_argument('--cutoffs', default=None, help="Duration cutoffs, in hours. Takes a list of integers as '[cutoff1,cutoff2,...]'.'")
    parser.add_argument('--lower', action="store_true", default=False, help="Use lower bound for unknown durations.'")

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file

    if not all([option in get_split.keys() for option in args.split_by]):
        raise IndexError("Please ensure all --split_by are among the available options "+str(get_split.keys())+"):",args.split_by)

    output_filename = os.path.join(args.output_directory,args.prefix+"durations"+("_"+"-".join(args.split_by) if args.split_by else "")+args.suffix+".csv")

    timewindow = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow.strip('()').strip('[]').split(',')])

    if args.cutoffs is not None:
        try:
            args.cutoffs = sorted([int(cutoff) for cutoff in args.cutoffs.strip('()[]').split(',')])
        except:
            raise ValueError("Please make sure the format of your --cutoffs argument is '[cutoff1,cutoff2,...]':",args.cutoffs)

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

    ######### Creates weighted flow file #################
    trj_durations(wflow_filename,output_filename,timewindow=timewindow,timeformat=args.timeformat,partials=args.partials,split_bys=args.split_by,max_transfers=args.max_transfers,consolidate=args.consolidate,cutoffs=args.cutoffs,lower=args.lower)
    #################################################
