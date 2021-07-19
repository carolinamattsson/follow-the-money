from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import math

from utils import parse, timewindow_trajectories, partial_trajectories
from utils import consolidate_txn_types, get_categ, get_motif, get_length, cumsum

get_split = {'categ':get_categ,'motif':get_motif,'length':get_length}

#######################################################################################################
def trj_summarize(wflow_file,output_file,timewindow=(None,None),timeformat="%Y-%m-%d %H:%M:%S",partials=False,split_bys=[],max_transfers=None,consolidate=False):
    ##########################################################################################
    wflow_header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
    summary_header = split_bys+["flows","amount","deposits","entrys","exits","users","median_dur_f","median_dur_a","median_dur_d"]
    with open(wflow_file,'r') as wflow_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # motifs is a nested dictionary: split-tuple -> property -> value
        summary = defaultdict(lambda: {"flows":0,"amount":0,"deposits":0,"entrys":set(),"exits":set(),"users":set(),"durations":[]})
        # re-define the global split-getters, if needed
        global get_split
        if consolidate is not None: get_split.update({'motif':lambda x: get_motif(x,consolidate=consolidate)})
        if max_transfers is not None: get_split.update({'motif':lambda x: get_motif(x,max_transfers=max_transfers),'length':lambda x: get_length(x,max_transfers=max_transfers)})
        if max_transfers is not None and consolidate is not None: get_split.update({'motif':lambda x: get_motif(x,consolidate=consolidate,max_transfers=max_transfers)})
        # populate the summary dictionary
        for wflow in partial_trajectories(timewindow_trajectories(reader_wflows,timewindow,timeformat),fees=partials):
            try:
                wflow = parse(wflow,timeformat)
                split = tuple(get_split[term](wflow) for term in split_bys) if split_bys else 'all'
                summary = update_summary(summary,split,wflow)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())
        # finalize the records
        summary = finalize_summary(summary,split_bys)
        # write the results
        write_summary(summary,output_file,summary_header)


def update_summary(summary,split,wflow):
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
    # duration distribution (if the trajectory has a duration)
    if wflow['trj_dur'] is not None:
        summary[split]["durations"].append((wflow['trj_dur'],wflow['trj_amt'],wflow['trj_txn']))
    return summary

def finalize_summary(summary,split_bys):
    '''
    finalize the summary dictionary, given this list of split_by terms
    '''
    for split in list(summary.keys()):
        # for each split of the trajectory data
        split_summary = summary[split]
        # generate a column for each term used to split the data
        for term,value in zip(split_bys,split):
            split_summary[term] = value
        # retrieve the number of unique entry points, exit points, and users
        split_summary["entrys"] = len(split_summary["entrys"])
        split_summary["exits"] = len(split_summary["exits"])
        split_summary["users"] = len(split_summary["users"])
        # summarize the duration distribution, if there was one
        if split_summary["durations"]:
            split_summary["durations"].sort()
            #flow_cumsum = list(cumsum([1 for x in split_summary["durations"]]))
            #flow_mid = next(i for i,v in enumerate(flow_cumsum) if v >= flow_cumsum[-1]/2)
            flow_mid = math.ceil(len(split_summary["durations"])/2) - 1
            split_summary["median_dur_f"] = split_summary["durations"][flow_mid][0]
            amt_cumsum = list(cumsum([x[1] for x in split_summary["durations"]]))
            amt_mid = next(i for i,v in enumerate(amt_cumsum) if v >= amt_cumsum[-1]/2)
            split_summary["median_dur_a"] = split_summary["durations"][amt_mid][0]
            nrm_cumsum = list(cumsum([x[2] for x in split_summary["durations"]]))
            nrm_mid = next(i for i,v in enumerate(nrm_cumsum) if v >= nrm_cumsum[-1]/2)
            split_summary["median_dur_d"] = split_summary["durations"][nrm_mid][0]
        else:
            split_summary["median_dur_f"] = ""
            split_summary["median_dur_a"] = ""
            split_summary["median_dur_d"] = ""
        # relieve some memory pressure
        del split_summary["durations"]
        # update this entry in the above dictionary
        summary[split] = split_summary
    return summary

def write_summary(summary,output_file,summary_header):
    with open(output_file,'w') as output_file:
        writer_summary = csv.DictWriter(output_file,summary_header,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_summary.writeheader()
        # print distribution
        for split in summary:
            writer_summary.writerow(summary[split])

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

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file

    if not all([option in get_split.keys() for option in args.split_by]):
        raise IndexError("Please ensure all --split_by are among the available options "+str(get_split.keys())+"):",args.split_by)

    output_filename = os.path.join(args.output_directory,args.prefix+"summary"+("_"+"-".join(args.split_by) if args.split_by else "")+args.suffix+".csv")

    timewindow = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow.strip('()').strip('[]').split(',')])

    try:
        joins = [join.split(':') for join in args.consolidate]
        joins = {join[0]:set(join[1].strip('()[]').split(',')) for join in joins}
    except:
        raise IndexError("Please make sure the format of your --join argument(s) is 'name:[type1,type2,...]'")

    all_joins_list = []
    for join in args.consolidate:
        all_joins_list.extend(args.consolidate[join])
    if len(all_joins_list) != len(set(all_joins_list)):
        raise ValueError("Please do not duplicate consolidated transaction types:",args.consolidate)

    ######### Creates weighted flow file #################
    trj_summarize(wflow_filename,output_filename,timewindow=timewindow,timeformat=args.timeformat,partials=args.partials,split_bys=args.split_by,max_transfers=args.max_transfers,consolidate=joins)
    #################################################
