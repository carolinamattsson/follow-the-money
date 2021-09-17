from datetime import datetime, timedelta
from collections import defaultdict
import traceback

from utils import parse, timewindow_trajectories, bin_duration
from utils import consolidate_txn_types, finalize_summary, write_summary

#######################################################################################################
#######################################################################################################

def get_account(wflow,idx):
    '''
    account at this index of this account-adjusted trajectory
    '''
    return wflow['acct_IDs'][idx]

def get_subcateg(wflow,idx):
    '''
    categs relative to this index of this account-adjusted trajectory
    '''
    subcateg = (wflow['trj_categ'][0] if idx==0 else 'transfer', wflow['trj_categ'][1] if idx==wflow['trj_len'] else 'transfer')
    return '~'.join(subcateg)

def get_submotif(wflow,idx,consolidate=None):
    '''
    transaction types at this index of this account-adjusted trajectory
    '''
    submotif = [wflow['txn_types'][idx],wflow['txn_types'][idx+1]]
    if consolidate is not None:
        submotif = consolidate_txn_types(submotif,consolidate)
    return '~'.join(submotif)

def get_delta_t(wflow,idx):
    '''
    delta_t at this index of this account-adjusted trajectory, with an
    indication of whether this value corresponds to a complete observation
    '''
    # Flag the ambiguous values
    complete = True
    #   durations extending outside the timewindow are ambiguous
    if wflow['txn_types'][idx]=="initial": complete = False
    if wflow['txn_types'][idx+1]=="final": complete = False
    #   untracked funds are ambiguous at the end of a trajectory
    if wflow['txn_types'][idx+1]=="untracked": complete = False
    # For account-based aggregation, values should always exist
    if wflow["acct_durs"][idx] is None:
        # untracked~?? pairs should not be here since the corresponding
        # ??~untracked pair already contributes the relevant duration
        raise ValueError("Instantaneous pairs (untracked~??) should not enter the account-based loop. Txn pair:",(wflow["txn_IDs"][idx],wflow["txn_IDs"][idx+1]))
    else:
        delta_t =  wflow["acct_durs"][idx]
    # Report the values (unambiguous or upper bounds)
    return delta_t, complete

#######################################################################################################

def define_acct_splits(cutoffs=None,consolidate=None,upper=False):
    '''
    define the functions used to get the split given a wflow and account index
    '''
    get_split = {'account':get_account,'subcateg':get_subcateg,'submotif':get_submotif,'interval': lambda x,y: bin_duration(*get_delta_t(x,y))}
    # re-define on account of keyword arguments, if needed
    if upper: get_split.update({'interval':lambda x,y: bin_duration(*get_delta_t(x,y),upper=upper)})
    if cutoffs is not None: get_split.update({'interval':lambda x,y: bin_duration(*get_delta_t(x,y),cutoffs=cutoffs,upper=upper)})
    if consolidate is not None: get_split.update({'submotif':lambda x,y: get_submotif(x,y,consolidate=consolidate)})
    # send them off
    return get_split

def acct_adjust(wflow,timewindow):
    '''
    adjust the various lists so that the accounts can be looped over,
    including a mask for the given timewindow
    '''
    # Handle the end of untracked trajectories
    #   (adjust here if you want these handled differently)
    if wflow['trj_categ'][1]=='untracked':
        wflow['txn_IDs'] = wflow['txn_IDs']+[None]
        wflow['txn_types'] = wflow['txn_types']+["untracked"]
        wflow['txn_revs'] = wflow['txn_revs']+[0]
    # Get rid of the first account and (if it exists) the exit account
    #   (these are useful only for trajectory-based summarizing)
    wflow['acct_IDs'].pop(0)
    if wflow['trj_categ'][1]=='withdraw':
        wflow['acct_IDs'].pop()
    # compute the largest allowable offsets in either direction
    offset_min = (timewindow[0]  - wflow['trj_timestamp']).total_seconds()/60/60 if timewindow[0] else -float('inf')
    offset_max = (timewindow[-1] - wflow['trj_timestamp']).total_seconds()/60/60 if timewindow[-1] else float('inf')
    # generate the account-based mask
    mask = [offset_min <= offset < offset_max for offset in [0.0]+wflow['acct_durs']]
    # return
    return wflow, mask

def accts_wflow(get_split,split_bys,wflow,mask):
    '''
    loop over accounts in the account-adjusted flow to get the relevant splits
    '''
    for idx, acct_ID in enumerate(wflow['acct_IDs']):
        if mask[idx]:
            split = tuple(get_split[term](wflow,idx) for term in split_bys) if split_bys else 'all'
            yield idx, split

#######################################################################################################
#######################################################################################################

def acct_aggregate(wflow_file,output_file,split_bys=[],consolidate=None,cutoffs=None,upper=False,timeformat="%Y-%m-%d %H:%M:%S",timewindow_acct=(None,None),timewindow_trj=(None,None)):
    #############################################################
    acct_summary_header = split_bys+["txn_pairs","txns_in","amount","fees","accts","median_dur_a","median_dur_d"]
    # motifs is a nested dictionary: split-tuple -> property -> valu
    get_split = define_acct_splits(consolidate=consolidate,cutoffs=cutoffs,upper=upper)
    acct_summary = defaultdict(lambda: {'txns_in':0,'txn_pairs':set(),'amount':0,'fees':0,"accts":set(),"durations":[]})
    ##########################################################################################
    wflow_header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
    with open(wflow_file,'r') as wflow_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        # populate the summary dictionary
        for wflow in timewindow_trajectories(reader_wflows,timewindow_trj,timeformat):
            try:
                wflow = parse(wflow,timeformat)
                wflow_aa, mask_aa = acct_adjust(wflow,timewindow_acct)
                for acct_idx, split in accts_wflow(get_split,split_bys,wflow_aa,mask_aa):
                    acct_summary = update_acct_summary(acct_summary,split,wflow_aa,acct_idx,upper)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())
        # finalize the records
        acct_summary = finalize_summary(acct_summary,split_bys,sets=['txn_pairs','accts'],flows=False)
        # write the results
        write_summary(acct_summary,output_file,acct_summary_header)


def update_acct_summary(summary,split,wflow,idx,upper):
    '''
    update the summary dictionary at this split with this account-adjusted
    trajectory at this account index
    '''
    # straightforward sums
    summary[split]["txns_in"] += wflow['txn_txns'][idx]
    summary[split]["amount"] += wflow['txn_amts'][idx]
    summary[split]["fees"] += wflow['txn_revs'][idx+1]
    # sets of accounts
    summary[split]["txn_pairs"].add((wflow['txn_IDs'][idx],wflow['txn_IDs'][idx+1]))
    summary[split]["accts"].add(wflow['acct_IDs'][idx])
    # duration distribution
    delta_t, complete = get_delta_t(wflow,idx)
    if upper and not complete: delta_t = float("inf")
    summary[split]["durations"].append((delta_t,wflow['txn_amts'][idx],wflow['txn_txns'][idx]))
    return summary

#######################################################################################################

def acct_durations(wflow_file,output_file,split_bys=[],consolidate=None,cutoffs=None,upper=False,timeformat="%Y-%m-%d %H:%M:%S",timewindow_acct=(None,None),timewindow_trj=(None,None)):
    #############################################################
    dists_header = ["duration","complete","txns_in","amount"]+split_bys
    get_split = define_acct_splits(consolidate=consolidate,cutoffs=cutoffs,upper=upper)
    ##########################################################################################
    with open(wflow_file,'r') as wflow_file, open(output_file,'w') as output_file:
        #############################################################
        wflow_header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        # the durations are continuous-valued; we will be streaming them out
        writer_dists  = csv.writer(output_file,delimiter=",",quotechar="'",escapechar="%")
        writer_dists.writerow(dists_header)
        # loop to grab the durations
        for wflow in timewindow_trajectories(reader_wflows,timewindow_trj,timeformat):
            try:
                wflow = parse(wflow,timeformat)
                wflow_aa, mask_aa = acct_adjust(wflow,timewindow_acct)
                for acct_idx, split in accts_wflow(get_split,split_bys,wflow_aa,mask_aa):
                    value = make_acct_value(wflow_aa,acct_idx)
                    writer_dists.writerow(value+list(split))
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())

def make_acct_value(wflow,idx):
    # Get the duration and an indication of whether it's just a lower bound
    duration, complete = get_delta_t(wflow,idx)
    # Duration, minimum duration, amount, and in-transaction-normalized amount
    return [duration,complete,wflow['txn_txns'][idx],wflow['txn_amts'][idx]]

#######################################################################################################
#######################################################################################################

if __name__ == '__main__':
    import argparse
    import sys
    import csv
    import os

    available_splits = define_acct_splits()
    available_splits = available_splits.keys()

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input weighted flow file (created by follow_the_money.py)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output filenames')
    parser.add_argument('--suffix', default="", help='Suffix appended to output filenames')
    parser.add_argument('--split_by', action='append', default=[], help="Split aggregation by any number of these options: "+str(available_splits))
    parser.add_argument('--consolidate', action='append', default=[], help="Transaction types to consolidate, as 'name:[type1,type2,...]'. Feel free to call multiple times.")
    parser.add_argument('--cutoffs', default=None, help="Duration cutoffs, in hours. Takes a list of integers as '[cutoff1,cutoff2,...]'.'")
    parser.add_argument('--upper', action="store_true", default=False, help="Use upper bound for unknown durations in binning and aggregation.'")
    parser.add_argument('--delta_t', action="store_true", default=False, help="Output the full, unbinned, distribution of delta_t.'")
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format used for timestamps in trajectory file & timewindow, as a string.')
    # time-based filtering; NOT RECOMMENDED
    parser.add_argument('--timewindow_acct', default='(,)', help='Filter on funds that *entered* accounts within this time window, as a tuple. NOT RECOMMENDED.')
    parser.add_argument('--timewindow_trj', default='(,)', help='Pre-filter on trajectories that begin within this time window, as a tuple. NOT RECOMMENDED.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file

    if not all([option in available_splits for option in args.split_by]):
        raise IndexError("Please ensure all --split_by are among the available options "+str(available_splits)+"):",args.split_by)

    if args.cutoffs is not None:
        try:
            args.cutoffs = sorted([int(cutoff) for cutoff in args.cutoffs.strip('()[]').split(',')])
        except:
            raise ValueError("Please make sure the format of your --cutoffs argument is '[cutoff1,cutoff2,...]':",args.cutoffs)

    name = "acct_agg" if not args.delta_t else "acct_delta_t"
    output_filename = os.path.join(args.output_directory,args.prefix+name+("_"+"_".join(args.split_by) if args.split_by else "")+args.suffix+".csv")

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

    timewindow_acct = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow_acct.strip('()').strip('[]').split(',')])
    timewindow_trj = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow_trj.strip('()').strip('[]').split(',')])

    ################ Creates aggregated summary file #################
    if not args.delta_t: acct_aggregate(wflow_filename,output_filename,split_bys=args.split_by,consolidate=args.consolidate,cutoffs=args.cutoffs,upper=args.upper,timeformat=args.timeformat,timewindow_acct=timewindow_acct,timewindow_trj=timewindow_trj)
    ################ Creates aggregated summary file #################
    if args.delta_t: acct_durations(wflow_filename,output_filename,split_bys=args.split_by,consolidate=args.consolidate,cutoffs=args.cutoffs,upper=args.upper,timeformat=args.timeformat,timewindow_acct=timewindow_acct,timewindow_trj=timewindow_trj)
    #################################################
