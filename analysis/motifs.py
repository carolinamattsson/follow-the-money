from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import math

#######################################################################################################
def time_filter(wflows,timewindow,timeformat):
    for wflow in wflows:
        if timewindow[0] or timewindow[-1]:
            timestamp = datetime.strptime(wflow['flow_timestamp'],timeformat)
        if timewindow[0] and timestamp < timewindow[0]:
            continue
        if timewindow[-1] and timestamp >= timewindow[-1]:
            continue
        yield wflow

def consolidate_txn_types(wflow, joins):
    for i,txn_type in enumerate(wflow['flow_txn_types']):
        for join in joins:
            if txn_type in joins[join]: wflow['flow_txn_types'][i] = join
    return wflow

def cumsum(a_list):
    total = 0
    for x in a_list:
        total += x
        yield total

#######################################################################################################
def find_motifs(wflow_file,motif_file,timewindow=(None,None),timeformat=None,joins=False,circulate=6):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_txn','flow_length','flow_length_nrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_amts','flow_revs','flow_txns','flow_durs','flow_categs']
    motif_header = ["motif","flows","amount","deposits","users","median_dur_f","median_dur_a","median_dur_d"]
    with open(wflow_file,'r') as wflow_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # motifs is a nested dictionary: motif_ID -> property -> value
        motifs = defaultdict(lambda: {"flows":0,"amount":0,"deposits":0,"users":set(),"durations":[]})
        # populate the motifs dictionary
        for wflow in time_filter(reader_wflows,timewindow,timeformat):
            try:
                wflow = parse(wflow)
                wflow = consolidate_txn_types(wflow, joins) if joins else wflow
                motifs = update_motifs(motifs,wflow,circulate)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())
        # finalize the records
        motifs = finalize_motifs(motifs)
        # write the results
        write_motifs(motifs,motif_file,motif_header)

def parse(wflow):
    #####################################################################################
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_amt']       = float(wflow['flow_amt'])
    wflow['flow_txn'] = float(wflow['flow_txn'])
    wflow['flow_duration']  = float(wflow['flow_duration'])
    wflow['flow_length']    = len(wflow['flow_txn_types'])
    return wflow

def consolidate_motif(txn_types, circulate):
    enter = txn_types[0]
    exit  = txn_types[-1]
    circ  = "~".join(txn_types[1:-1])
    if len(txn_types) >= circulate:
        circ = "circulate"
    return "~".join([enter]+[circ]+[exit]) if circ else "~".join([enter]+[exit])

def update_motifs(motifs, wflow, circulate):
    # define the motif
    motif = consolidate_motif(wflow['flow_txn_types'], circulate)
    # update the motif
    motifs[motif]["flows"]    += 1
    motifs[motif]["amount"]   += wflow['flow_amt']
    motifs[motif]["deposits"] += wflow['flow_txn']
    motifs[motif]["users"].update(wflow['flow_acct_IDs'][1:-1])
    motifs[motif]["durations"].append((wflow['flow_duration'],wflow['flow_amt'],wflow['flow_txn']))
    return motifs

def finalize_motifs(motifs):
    for motif in list(motifs.keys()):
        motif_dict = motifs[motif]
        motif_dict["motif"] = motif
        motif_dict["users"] = len(motif_dict["users"])
        if motif_dict["durations"]:
            motif_dict["durations"].sort()
            #flow_cumsum = list(cumsum([1 for x in motif_dict["durations"]]))
            #flow_mid = next(i for i,v in enumerate(flow_cumsum) if v >= flow_cumsum[-1]/2)
            flow_mid = math.ceil(len(motif_dict["durations"])/2) - 1
            motif_dict["median_dur_f"] = motif_dict["durations"][flow_mid][0]
            amt_cumsum = list(cumsum([x[1] for x in motif_dict["durations"]]))
            amt_mid = next(i for i,v in enumerate(amt_cumsum) if v >= amt_cumsum[-1]/2)
            motif_dict["median_dur_a"] = motif_dict["durations"][amt_mid][0]
            nrm_cumsum = list(cumsum([x[2] for x in motif_dict["durations"]]))
            nrm_mid = next(i for i,v in enumerate(nrm_cumsum) if v >= nrm_cumsum[-1]/2)
            motif_dict["median_dur_d"] = motif_dict["durations"][nrm_mid][0]
        else:
            motif_dict["median_dur_f"] = ""
            motif_dict["median_dur_a"] = ""
            motif_dict["median_dur_d"] = ""
        del motif_dict["durations"]
        motifs[motif] = motif_dict
    return motifs

def write_motifs(motifs,motifs_file,motif_header):
    with open(motifs_file,'w') as motifs_file:
        writer_motif = csv.DictWriter(motifs_file,motif_header,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_motif.writeheader()
        # print distribution
        for motif in motifs:
            writer_motif.writerow(motifs[motif])

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
    parser.add_argument('--circulate', type=int, default=4, help='The length at which flows are considered to circulate -- longer ones are folded in.')
    parser.add_argument('--timewindow', default='(,)', help='Include funds that entered the system within this time window.')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format to read the --timewindow tuple, if different.')
    parser.add_argument('--join', action='append', default=[], help='Enter & exit types with these terms are joined (takes tuples).')
    parser.add_argument('--name', action='append', default=[], help='The name to give this group of transaction types.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    motifs_filename = os.path.join(args.output_directory,args.prefix+"motifs.csv")

    timewindow = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow.strip('()').split(',')])
    timeformat = args.timeformat

    if len(args.join) == len(args.name):
        joins = {join[0]:set(join[1].strip('()').strip('[]').split(',')) for join in zip(args.name,args.join)}
    else:
        raise IndexError("Please provide a name for each set of joined transaction types:",args.name,args.join)

    all_joins_list = []
    for join in joins:
        all_joins_list.extend(joins[join])
    if len(all_joins_list) != len(set(all_joins_list)):
        raise ValueError("Please do not duplicate joined transaction types:",args.join)

    if 'inferred' in all_joins_list and not args.infer:
        raise ValueError("The transaction type 'inferred' cannot be changed unless the --infer flag is also used:",args.join,args.infer)

    ######### Creates weighted flow file #################
    find_motifs(wflow_filename,motifs_filename,timewindow=timewindow,timeformat=timeformat,joins=joins,circulate=args.circulate)
    #################################################
