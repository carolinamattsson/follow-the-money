from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import math

from utils import parse, timewindow_trajectories, consolidate_txn_types
from utils import get_motif, cumsum

#######################################################################################################
def find_motifs(wflow_file,motif_file,circulate=None,timewindow=(None,None),timeformat="%Y-%m-%d %H:%M:%S",joins=False):
    ##########################################################################################
    wflow_header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
    motif_header = ["motif","flows","amount","deposits","users","median_dur_f","median_dur_a","median_dur_d"]
    with open(wflow_file,'r') as wflow_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # motifs is a nested dictionary: motif_ID -> property -> value
        motifs = defaultdict(lambda: {"flows":0,"amount":0,"deposits":0,"users":set(),"durations":[]})
        # populate the motifs dictionary
        for wflow in timewindow_trajectories(reader_wflows,timewindow,timeformat):
            try:
                wflow = parse(wflow,timeformat)
                wflow = consolidate_txn_types(wflow, joins) if joins else wflow
                motif = get_motif(wflow,max_transfers=circulate)
                motifs = update_motifs(motifs,wflow,motif)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())
        # finalize the records
        motifs = finalize_motifs(motifs)
        # write the results
        write_motifs(motifs,motif_file,motif_header)

def update_motifs(motifs, wflow, motif):
    # update the motif summary
    motifs[motif]["flows"]    += 1
    motifs[motif]["amount"]   += wflow['trj_amt']
    motifs[motif]["deposits"] += wflow['trj_txn']
    motifs[motif]["users"].update(wflow['acct_IDs'][1:-1])
    motifs[motif]["durations"].append((wflow['trj_dur'],wflow['trj_amt'],wflow['trj_txn']))
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
    parser.add_argument('--prefix', default="", help='Prefix prepended to output filenames')
    parser.add_argument('--suffix', default="", help='Suffix appended to output filenames')
    parser.add_argument('--circulate', default=None, help='Consecutive transfers after which money is considered to "circulate", as an integer.')
    parser.add_argument('--timewindow', default="(,)", help='Include trajectories that begin within this time window, as a tuple.')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format used for timestamps in trajectory file & timewindow, as a string.')
    parser.add_argument('--join', action='append', default=[], help='Transaction types to join into one, as a tuple. (can be called multiple times)')
    parser.add_argument('--name', action='append', default=[], help='The name for the joined group, as a string. (called once for each --join)')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    motifs_filename = os.path.join(args.output_directory,args.prefix+"motifs"+args.suffix+".csv")

    timewindow = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow.strip('()').strip('[]').split(',')])

    if len(args.join) == len(args.name):
        joins = {join[0]:set(join[1].strip('()').strip('[]').split(',')) for join in zip(args.name,args.join)}
    else:
        raise IndexError("Please provide a name for each set of joined transaction types:",args.name,args.join)

    all_joins_list = []
    for join in joins:
        all_joins_list.extend(joins[join])
    if len(all_joins_list) != len(set(all_joins_list)):
        raise ValueError("Please do not duplicate joined transaction types:",args.join)

    ######### Creates weighted flow file #################
    find_motifs(wflow_filename,motifs_filename,circulate=args.circulate,timewindow=timewindow,timeformat=args.timeformat,joins=joins)
    #################################################
