from collections import defaultdict
import traceback
import math

#######################################################################################################
def split_by_month(wflows,infer):
    for wflow in wflows:
        if not infer and "inferred" in wflow["flow_txn_types"]:
            continue
        month_ID = "-".join(wflow['flow_timestamp'].split("-")[:-1])
        yield month_ID, wflow

def cumsum(a_list):
    total = 0
    for x in a_list:
        total += x
        yield total

#######################################################################################################
def find_motifs(wflow_file,motif_file,issues_file,infer=False,join=False,circulate=6):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_split_categs']
    motif_header = ["motif","flows","amount","deposits","users","median_dur_f","median_dur_a","median_dur_d"]
    with open(wflow_file,'r') as wflow_file, open(issues_file,'w') as issues_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # motifs is a nested dictionary: split_categ -> motif_ID -> property -> value
        motifs = defaultdict(lambda: defaultdict(lambda: {"flows":0,"amount":0,"deposits":0,"users":set(),"durations":[]}))
        # populate the motifs dictionary
        for split_categ, wflow in split_by_month(reader_wflows,infer):
            try:
                wflow = parse(wflow)
                motifs = update_motifs(motifs,split_categ,wflow,join,circulate)
            except:
                writer_issues.writerow([wflow[term] for term in wflow]+[traceback.format_exc()])
        # get all the transaction types seen in any split_category
        breakdowns = set()
        for split_categ in motifs:
            breakdowns.update(motifs[split_categ].keys())
        # create an overall dists dictionary, and fill in any gaps in the others
        motifs = combine_motifs(motifs,breakdowns)
        # write the piecharts
        for split_categ in motifs:
            write_motifs(motifs[split_categ],motif_file,split_categ,motif_header)

def parse(wflow):
    #####################################################################################
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_amt']       = float(wflow['flow_amt'])
    wflow['flow_frac_root'] = float(wflow['flow_frac_root'])
    wflow['flow_duration']  = float(wflow['flow_duration'])
    wflow['flow_length']    = len(wflow['flow_txn_types'])
    return wflow

def consolidate_motif(txn_types, join, circulate):
    enter = txn_types[0]
    exit  = txn_types[-1]
    circ  = "~".join(txn_types[1:-1])
    for i,terms in enumerate(join):
        if wflow['flow_txn_types'][0] in terms:  enter = "joined_"+str(i)
        if wflow['flow_txn_types'][-1] in terms: exit  = "joined_"+str(i)
    if wflow['flow_length'] >= circulate:        circ = "circulate"
    return "~".join([enter]+[circ]+[exit]) if circ else "~".join([enter]+[exit])

def update_motifs(motifs, split_categ, wflow, join, circulate):
    # define the motif
    motif = consolidate_motif(wflow['flow_txn_types'], join, circulate)
    # update the motif
    motifs[split_categ][motif]["flows"]    += 1
    motifs[split_categ][motif]["amount"]   += wflow['flow_amt']
    motifs[split_categ][motif]["deposits"] += wflow['flow_frac_root']
    motifs[split_categ][motif]["users"].update(wflow['flow_acct_IDs'][1:-1])
    motifs[split_categ][motif]["durations"].append((wflow['flow_duration'],wflow['flow_amt'],wflow['flow_frac_root']))
    return motifs

def combine_motifs(motifs,breakdowns):
    for split_categ in list(motifs):
        for motif in breakdowns:
            motifs['TOTAL'][motif]["flows"]    += motifs[split_categ][motif]["flows"]
            motifs['TOTAL'][motif]["amount"]   += motifs[split_categ][motif]["amount"]
            motifs['TOTAL'][motif]["deposits"] += motifs[split_categ][motif]["deposits"]
            motifs['TOTAL'][motif]["users"].union(motifs[split_categ][motif]["users"])
            motifs['TOTAL'][motif]["durations"] = motifs['TOTAL'][motif]["durations"]+motifs[split_categ][motif]["durations"]
    return motifs

def finalize_motif(motif,motif_dict):
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
    return motif_dict

def write_motifs(motifs,motifs_file,split_categ,motif_header):
    this_file = motifs_file.split(".csv")[0]+"_"+str(split_categ)+".csv"
    with open(this_file,'w') as this_file:
        writer_motif = csv.DictWriter(this_file,motif_header,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_motif.writeheader()
        # print distribution
        for motif in motifs:
            writer_motif.writerow(finalize_motif(motif,motifs[motif]))

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
    parser.add_argument('--infer', action="store_true", default=False, help='Include flows that begin or end with inferred transactions')
    parser.add_argument('--circulate', type=int, default=6, help='The length at which flows are considered to circulate -- longer ones are folded in.')
    parser.add_argument('--join', action='append', default=[], help='Enter & exit types with these terms are joined (takes tuples).')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    motifs_filename = os.path.join(args.output_directory,args.prefix+"motifs.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"motifs_issues.txt")

    args.join = [x.strip('()').split(',') for x in args.join]

    ######### Creates weighted flow file #################
    find_motifs(wflow_filename,motifs_filename,report_filename,infer=args.infer,join=args.join,circulate=args.circulate)
    #################################################
