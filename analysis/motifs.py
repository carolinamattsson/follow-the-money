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
def find_motifs(wflow_file,motif_file,issues_file,infer=False,join=False,circulate=6,save_motifs=[]):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_split_categs']
    motif_header = ["motif","flows","amount","deposits","users"]
    with open(wflow_file,'r') as wflow_file, open(issues_file,'w') as issues_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # motifs is a nested dictionary: split_categ -> motif_ID -> property -> value
        motifs = defaultdict(lambda: defaultdict(lambda: {"flows":0,"amount":0,"deposits":0,"users":set(),"details":[]}))
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
        # write the detailed duration lists
        if save_motifs: print_motif_details(motifs_filename,save_motifs,motifs)
        # write the summaries
        for split_categ in motifs:
            write_motifs(motifs[split_categ],motif_file,split_categ,motif_header)

def parse(wflow):
    #####################################################################################
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_acct_IDs']  = wflow['flow_acct_IDs'].strip('[]').split(',')
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
        if txn_types[0] in terms:  enter = "joined_"+str(i)
        if txn_types[-1] in terms: exit  = "joined_"+str(i)
    if len(txn_types) >= circulate:
        circ = "circulate"
    return "~".join([enter]+[circ]+[exit]) if circ else "~".join([enter]+[exit])

def update_motifs(motifs, split_categ, wflow, join, circulate):
    # define the motif
    motif = consolidate_motif(wflow['flow_txn_types'], join, circulate)
    # update the motif
    motifs[split_categ][motif]["flows"]    += 1
    motifs[split_categ][motif]["amount"]   += wflow['flow_amt']
    motifs[split_categ][motif]["deposits"] += wflow['flow_frac_root']
    motifs[split_categ][motif]["users"].update(wflow['flow_acct_IDs'][1:-1])
    motifs[split_categ][motif]["details"].append((wflow['flow_acct_IDs'][0],wflow['flow_acct_IDs'][-1],wflow['flow_amt'],wflow['flow_frac_root'],wflow['flow_duration']))
    return motifs

def combine_motifs(motifs,breakdowns):
    for split_categ in list(motifs):
        for motif in breakdowns:
            motifs['TOTAL'][motif]["flows"]    += motifs[split_categ][motif]["flows"]
            motifs['TOTAL'][motif]["amount"]   += motifs[split_categ][motif]["amount"]
            motifs['TOTAL'][motif]["deposits"] += motifs[split_categ][motif]["deposits"]
            motifs['TOTAL'][motif]["users"].union(motifs[split_categ][motif]["users"])
            motifs['TOTAL'][motif]["details"] = motifs['TOTAL'][motif]["details"]+motifs[split_categ][motif]["details"]
    return motifs

def finalize_motif(motif,motif_dict):
    motif_dict["motif"] = motif
    motif_dict["users"] = len(motif_dict["users"])
    del motif_dict["details"]
    return motif_dict

def print_motif_details(motifs_filename,save_motifs,motif_dict):
    for split_categ in motif_dict:
        motif_details_filename = motifs_filename.split('.csv')[0]+'_'+split_categ+'.details'
        with open(motif_details_filename,'w') as motif_details_file:
            motif_details_file.write('motif,entry,exit,flow_amt,frac_root,flow_dur\n')
            for motif in motif_dict[split_categ]:
                if any(txn_type in motif for txn_type in save_motifs):
                    for flow in motif_dict[split_categ][motif]["details"]:
                        motif_details_file.write(','.join([motif]+[str(x) for x in flow])+'\n')

def write_motifs(motifs,motifs_file,split_categ,motif_header):
    this_file = motifs_file.split(".csv")[0]+"_"+str(split_categ)+".csv"
    with open(this_file,'w') as this_file:
        writer_motif = csv.DictWriter(this_file,motif_header,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_motif.writeheader()
        # print distribution
        for motif in motifs:
            motif_dict = finalize_motif(motif,motifs[motif])
            writer_motif.writerow(motif_dict)

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
    parser.add_argument('--circulate', type=int, default=4, help='The length at which flows are considered to circulate -- longer ones are folded in.')
    parser.add_argument('--join', action='append', default=[], help='Enter & exit types with these terms are joined (takes tuples).')
    parser.add_argument('--save', action='append', default=[], help='Save to file the detailed list of flows that contain this type.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    motifs_filename = os.path.join(args.output_directory,args.prefix+"motifs.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"motifs.issues")

    args.join = [x.strip('()').split(',') for x in args.join]

    ######### Creates weighted flow file #################
    find_motifs(wflow_filename,motifs_filename,report_filename,infer=args.infer,join=args.join,circulate=args.circulate,save_motifs=args.save)
    #################################################
