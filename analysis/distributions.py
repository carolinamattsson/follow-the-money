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

#######################################################################################################
def find_distributions(wflow_file,dist_file,issues_file,digits_amt=2,digits_nrm=4,digits_dur=4,infer=False):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_split_categs']
    with open(wflow_file,'r') as wflow_file, open(issues_file,'w') as issues_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # dists is a nested dictionary: thing_we_are_counting -> split_split_categ -> value -> counts
        counting = ["root_amt","wflow_amt","wflow_nrm","duration"]
        dists = {thing:defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0))) for thing in counting}
        # populate the dists dictionary
        for split_categ, wflow in split_by_month(reader_wflows,infer):
            try:
                wflow = parse(wflow)
                dists = update_dists(dists,split_categ,wflow,digits_amt,digits_nrm,digits_dur)
            except:
                writer_issues.writerow([wflow[term] for term in wflow]+[traceback.format_exc()])
        # get all the transaction types seen in any split_category
        breakdowns = {}
        for counting in dists:
            breakdowns[counting] = set()
            for split_categ in dists[counting]:
                for value in dists[counting][split_categ]:
                    breakdowns[counting].update(dists[counting][split_categ][value].keys())
        # create an overall dists dictionary, and fill in any gaps in the others
        dists = combine_dists(dists,breakdowns,digits_amt,digits_nrm,digits_dur)
        # write the piecharts
        for counting in dists:
            for split_categ in dists[counting]:
                write_dist(dists[counting][split_categ],dist_file,counting,split_categ,breakdowns[counting])

def parse(wflow):
    #####################################################################################
    wflow['flow_txn_IDs']   = wflow['flow_txn_IDs'].strip('[]').split(',')
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_rev_fracs'] = [float(frac) for frac in wflow['flow_rev_fracs'].strip('[]').split(',')]
    wflow['flow_amt']       = float(wflow['flow_amt'])
    wflow['flow_frac_root'] = float(wflow['flow_frac_root'])
    wflow['flow_categs']    = tuple(wflow['flow_categs'].strip('()').split(','))
    wflow['flow_duration']  = float(wflow['flow_duration'])
    wflow['flow_durations'] = [] if wflow['flow_durations'] == "[]" else [float(dur) for dur in wflow['flow_durations'].strip('[]').split(',')]
    wflow['flow_length']    = len(wflow['flow_txn_IDs'])
    wflow['flow_type']      = None
    if wflow['flow_length'] == 1:
        wflow['flow_type'] = 'OTC'
    elif wflow['flow_length'] == 2 and wflow['flow_duration'] < 24:
        wflow['flow_type'] = 'OTC_P2P'
    elif wflow['flow_length'] == 2:
        wflow['flow_type'] = 'non_P2P'
    elif wflow['flow_length'] > 2:
        wflow['flow_type'] = 'P2P'
    return wflow

def update_dists(dists, split_categ, wflow, digits_amt, digits_nrm, digits_dur):
    rounded_amt  = 10**round(math.log(wflow['flow_amt'],10),digits_amt) if wflow['flow_amt'] else 0
    rounded_nrm  = round(wflow['flow_frac_root'],digits_nrm)
    rounded_dur  = 10**round(math.log(wflow['flow_duration'],10),digits_dur) if wflow['flow_duration'] else 0
    rounded_root = 10**round(math.log(wflow['flow_amt']/wflow['flow_frac_root'],10),digits_amt) if wflow['flow_amt'] else 0
    # fracs & deposits
    dists["root_amt"][split_categ][rounded_root]['_roots'] += wflow['flow_frac_root']
    dists["root_amt"][split_categ][rounded_root]['_roots_'+wflow['flow_txn_types'][0]] += wflow['flow_frac_root']
    if wflow['flow_categs'][0] == 'deposit':
        dists["root_amt"][split_categ][rounded_root]['_deposits'] += wflow['flow_frac_root']
        dists["root_amt"][split_categ][rounded_root]['_deposits_'+wflow['flow_txn_types'][0]] += wflow['flow_frac_root']
    # flows, by amount
    dists['wflow_amt'][split_categ][rounded_amt]['__obs'] += 1
    dists['wflow_amt'][split_categ][rounded_amt]['start_'+wflow['flow_txn_types'][0]] += 1
    dists['wflow_amt'][split_categ][rounded_amt]['end_'+wflow['flow_txn_types'][-1]]  += 1
    if wflow['flow_type']: dists['wflow_amt'][split_categ][rounded_amt]['type_'+wflow['flow_type']] += 1
    # flows, by fraction of frac transaction
    dists['wflow_nrm'][split_categ][rounded_nrm]['__obs'] += 1
    dists['wflow_nrm'][split_categ][rounded_nrm]['start_'+wflow['flow_txn_types'][0]] += 1
    dists['wflow_nrm'][split_categ][rounded_nrm]['end_'+wflow['flow_txn_types'][-1]]  += 1
    if wflow['flow_type']: dists['wflow_nrm'][split_categ][rounded_nrm]['type_'+wflow['flow_type']] += 1
    # duration
    flow_prev_dur  = 0
    flow_prev_amt  = wflow['flow_amt']
    flow_prev_nrm  = wflow['flow_frac_root']
    last = len(wflow['flow_durations'])-1
    for i,flow_this_dur in enumerate(wflow['flow_durations']):
        flow_this_dur = 10**round(math.log(flow_this_dur,10),digits_dur) if flow_this_dur else 0
        flow_this_amt = wflow['flow_amt']*(1-wflow['flow_rev_fracs'][i])
        flow_this_nrm = wflow['flow_frac_root']*(1-wflow['flow_rev_fracs'][i])
        dists['duration'][split_categ][flow_prev_dur]['__system_amt'] += flow_prev_amt - flow_this_amt
        dists['duration'][split_categ][flow_prev_dur]['__system_nrm'] += flow_prev_nrm - flow_this_nrm
        dists['duration'][split_categ][flow_this_dur]['__node_amt']  += flow_this_amt
        dists['duration'][split_categ][flow_this_dur]['__node_nrm']  += flow_this_nrm
        if i == 0 and wflow['flow_categs'][0] == 'deposit':
            if wflow['flow_length'] == 2 and wflow['flow_categs'][1] == 'withdraw':
                dists['duration'][split_categ][flow_this_dur]['_inout_amt'] += flow_this_amt
                dists['duration'][split_categ][flow_this_dur]['_inout_nrm'] += flow_this_nrm
            else:
                dists['duration'][split_categ][flow_this_dur]['_in_amt'] += flow_this_amt
                dists['duration'][split_categ][flow_this_dur]['_in_nrm'] += flow_this_nrm
        elif i == last and wflow['flow_categs'][1] == 'withdraw':
            dists['duration'][split_categ][flow_this_dur]['_out_amt'] += flow_this_amt
            dists['duration'][split_categ][flow_this_dur]['_out_nrm'] += flow_this_nrm
        else:
            dists['duration'][split_categ][flow_this_dur]['_p2p_amt'] += flow_this_amt
            dists['duration'][split_categ][flow_this_dur]['_p2p_nrm'] += flow_this_nrm
        dists['duration'][split_categ][flow_this_dur]['post_'+wflow['flow_txn_types'][i]+'_amt']  += flow_this_amt
        dists['duration'][split_categ][flow_this_dur]['post_'+wflow['flow_txn_types'][i]+'_nrm']  += flow_this_nrm
        dists['duration'][split_categ][flow_this_dur]['pre_'+wflow['flow_txn_types'][i+1]+'_amt'] += flow_this_amt
        dists['duration'][split_categ][flow_this_dur]['pre_'+wflow['flow_txn_types'][i+1]+'_nrm'] += flow_this_nrm
        flow_prev_dur  = 10**round(math.log(flow_prev_dur+flow_this_dur,10),digits_dur) if flow_prev_dur+flow_this_dur else 0
        flow_prev_amt  = flow_this_amt
        flow_prev_nrm  = flow_this_nrm
    dists['duration'][split_categ][flow_prev_dur]['__system_amt'] += flow_prev_amt
    dists['duration'][split_categ][flow_prev_dur]['__system_nrm'] += flow_prev_nrm
    return dists

def combine_dists(dists,breakdowns,digits_amt,digits_nrm,digits_dur):
    for counting in dists:
        for split_categ in list(dists[counting]):
            for value in dists[counting][split_categ]:
                for breakdown in breakdowns[counting]:
                    dists[counting]['TOTAL'][value][breakdown] += dists[counting][split_categ][value][breakdown]
        for split_categ in dists[counting]:
            for value in dists[counting][split_categ]:
                for breakdown in breakdowns[counting]:
                    if breakdown[-4:] == "_amt": dists[counting][split_categ][value][breakdown] = round(dists[counting][split_categ][value][breakdown],digits_amt)
                    if breakdown[-4:] == "_nrm": dists[counting][split_categ][value][breakdown] = round(dists[counting][split_categ][value][breakdown],digits_nrm)
    return dists

def write_dist(dist,dist_file,counting,split_categ,breakdown):
    this_dist_file = dist_file.split(".csv")[0]+"_"+counting+"_"+str(split_categ)+".csv"
    with open(this_dist_file,'w') as this_dist_file:
        header_dist = ["___"+counting]+[term for term in sorted(list(breakdown))]
        writer_dist = csv.DictWriter(this_dist_file,header_dist,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_dist.writerow({term:term.strip('_') for term in header_dist})
        # print distribution
        for value in dist:
            dist[value]["___"+counting] = value
            writer_dist.writerow(dist[value])

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
    parser.add_argument('--digits_amt', type=int, default=3, help='Decimal places in rounded amount')
    parser.add_argument('--digits_nrm', type=int, default=3, help='Decimal places in rounded normalized amount (ie. fraction of root transaction)')
    parser.add_argument('--digits_dur', type=int, default=3, help='Decimal places in rounded hours of duration')
    parser.add_argument('--infer', action="store_true", default=False, help='Include flows that begin or end with inferred transactions')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    dists_filename = os.path.join(args.output_directory,args.prefix+"dist.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"dist_issues.txt")

    ######### Creates weighted flow file #################
    find_distributions(wflow_filename,dists_filename,report_filename,digits_amt=int(args.digits_amt),digits_nrm=int(args.digits_nrm),digits_dur=int(args.digits_dur),infer=args.infer)
    #################################################
