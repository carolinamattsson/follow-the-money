from collections import defaultdict
import traceback

#######################################################################################################
def split_by_month(wflows,infer):
    for wflow in wflows:
        if not infer and "inferred" in wflow["flow_txn_types"]:
            continue
        month_ID = "-".join(wflow['flow_timestamp'].split("-")[:-1])
        yield month_ID, wflow

#######################################################################################################
def piechart_by_categ(wflow_file,piechart_file,issues_file,max_hops=6,infer=False):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_categs']
    piechart_header = ['hop','txn_type','amount','frac_root','txns','senders']
    with open(wflow_file,'r') as wflow_file, open(issues_file,'w') as issues_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # piecharts is a rather nested dictionary: split_categ -> txn_type -> hop -> base_dictionary
        piecharts = defaultdict(lambda: defaultdict(lambda: {hop:{'amount':0,'frac_root':0,'txns':set(),'senders':set()} for hop in range(max_hops+1)}))
        # populate the piechart dictionary
        for categ, wflow in split_by_month(reader_wflows,infer):
            try:
                wflow = parse(wflow)
                piecharts = update_piechart(piecharts, categ, wflow, max_hops)
            except:
                writer_issues.writerow([wflow[term] for term in wflow]+[traceback.format_exc()])
        # get all the categories seen
        categs = set(piecharts.keys())
        # get all the transaction types seen in any category
        txn_types = set()
        for categ in piecharts:
            txn_types.update(piecharts[categ].keys())
        # create an overall piechart dictionary, and fill in any gaps in the others
        piecharts = combine_piecharts(piecharts,categs,txn_types)
        # calculating diagnostics for each piechart dictionary
        diagnostics = summarize_piecharts(piecharts)
        # finalize the piechart dictionary
        piecharts = finalize_piechart(piecharts)
        # write the piecharts
        for categ in piecharts:
            this_piechart_file = piechart_file.split(".csv")[0]+"_"+str(categ)+".csv"
            write_piechart(diagnostics[categ],piecharts[categ],this_piechart_file,piechart_header)

def parse(wflow):
    #####################################################################################
    wflow['flow_acct_IDs']  = wflow['flow_acct_IDs'].strip('[]').split(',')
    wflow['flow_txn_IDs']   = wflow['flow_txn_IDs'].strip('[]').split(',')
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_rev_fracs'] = wflow['flow_rev_fracs'].strip('[]').split(',')
    wflow['flow_amt']       = float(wflow['flow_amt'])
    wflow['flow_frac_root'] = float(wflow['flow_frac_root'])
    wflow['flow_categs']    = tuple(wflow['flow_categs'].strip('()').split(','))
    return wflow

def update_piechart(piecharts, categ, wflow, max_hops):
    #####################################################################################
    for i,txn_type in enumerate(wflow['flow_txn_types']):
        # adjust for whether or not the weighted flow begins with a deposit (hop = 0)
        hop = i if wflow['flow_categs'][0] == 'deposit' else i+1
        # limit hops to max_hops
        if hop > max_hops:
            break
        # update aggregates
        if wflow['flow_txn_IDs'][i]:
            # continuous terms
            rev_frac = float(wflow['flow_rev_fracs'][i]) - float(wflow['flow_rev_fracs'][i-1]) if (i > 0 and wflow['flow_rev_fracs'][i-1]) else float(wflow['flow_rev_fracs'][i])
            piecharts[categ][txn_type][hop]['amount']    += wflow['flow_amt']*(1-rev_frac)
            piecharts[categ]['FEE'][hop]['amount']       += wflow['flow_amt']*(rev_frac)
            piecharts[categ][txn_type][hop]['frac_root'] += wflow['flow_frac_root']*(1-rev_frac)
            piecharts[categ]['FEE'][hop]['frac_root']    += wflow['flow_frac_root']*(rev_frac)
            # discrete terms
            piecharts[categ][txn_type][hop]['txns'].add(wflow['flow_txn_IDs'][i])
            if wflow['flow_acct_IDs'][i]:
                piecharts[categ][txn_type][hop]['senders'].add(wflow['flow_acct_IDs'][i])
    return piecharts

def combine_piecharts(piecharts,categs,txn_types):
    for categ in categs:
        for txn_type in txn_types:
            for hop in piecharts[categ][txn_type]:
                # continuous terms
                piecharts['TOTAL'][txn_type][hop]['amount']    += piecharts[categ][txn_type][hop]['amount']
                piecharts['TOTAL'][txn_type][hop]['frac_root'] += piecharts[categ][txn_type][hop]['frac_root']
                # discrete terms
                piecharts['TOTAL'][txn_type][hop]['txns'].update(piecharts[categ][txn_type][hop]['txns'])
                piecharts['TOTAL'][txn_type][hop]['senders'].update(piecharts[categ][txn_type][hop]['senders'])
    return piecharts

def summarize_piecharts(piecharts):
    diagnostics = defaultdict(lambda: defaultdict(lambda: {'amount':0,'frac_root':0,'txns':set(),'senders':set()}))
    for categ in piecharts:
        for txn_type in piecharts[categ]:
            for hop in piecharts[categ][txn_type]:
                # continuous terms
                diagnostics[categ][txn_type]['amount']   += piecharts[categ][txn_type][hop]['amount']
                diagnostics[categ][txn_type]['frac_root'] += piecharts[categ][txn_type][hop]['frac_root']
                # discrete terms
                diagnostics[categ][txn_type]['txns'].update(piecharts[categ][txn_type][hop]['txns'])
                diagnostics[categ][txn_type]['senders'].update(piecharts[categ][txn_type][hop]['senders'])
            # continuous terms
            diagnostics[categ]['TOTAL']['amount']   += diagnostics[categ][txn_type]['amount']
            diagnostics[categ]['TOTAL']['frac_root'] += diagnostics[categ][txn_type]['frac_root']
            # discrete terms
            diagnostics[categ]['TOTAL']['txns'].update(diagnostics[categ][txn_type]['txns'])
            diagnostics[categ]['TOTAL']['senders'].update(diagnostics[categ][txn_type]['senders'])
    for categ in diagnostics:
        for txn_type in diagnostics[categ]:
            # discrete terms
            diagnostics[categ][txn_type]['txns']    = len(diagnostics[categ][txn_type]['txns'])
            diagnostics[categ][txn_type]['senders'] = len(diagnostics[categ][txn_type]['senders'])
    return diagnostics

def finalize_piechart(piechart):
    for categ in piechart:
        for txn_type in piechart[categ]:
            for hop in piechart[categ][txn_type]:
                # discrete terms
                piechart[categ][txn_type][hop]['txns']    = len(piechart[categ][txn_type][hop]['txns'])
                piechart[categ][txn_type][hop]['senders'] = len(piechart[categ][txn_type][hop]['senders'])
    return piechart

def write_piechart(diagnostics,piechart,piechart_file,piechart_header):
    with open(piechart_file,'w') as piechart_file:
        writer_piechart = csv.DictWriter(piechart_file,piechart_header,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_piechart.writeheader()
        # print diagnostics
        for txn_type in diagnostics:
            writer_piechart.writerow({'hop':'TOTAL','txn_type':txn_type,'amount':diagnostics[txn_type]['amount'],\
                                                                        'frac_root':diagnostics[txn_type]['frac_root'],\
                                                                        'txns':diagnostics[txn_type]['txns'],\
                                                                        'senders':diagnostics[txn_type]['senders']})
        # print diagnostics
        for txn_type in piechart:
            for hop in piechart[txn_type]:
                writer_piechart.writerow({'hop':hop,'txn_type':txn_type,'amount':piechart[txn_type][hop]['amount'],\
                                                                        'frac_root':piechart[txn_type][hop]['frac_root'],\
                                                                        'txns':piechart[txn_type][hop]['txns'],\
                                                                        'senders':piechart[txn_type][hop]['senders']})

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
    parser.add_argument('--max_hops', type=int, default=6, help='Create a bar chart file out to this step')
    parser.add_argument('--infer', action="store_true", default=False, help='Include flows that begin or end with inferred transactions')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    chart_filename = os.path.join(args.output_directory,args.prefix+"chart.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"chart_issues.txt")

    ######### Creates weighted flow file #################
    piechart_by_categ(wflow_filename,chart_filename,report_filename,max_hops=int(args.max_hops),infer=args.infer)
    #################################################
