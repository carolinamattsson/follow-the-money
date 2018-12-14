from datetime import datetime, timedelta
from collections import defaultdict
from collections import deque
import traceback

#######################################################################################################
def split_by_month(wflows,infer):
    for wflow in wflows:
        if not infer and "inferred" in wflow["flow_txn_types"]:
            continue
        month_ID = "-".join(wflow['flow_timestamp'].split("-")[:-1])
        yield month_ID, wflow

def get_days(flow_timestamp,flow_duration,timeformat,instant=0):
    if flow_duration <= instant:
        return 0
    else:
        start_timestamp = datetime.strptime(flow_timestamp,timeformat)
        end_timestamp   = start_timestamp + timedelta(hours=flow_duration)
        days = (end_timestamp.date()-start_timestamp.date()).days
        return days + 1

#######################################################################################################
def daychart_by_categ(wflow_file,daychart_file,issues_file,day_list,timeformat,instant,infer):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_categs']
    daychart_header = ['day','txn_type','amount','normalized','accounts']
    day_list.sort()
    with open(wflow_file,'r') as wflow_file, open(issues_file,'w') as issues_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # daycharts is a rather nested dictionary: split_categ -> txn_type -> day -> base_dictionary
        daycharts = defaultdict(lambda: defaultdict(lambda: {day:{'amount':0,'normalized':0,'accounts':set()} for day in day_list}))
        # populate the daychart dictionary
        for categ, wflow in split_by_month(reader_wflows,infer):
            try:
                wflow = parse(wflow)
                daycharts = update_daychart(daycharts, categ, wflow, day_list, timeformat, instant)
            except:
                writer_issues.writerow([wflow[term] for term in wflow]+[traceback.format_exc()])
        # get all the categories seen
        categs = set(daycharts.keys())
        # get all the transaction types seen in any category
        txn_types = set()
        for categ in daycharts:
            txn_types.update(daycharts[categ].keys())
        # create an overall daychart dictionary, and fill in any gaps in the others
        daycharts = combine_charts(daycharts,categs,txn_types)
        # calculating diagnostics for each daychart dictionary
        day_diagnostics = summarize_charts(daycharts)
        # finalize the daychart dictionary
        daycharts = finalize_charts(daycharts)
        # write the daycharts
        for categ in daycharts:
            this_daychart_file = daychart_file.split(".csv")[0]+"_"+str(categ)+".csv"
            write_daychart(day_diagnostics[categ],daycharts[categ],this_daychart_file,daychart_header)

def parse(wflow):
    #####################################################################################
    wflow['flow_acct_IDs']  = wflow['flow_acct_IDs'].strip('[]').split(',')
    wflow['flow_txn_IDs']   = wflow['flow_txn_IDs'].strip('[]').split(',')
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_rev_fracs'] = [float(frac) for frac in wflow['flow_rev_fracs'].strip('[]').split(',')]
    wflow['flow_amt']       = float(wflow['flow_amt'])
    wflow['flow_frac_root'] = float(wflow['flow_frac_root'])
    wflow['flow_duration']  = float(wflow['flow_duration'])
    wflow['flow_durations'] = [] if wflow['flow_durations'] == "[]" else [float(dur) for dur in wflow['flow_durations'].strip('[]').split(',')]
    wflow['flow_categs']    = tuple(wflow['flow_categs'].strip('()').split(','))
    return wflow

def update_daychart(daycharts, categ, wflow, day_list, timeformat, instant):
    #####################################################################################
    day_list = deque(day_list)
    # first, note the money at moment of entry to allow for instantaneous in-outs
    flow_dur  = 0
    flow_prev_amt  = wflow['flow_amt']
    flow_prev_nrm  = wflow['flow_frac_root']
    wflow['flow_durations'] = [0]+wflow['flow_durations']
    entry = day_list.popleft() if day_list[0] == 0 else 0
    # now, the record the money at the all the points in day_list
    for i,flow_this_dur in enumerate(wflow['flow_durations']):
        flow_dur = flow_dur+flow_this_dur
        while get_days(wflow['flow_timestamp'],flow_dur,timeformat,instant) > entry:
            daycharts[categ][flow_this_type][entry]['amount']    += flow_this_amt
            daycharts[categ][flow_this_type][entry]['normalized'] += flow_this_nrm
            daycharts[categ][flow_this_type][entry]['accounts'].add(flow_this_acct)
            daycharts[categ]['FEE'][entry]['amount']             += flow_prev_amt-flow_this_amt
            daycharts[categ]['FEE'][entry]['normalized']          += flow_prev_nrm-flow_this_nrm
            flow_prev_amt = flow_this_amt
            flow_prev_nrm = flow_this_nrm
            try:
                entry = day_list.popleft()
            except:
                break
        flow_this_amt  = wflow['flow_amt']*(1-wflow['flow_rev_fracs'][i])
        flow_this_nrm  = wflow['flow_frac_root']*(1-wflow['flow_rev_fracs'][i])
        flow_this_acct = wflow['flow_acct_IDs'][i]
        flow_this_type = wflow['flow_txn_types'][i]
    while get_days(wflow['flow_timestamp'],flow_dur,timeformat,instant) >= entry:
        daycharts[categ][flow_this_type][entry]['amount']    += flow_this_amt
        daycharts[categ][flow_this_type][entry]['normalized'] += flow_this_nrm
        daycharts[categ][flow_this_type][entry]['accounts'].add(flow_this_acct)
        daycharts[categ]['FEE'][entry]['amount']             += flow_prev_amt-flow_this_amt
        daycharts[categ]['FEE'][entry]['normalized']          += flow_prev_nrm-flow_this_nrm
        flow_prev_amt = flow_this_amt
        flow_prev_nrm = flow_this_nrm
        try:
            entry = day_list.popleft()
        except:
            pass
    return daycharts

def combine_charts(charts,categs,txn_types):
    for categ in categs:
        for txn_type in txn_types:
            for day in charts[categ][txn_type]:
                # continuous terms
                charts['TOTAL'][txn_type][day]['amount']    += charts[categ][txn_type][day]['amount']
                charts['TOTAL'][txn_type][day]['normalized'] += charts[categ][txn_type][day]['normalized']
                # discrete terms
                charts['TOTAL'][txn_type][day]['accounts'].update(charts[categ][txn_type][day]['accounts'])
    return charts

def summarize_charts(charts):
    diagnostics = defaultdict(lambda: defaultdict(lambda: {'amount':0,'normalized':0,'accounts':set()}))
    for categ in charts:
        for txn_type in charts[categ]:
            for day in charts[categ][txn_type]:
                # continuous terms
                diagnostics[categ][txn_type]['amount']    += charts[categ][txn_type][day]['amount']
                diagnostics[categ][txn_type]['normalized'] += charts[categ][txn_type][day]['normalized']
                # discrete terms
                diagnostics[categ][txn_type]['accounts'].update(charts[categ][txn_type][day]['accounts'])
            # continuous terms
            diagnostics[categ]['TOTAL']['amount']    += diagnostics[categ][txn_type]['amount']
            diagnostics[categ]['TOTAL']['normalized'] += diagnostics[categ][txn_type]['normalized']
            # discrete terms
            diagnostics[categ]['TOTAL']['accounts'].update(diagnostics[categ][txn_type]['accounts'])
    for categ in diagnostics:
        for txn_type in diagnostics[categ]:
            # discrete terms
            diagnostics[categ][txn_type]['accounts'] = len(diagnostics[categ][txn_type]['accounts'])
    return diagnostics

def finalize_charts(charts):
    for categ in charts:
        for txn_type in charts[categ]:
            for day in charts[categ][txn_type]:
                # discrete terms
                charts[categ][txn_type][day]['accounts'] = len(charts[categ][txn_type][day]['accounts'])
    return charts

def write_daychart(diagnostics,daychart,daychart_file,daychart_header):
    with open(daychart_file,'w') as daychart_file:
        writer_daychart = csv.DictWriter(daychart_file,daychart_header,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_daychart.writeheader()
        # print diagnostics
        for txn_type in diagnostics:
            writer_daychart.writerow({'day':'TOTAL','txn_type':txn_type,'amount':diagnostics[txn_type]['amount'],\
                                                                        'normalized':diagnostics[txn_type]['normalized'],\
                                                                        'accounts':diagnostics[txn_type]['accounts']})
        # print diagnostics
        for txn_type in daychart:
            for day in daychart[txn_type]:
                writer_daychart.writerow({'day':day,'txn_type':txn_type,'amount':daychart[txn_type][day]['amount'],\
                                                                        'normalized':daychart[txn_type][day]['normalized'],\
                                                                        'accounts':daychart[txn_type][day]['accounts']})

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
    parser.add_argument('--day_list', default='0,1,2,4,7,30,90', help='Time cutoffs (in days) separated by commas. Must start with zero. Integers only.')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Timeformat of the flow timestamp, if different from %Y-%m-%d %H:%M:%S')
    parser.add_argument('--instant', type=float, default=0, help='Durations less than or equal to this value (in hours) are considered instant')
    parser.add_argument('--infer', action="store_true", default=False, help='Include flows that begin or end with inferred transactions')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    daychart_filename = os.path.join(args.output_directory,args.prefix+"chart_days.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"chart_days_issues.txt")

    args.day_list = [int(cutoff) for cutoff in args.day_list.split(",")]

    ######### Creates weighted flow file #################
    daychart_by_categ(wflow_filename,daychart_filename,report_filename,args.day_list,args.timeformat,args.instant,args.infer)
    #################################################
