from datetime import datetime, timedelta
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
def find_agents(wflow_file,agent_file,issues_file,infer=False,join=False,circulate=4,sources=[],sinks=[],timeformat="%Y-%m-%d %H:%M:%S",instant=0):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_split_categs']
    with open(wflow_file,'r') as wflow_file, open(issues_file,'w') as issues_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # agents is a nested dictionary: split_categ -> agent_ID -> property -> value
        agents = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))
        # populate the agents dictionary
        for split_categ, wflow in split_by_month(reader_wflows,infer):
            try:
                wflow = parse(wflow,sources,sinks)
                agents = update_agents(agents,split_categ,wflow,join,circulate,timeformat,instant)
            except:
                writer_issues.writerow([wflow[term] for term in wflow]+[traceback.format_exc()])
        # get all the transaction types seen in any split_category
        breakdowns = {}
        for split_categ in agents:
            breakdowns[split_categ] = set()
            for agent in agents[split_categ]:
                breakdowns[split_categ].update(agents[split_categ][agent].keys())
        breakdowns['TOTAL'] = set()
        for split_categ in agents:
            breakdowns['TOTAL'] = breakdowns['TOTAL'].union(breakdowns[split_categ])
        # create an overall dists dictionary, and fill in any gaps in the others
        agents = combine_agents(agents,breakdowns)
        # divide out the amount from the duration*amount
        dur_breakdowns = {}
        for split_categ in breakdowns:
            dur_breakdowns[split_categ] = {term for term in breakdowns[split_categ] if '_dur' in term}
            for dur_term in dur_breakdowns[split_categ]:
                breakdowns[split_categ].update([dur_term.split('_dur')[0]+'_median_dur_a',dur_term.split('_dur')[0]+'_median_dur_d'])
                breakdowns[split_categ].remove(dur_term)
        # write the piecharts
        for split_categ in agents:
            write_agents(agents[split_categ],agent_file,split_categ,breakdowns[split_categ],dur_breakdowns[split_categ])

def parse(wflow,sources,sinks):
    #####################################################################################
    wflow['flow_acct_IDs']   = wflow['flow_acct_IDs'].strip('[]').split(',')
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_rev_fracs'] = [float(frac) for frac in wflow['flow_rev_fracs'].strip('[]').split(',')]
    wflow['flow_amt']       = float(wflow['flow_amt'])
    wflow['flow_frac_root'] = float(wflow['flow_frac_root'])
    wflow['flow_categs']    = tuple(wflow['flow_categs'].strip('()').split(','))
    wflow['flow_duration']  = float(wflow['flow_duration'])
    wflow['flow_durations'] = [] if wflow['flow_durations'] == "[]" else [float(dur) for dur in wflow['flow_durations'].strip('[]').split(',')]
    wflow['flow_length']    = len(wflow['flow_txn_IDs'])
    # note when the source/sinks are the provider instead
    if wflow['flow_txn_types'][-1] in sinks:
        wflow['flow_acct_IDs'][-1] = wflow['flow_txn_types'][-1]
    if wflow['flow_txn_types'][0] in sources:
        wflow['flow_acct_IDs'][0] = wflow['flow_txn_types'][0]
    return wflow

def get_days(flow_timestamp,flow_duration,timeformat,instant=0):
    if flow_duration <= instant:
        return 0
    else:
        start_timestamp = datetime.strptime(flow_timestamp,timeformat)
        end_timestamp   = start_timestamp + timedelta(hours=flow_duration)
        days = (end_timestamp.date()-start_timestamp.date()).days
        return days + 1

def consolidate_motif(txn_types, join, circulate):
    enter = txn_types[0]
    exit  = txn_types[-1]
    circ  = "~".join(txn_types[1:-1])
    for i,terms in enumerate(join):
        if txn_types[0] in terms:   enter = "joined_"+str(i)
        if txn_types[-1] in terms:  exit  = "joined_"+str(i)
    if len(txn_types) >= circulate: circ = "circulate"
    return "~".join([enter]+[circ]+[exit]) if circ else "~".join([enter]+[exit])

def update_agents(agents, split_categ, wflow, join, circulate, timeformat, instant):
    # define the motif
    motif = consolidate_motif(wflow['flow_txn_types'], join, circulate)
    # for the source agent
    source = wflow['flow_acct_IDs'][0]
    agents[split_categ][source]['__source_amt'] += wflow['flow_amt']
    agents[split_categ][source]['__source_nrm'] += wflow['flow_frac_root']
    if agents[split_categ][source]['__source_dur'] == 0:
        agents[split_categ][source]['__source_dur'] = []
    agents[split_categ][source]['__source_dur'].append((wflow['flow_duration'],wflow['flow_amt'],wflow['flow_frac_root']))
    # also by motif
    agents[split_categ][source]['source_'+motif+'_amt'] += wflow['flow_amt']
    agents[split_categ][source]['source_'+motif+'_nrm'] += wflow['flow_frac_root']
    if agents[split_categ][source]['source_'+motif+'_dur'] == 0:
        agents[split_categ][source]['source_'+motif+'_dur'] = []
    agents[split_categ][source]['source_'+motif+'_dur'].append((wflow['flow_duration'],wflow['flow_amt'],wflow['flow_frac_root']))
    # special check for potential P2PF
    if motif == "CASHIN~CASHOUT" and get_days(wflow['flow_timestamp'],wflow['flow_duration'],timeformat,instant) == 1:
        agents[split_categ][source]['source_1user1day_amt'] += wflow['flow_amt']
        agents[split_categ][source]['source_1user1day_nrm'] += wflow['flow_frac_root']
        if agents[split_categ][source]['source_1user1day_dur'] == 0:
            agents[split_categ][source]['source_1user1day_dur'] = []
        agents[split_categ][source]['source_1user1day_dur'].append((wflow['flow_duration'],wflow['flow_amt'],wflow['flow_frac_root']))
    # and now the taget :)
    sink = wflow['flow_acct_IDs'][-1]
    agents[split_categ][sink]['__sink_amt'] += wflow['flow_amt']
    agents[split_categ][sink]['__sink_nrm'] += wflow['flow_frac_root']
    if agents[split_categ][sink]['__sink_dur'] == 0:
        agents[split_categ][sink]['__sink_dur'] = []
    agents[split_categ][sink]['__sink_dur'].append((wflow['flow_duration'],wflow['flow_amt'],wflow['flow_frac_root']))
    # also by motif
    agents[split_categ][sink]['sink_'+motif+'_amt'] += wflow['flow_amt']
    agents[split_categ][sink]['sink_'+motif+'_nrm'] += wflow['flow_frac_root']
    if agents[split_categ][sink]['sink_'+motif+'_dur'] == 0:
        agents[split_categ][sink]['sink_'+motif+'_dur'] = []
    agents[split_categ][sink]['sink_'+motif+'_dur'].append((wflow['flow_duration'],wflow['flow_amt'],wflow['flow_frac_root']))
    return agents

def combine_agents(agents,breakdowns):
    for split_categ in list(agents):
        for agent in agents[split_categ]:
            for breakdown in agents[split_categ][agent]:
                if '_dur' in breakdown and agents['TOTAL'][agent][breakdown] == 0:
                    agents['TOTAL'][agent][breakdown] = []
                if agents[split_categ][agent][breakdown]:
                    agents['TOTAL'][agent][breakdown] += agents[split_categ][agent][breakdown]
    return agents

def finalize_agent(agent,dur_breakdown):
    for dur_term in dur_breakdown:
        try:
            agent[dur_term].sort()
            amt_cumsum = list(cumsum([x[1] for x in agent[dur_term]]))
            amt_mid = next(i for i,v in enumerate(amt_cumsum) if v >= amt_cumsum[-1]/2)
            agent[dur_term.split('_dur')[0]+'_median_dur_a'] = agent[dur_term][amt_mid][0]
            agent[dur_term.split('_dur')[0]+'_mean_dur_a']   = sum([x[0]*x[1] for x in agent[dur_term]])/sum([x[1] for x in agent[dur_term]])
            nrm_cumsum = list(cumsum([x[2] for x in agent[dur_term]]))
            nrm_mid = next(i for i,v in enumerate(nrm_cumsum) if v >= nrm_cumsum[-1]/2)
            agent[dur_term.split('_dur')[0]+'_median_dur_d'] = agent[dur_term][nrm_mid][0]
            agent[dur_term.split('_dur')[0]+'_mean_dur_d']   = sum([x[0]*x[2] for x in agent[dur_term]])/sum([x[2] for x in agent[dur_term]])
        except:
            agent[dur_term.split('_dur')[0]+'_median_dur_a'] = ''
            agent[dur_term.split('_dur')[0]+'_median_dur_d'] = ''
        del agent[dur_term]
    return agent

def write_agents(agents,agents_file,split_categ,breakdown,dur_breakdown):
    this_file = agents_file.split(".csv")[0]+"_"+str(split_categ)+".csv"
    with open(this_file,'w') as this_file:
        header_agent = ["___agent_ID"]+[term for term in sorted(list(breakdown))]
        writer_agent = csv.DictWriter(this_file,header_agent,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_agent.writerow({term:term.strip('_') for term in header_agent})
        # print distribution
        for agent in list(agents.keys()):
            agents[agent] = finalize_agent(agents[agent],dur_breakdown)
            agents[agent]["___agent_ID"] = agent
            writer_agent.writerow(agents[agent])
            del agents[agent]

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
    parser.add_argument('--circulate', type=int, default=4, help='The length at which flows are considered to circulate -- longer ones are folded down.')
    parser.add_argument('--join', action='append', default=[], help='Enter & exit types with these terms are joined (takes tuples).')
    parser.add_argument('--source', action='append', default=[], help='Transaction types that are their own sources (first sender is ignored)')
    parser.add_argument('--sink', action='append', default=[], help='Transaction types that are their own sinks (last recipient is ignored)')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Timeformat of the flow timestamp, if different from %Y-%m-%d %H:%M:%S')
    parser.add_argument('--instant', type=float, default=0, help='Durations less than or equal to this value (in hours) are considered instant')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    agents_filename = os.path.join(args.output_directory,args.prefix+"agents.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"agents_issues.txt")

    args.join = [x.strip('()').split(',') for x in args.join]

    ######### Creates weighted flow file #################
    find_agents(wflow_filename,agents_filename,report_filename,infer=args.infer,join=args.join,circulate=args.circulate,sources=args.source,sinks=args.sink,timeformat=args.timeformat,instant=args.instant)
    #################################################
