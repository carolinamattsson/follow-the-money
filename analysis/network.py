# this takes as input the weighted_flowfile.csv SORTED by the agent beginning the transaction
# specifically:
# (head -1 DATE_weighted_flowfile.csv && tail -n +2 DATE_weighted_flowfile.csv | sort -t, -k7 -s) > DATE_weighted_flowfile_byagent.csv

#######################################################################################################
from datetime import datetime, timedelta
from collections import defaultdict

#######################################################################################################
def aggregate_enter_exit(flow_filename,enter_exit_filename,agent_filename,issues_filename,processes=1,sources=[],targets=[],infer=False,timeformat="%Y-%m-%d %H:%M:%S",instant=0):
    from multiprocessing import Pool
    import traceback
    ##########################################################################################
    global issues_file, writer_issues, enter_exit_header, own_sources, own_targets, flow_timeformat, flow_instant
    flow_timeformat = timeformat
    flow_instant    = instant
    own_sources = sources + ['inferred']
    own_targets = targets + ['inferred']
    wflow_header      = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_categs']
    enter_exit_header = ['enter_ID','exit_ID','edge_type_amt','edge_type_nrm','total_users','total_normalized','total_amount']
    enter_exit_header = enter_exit_header + [split+"_"+weight for split in ["0user","1user","2user","3+user"] for weight in ["amt","nrm"]]
    enter_exit_header = enter_exit_header + [split+"_"+weight for split in ["0days","1days","2days","3+days"] for weight in ["amt","nrm"]]
    enter_exit_header = enter_exit_header + [split+"_"+weight for split in ["1user_1days","1user_2days","1user_3+days"] for weight in ["amt","nrm"]]
    agent_header      = ['agent_ID','agent_type_deposit_txns','agent_type_deposit_amt','agent_type_withdraw_amt','deposit_users','deposit_txns','deposits_amt','withdraw_users','withdraw_amt','self_users','self_txns','self_amt']
    #agent_header      = agent_header + [term+'~'+exit for term in ['deposits_exit_deposits','deposits_exit_amount'] for exit in ['CASHOUT','ATMWD','BILLPAY','TOPUP','TOPUP_TRANSFER','DTOPUP','REVENUE','UNLOAD','inferred','other']]
    #agent_header      = agent_header + [term+'~'+enter for term in ['withdraws_enter_amount'] for enter in ['CASHIN','BULKPAY','LOAD','inferred','other'] ]
    ##########################################################################################
    agent_summary     = defaultdict(lambda: {'agent_ID':None,\
                                             'agent_type_deposit_txns':defaultdict(int),\
                                             'agent_type_deposit_amt':defaultdict(int),\
                                             'agent_type_withdraw_amt':defaultdict(int),\
                                             'deposit_users':set(),\
                                             'deposit_txns':0,\
                                             'deposit_amt':0,\
                                             'withdraw_users':set(),\
                                             'withdraw_amt':0,\
                                             'self_users':0,\
                                             'self_txns':0,\
                                             'self_amt':0\
                                             })
                                             #'deposit_users':set(),\
                                             #'deposit_txns_2':set(),\
                                             #'withdraw_users':set(),\
                                             #'withdraw_txns':set()
    with open(flow_filename,'r') as flow_file, open(enter_exit_filename,'w') as enter_exit_file, open(issues_filename,'w') as issues_file:
        reader_flows      = csv.DictReader(flow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_enter_exit = csv.writer(enter_exit_file,delimiter=",",quotechar="'",escapechar="%")
        writer_issues     = csv.writer(issues_file,delimiter=",",quotechar="'",escapechar="%")
        #############################################################
        writer_enter_exit.writerow(enter_exit_header)
        #############################################################
        if processes > 1:
            pool = Pool(processes=processes)
            agent_adjacencies = pool.imap_unordered(make_network, gen_groups(reader_flows,infer))
        else:
            agent_adjacencies = (make_network(agent) for agent in gen_groups(reader_flows,infer))
        #############################################################
        for agent_adjacency in agent_adjacencies:
            for exit_agent in agent_adjacency:
                agent_summary = update_agent_summary(agent_summary,agent_adjacency[exit_agent])
                agent_link    = finalize_link(agent_adjacency[exit_agent])
                writer_enter_exit.writerow([agent_link[term] for term in enter_exit_header])
    if processes > 1:
        pool.close()
        pool.join()
    with open(agent_filename,'w') as agent_file, open(issues_filename,'a') as issues_file:
        writer_agents     = csv.writer(agent_file,delimiter=",",quotechar="'",escapechar="%")
        writer_issues     = csv.writer(issues_file,delimiter=",",quotechar="'",escapechar="%")
        writer_agents.writerow(agent_header)
        for agent in agent_summary:
            try:
                agent = finalize_agent(agent_summary[agent])
                writer_agents.writerow([(agent[term] if term in agent else 0) for term in agent_header])
            except:
                writer_issues.writerow([agent_summary[agent][term] for term in agent_summary[agent]]+[traceback.format_exc()])
    return

def make_network(agent):
    from collections import defaultdict
    import traceback
    # initialize the adjacency matrix and contribution to the agent summary file
    agent_adjacency = {}
    # count up the stuff
    for flow in agent:
        try:
            # parse the lists into lists
            for term in ['flow_acct_IDs','flow_txn_IDs','flow_txn_types']:
                flow[term] = flow[term].strip('[]').split(',')
            # convert the numerical columns to float, and get the fraction of this flow that exited as revenue
            for term in ['flow_amt','flow_frac_root','flow_duration']:
                flow[term] = float(flow[term])
            # note when the topup targets are actually the provider
            if flow['flow_txn_types'][-1] in own_targets:
                flow['flow_acct_IDs'][-1] = flow['flow_txn_types'][-1]
            if flow['flow_txn_types'][0]  in own_sources:
                flow['flow_acct_IDs'][0] = flow['flow_txn_types'][0]
            # start a new entry for the exit agent, if it's a new one
            exit_agent = flow['flow_acct_IDs'][-1]
            if exit_agent not in agent_adjacency:
                agent_adjacency[exit_agent]                  = {term:0 for term in enter_exit_header}
                agent_adjacency[exit_agent]['enter_ID']      = flow['flow_acct_IDs'][0]
                agent_adjacency[exit_agent]['exit_ID']       = flow['flow_acct_IDs'][-1]
                agent_adjacency[exit_agent]['edge_type_amt'] = {'enter':defaultdict(int),'exit':defaultdict(int)}
                agent_adjacency[exit_agent]['edge_type_nrm'] = {'enter':defaultdict(int),'exit':defaultdict(int)}
                agent_adjacency[exit_agent]['enter_users']   = set()
                agent_adjacency[exit_agent]['exit_users']    = set()
                agent_adjacency[exit_agent]['total_users']   = set()
            # get the current one :)
            agent_link = agent_adjacency[exit_agent]
            # keep track of the largest type
            agent_link['edge_type_amt']['enter'][flow['flow_txn_types'][0]] += flow['flow_amt']
            agent_link['edge_type_nrm']['enter'][flow['flow_txn_types'][0]] += flow['flow_frac_root']
            agent_link['edge_type_amt']['exit'][flow['flow_txn_types'][-1]] += flow['flow_amt']
            agent_link['edge_type_nrm']['exit'][flow['flow_txn_types'][-1]] += flow['flow_frac_root']
            # update the amount
            agent_link['enter_users'].update(flow['flow_acct_IDs'][1:-1][0] if flow['flow_acct_IDs'][1:-1] else [])
            #agent_link['enter_txns'].add(flow['flow_txn_IDs'][0]) \ use with caution - very slow and uses lots of memory
            agent_link['exit_users'].update(flow['flow_acct_IDs'][1:-1][-1] if flow['flow_acct_IDs'][1:-1] else [])
            #agent_link['exit_txns'].add(flow['flow_txn_IDs'][-1]) \ use with caution - very slow and uses lots of memory
            agent_link['total_users'].update(flow['flow_acct_IDs'][1:-1])
            agent_link['total_normalized'] += flow['flow_frac_root']
            agent_link['total_amount']     += flow['flow_amt']
            # check where to attribute the amount
            users = len(flow['flow_acct_IDs'][1:-1])
            number_users = "".join([str(users) if users<3 else "3+","user"])
            days = get_days(flow['flow_timestamp'],flow["flow_duration"],flow_timeformat,flow_instant)
            number_days = "".join([str(days) if days<3 else "3+","days"])
            agent_link[number_users+"_amt"] += flow['flow_amt']
            agent_link[number_users+"_nrm"] += flow['flow_frac_root']
            agent_link[number_days+"_amt"] += flow['flow_amt']
            agent_link[number_days+"_nrm"] += flow['flow_frac_root']
            if number_users == "1user" and number_days != "0days":
                agent_link[number_users+"_"+number_days+"_amt"] += flow['flow_amt']
                agent_link[number_users+"_"+number_days+"_nrm"] += flow['flow_frac_root']
        except:
            writer_issues.writerow(['could not make_network for flow:',flow['flow_txn_IDs'],traceback.format_exc()])
            issues_file.flush()
    return agent_adjacency

def get_days(flow_timestamp,flow_duration,timeformat,instant=0):
    if flow_duration <= instant:
        return 0
    else:
        start_timestamp = datetime.strptime(flow_timestamp,timeformat)
        end_timestamp   = start_timestamp + timedelta(hours=flow_duration)
        days = (end_timestamp.date()-start_timestamp.date()).days
        return days + 1

def finalize_link(agent_link):
    agent_link['total_users'] = len(agent_link['total_users'])
    agent_link['edge_type_amt']  = "-".join([max(agent_link['edge_type_amt']['enter'], key=agent_link['edge_type_amt']['enter'].get) if len(agent_link['edge_type_amt']['enter'])>0 else 'None', \
                                             max(agent_link['edge_type_amt']['exit'],  key=agent_link['edge_type_amt']['exit'].get)  if len(agent_link['edge_type_amt']['exit'])>0 else 'None'])
    agent_link['edge_type_nrm']  = "-".join([max(agent_link['edge_type_nrm']['enter'], key=agent_link['edge_type_nrm']['enter'].get) if len(agent_link['edge_type_nrm']['enter'])>0 else 'None', \
                                             max(agent_link['edge_type_nrm']['exit'],  key=agent_link['edge_type_nrm']['exit'].get)  if len(agent_link['edge_type_nrm']['exit'])>0 else 'None'])
    return agent_link

def update_agent_summary(agent_summary,agent_link):
    # first update the enter_agent
    agent = agent_summary[agent_link['enter_ID']]
    if not agent['agent_ID']: agent['agent_ID'] = agent_link['enter_ID']
    for type in agent_link['edge_type_nrm']['enter']:
        agent['agent_type_deposit_txns'][type] += agent_link['edge_type_nrm']['enter'][type]
    for type in agent_link['edge_type_amt']['enter']:
        agent['agent_type_deposit_amt'][type] += agent_link['edge_type_amt']['enter'][type]
    agent['deposit_users'].update(agent_link['enter_users'])
    #agent['deposits_txns'].update(agent_link['enter_txns'])
    agent['deposit_txns'] += agent_link['total_normalized']
    agent['deposit_amt']  += agent_link['total_amount']
    # note the self-loops
    if agent_link['enter_ID'] == agent_link['exit_ID']:
        agent['self_users'] = len(agent_link['enter_users'] & agent_link['exit_users'])
        agent['self_txns']  = agent_link['total_normalized']
        agent['self_amt']   = agent_link['total_amount']
    # then the exit_agent
    agent = agent_summary[agent_link['exit_ID']]
    if not agent['agent_ID']: agent['agent_ID'] = agent_link['exit_ID']
    for type in agent_link['edge_type_amt']['exit']:
        agent['agent_type_withdraw_amt'][type] += agent_link['edge_type_amt']['exit'][type]
    agent['withdraw_users'].update(agent_link['exit_users'])
    #agent['withdraws_txns'].update(agent_link['exit_txns'])
    agent['withdraw_amt'] += agent_link['total_amount']
    return agent_summary

def finalize_agent(agent):
    agent['agent_type_deposit_txns'] = max(agent['agent_type_deposit_txns'], key=agent['agent_type_deposit_txns'].get) if len(agent['agent_type_deposit_txns'])>0 else 'None'
    agent['agent_type_deposit_amt']  = max(agent['agent_type_deposit_amt'],  key=agent['agent_type_deposit_amt'].get)  if len(agent['agent_type_deposit_amt'])>0  else 'None'
    agent['agent_type_withdraw_amt'] = max(agent['agent_type_withdraw_amt'], key=agent['agent_type_withdraw_amt'].get) if len(agent['agent_type_withdraw_amt'])>0 else 'None'
    agent['deposit_users']  = len(agent['deposit_users'])
    #agent['deposits_txns']  = len(agent['deposits_txns'])
    agent['withdraw_users'] = len(agent['withdraw_users'])
    #agent['withdraws_txns'] = len(agent['withdraws_txns'])
    return agent

def gen_groups(wflows,infer):
    agent_deposits = []
    old_agent = None
    for flow in wflows:
        if not infer and "inferred" in flow["flow_txn_types"]:
            continue
        if flow['flow_txn_types'][0:5] == "[OTC_": flow['flow_txn_types'] = '['+flow['flow_txn_types'][5:]
        agent_ID = flow['flow_acct_IDs'].strip('[]').split(',')[0]
        if not old_agent or old_agent == agent_ID:
            agent_deposits.append(flow)
            old_agent = agent_ID
        else:
            yield agent_deposits
            del agent_deposits[:]
            agent_deposits = [flow]
            old_agent = agent_ID
    yield agent_deposits

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
    parser.add_argument('--parallel', type=int, default=1, help='The max number of parallel processes to launch.')
    parser.add_argument('--source', action='append', default=[], help='Transaction types that are their own sources (first sender is ignored)')
    parser.add_argument('--target', action='append', default=[], help='Transaction types that are their own targets (last recipient is ignored)')
    parser.add_argument('--infer', action="store_true", default=False, help='Include flows that begin or end with inferred transactions')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Timeformat of the flow timestamp, if different from %Y-%m-%d %H:%M:%S')
    parser.add_argument('--instant', type=float, default=0, help='Durations less than or equal to this value (in hours) are considered instant')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)
    if args.parallel < 1:
        raise ValueError("--parallel must be a positive integer",args.parallel)

    wflow_filename = args.input_file
    network_filename = os.path.join(args.output_directory,args.prefix+"network.csv")
    agents_filename = os.path.join(args.output_directory,args.prefix+"agents.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"network_issues.txt")

    ##### Creates network file, and agent+ file #####
    aggregate_enter_exit(wflow_filename,network_filename,agents_filename,report_filename,processes=args.parallel,sources=args.source,targets=args.target,infer=args.infer,timeformat=args.timeformat,instant=args.instant)
    #################################################
