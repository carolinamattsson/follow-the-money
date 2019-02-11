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
def find_users(wflow_file,user_file,issues_file,infer=False,join=[]):
    ##########################################################################################
    wflow_header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_split_categs']
    with open(wflow_file,'r') as wflow_file, open(issues_file,'w') as issues_file:
        reader_wflows   = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # users is a nested dictionary: split_categ -> user_ID -> property -> value
        users = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))
        # populate the users dictionary
        for split_categ, wflow in split_by_month(reader_wflows,infer):
            try:
                wflow = parse(wflow)
                users = update_users(users,split_categ,wflow,join)
            except:
                writer_issues.writerow([wflow[term] for term in wflow]+[traceback.format_exc()])
        # get all the transaction types seen in any split_category
        breakdowns = {}
        for split_categ in users:
            breakdowns[split_categ] = set()
            for user in users[split_categ]:
                breakdowns[split_categ].update(users[split_categ][user].keys())
        breakdowns['TOTAL'] = set()
        for split_categ in users:
            breakdowns['TOTAL'] = breakdowns['TOTAL'].union(breakdowns[split_categ])
        # create an overall dists dictionary, and fill in any gaps in the others
        users = combine_users(users,breakdowns)
        # divide out the amount from the duration*amount
        dur_breakdowns = {}
        for split_categ in breakdowns:
            dur_breakdowns[split_categ] = {term for term in breakdowns[split_categ] if '_dur' in term}
            for dur_term in dur_breakdowns[split_categ]:
                breakdowns[split_categ].update([dur_term.split('_dur')[0]+'_median_dur_a',dur_term.split('_dur')[0]+'_median_dur_d'])
                breakdowns[split_categ].remove(dur_term)
        # write the piecharts
        for split_categ in users:
            writer_issues.writerow(["writing ",split_categ])
            write_users(users[split_categ],user_file,split_categ,breakdowns[split_categ],dur_breakdowns[split_categ])

def parse(wflow):
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
    return wflow

def consolidate_txn_types(txn_types, join):
    joined_txn_types = txn_types
    for i,term in enumerate(txn_types):
        for j,terms in enumerate(join):
            if term in terms: joined_txn_types[i] = "joined_"+str(j)
    return joined_txn_types

def update_users(users, split_categ, wflow, join):
    # first, consolidate the transaction types
    txn_types = consolidate_txn_types(wflow['flow_txn_types'],join)
    # for each user in the list
    flow_prev_amt  = wflow['flow_amt']
    last = len(wflow['flow_acct_IDs'][1:-1])-1
    for i,flow_this_user in enumerate(wflow['flow_acct_IDs'][1:-1]):
        flow_this_amt = wflow['flow_amt']      *(1-wflow['flow_rev_fracs'][i])
        flow_this_nrm = wflow['flow_frac_root']*(1-wflow['flow_rev_fracs'][i])
        flow_this_dur = (wflow['flow_durations'][i],flow_this_amt,flow_this_nrm)
        # record the total amount passing through this node, and the amount-averaged duration
        users[split_categ][flow_this_user]['__node_amt'] += flow_this_amt
        if users[split_categ][flow_this_user]['__node_dur'] == 0:
            users[split_categ][flow_this_user]['__node_dur'] = []
        users[split_categ][flow_this_user]['__node_dur'].append(flow_this_dur)
        # also the way it's passing through
        users[split_categ][flow_this_user][txn_types[i]+'~'+txn_types[i+1]+'_amt'] += flow_this_amt
        if users[split_categ][flow_this_user][txn_types[i]+'~'+txn_types[i+1]+'_dur'] == 0:
            users[split_categ][flow_this_user][txn_types[i]+'~'+txn_types[i+1]+'_dur'] = []
        users[split_categ][flow_this_user][txn_types[i]+'~'+txn_types[i+1]+'_dur'].append(flow_this_dur)
        # also in the four total categories
        if i == 0 and wflow['flow_categs'][0] == 'deposit':
            if wflow['flow_length'] == 2 and wflow['flow_categs'][1] == 'withdraw':
                users[split_categ][flow_this_user]['_depwtd_amt'] += flow_this_amt
                if users[split_categ][flow_this_user]['_depwtd_dur'] == 0:
                    users[split_categ][flow_this_user]['_depwtd_dur'] = []
                users[split_categ][flow_this_user]['_depwtd_dur'].append(flow_this_dur)
            else:
                users[split_categ][flow_this_user]['_deptfr_amt'] += flow_this_amt
                if users[split_categ][flow_this_user]['_deptfr_dur'] == 0:
                    users[split_categ][flow_this_user]['_deptfr_dur'] = []
                users[split_categ][flow_this_user]['_deptfr_dur'].append(flow_this_dur)
        elif i == last and wflow['flow_categs'][1] == 'withdraw':
            users[split_categ][flow_this_user]['_tfrwtd_amt'] += flow_this_amt
            if users[split_categ][flow_this_user]['_tfrwtd_dur'] == 0:
                users[split_categ][flow_this_user]['_tfrwtd_dur'] = []
            users[split_categ][flow_this_user]['_tfrwtd_dur'].append(flow_this_dur)
        else:
            users[split_categ][flow_this_user]['_tfrtfr_amt'] += flow_this_amt
            if users[split_categ][flow_this_user]['_tfrtfr_dur'] == 0:
                users[split_categ][flow_this_user]['_tfrtfr_dur'] = []
            users[split_categ][flow_this_user]['_tfrtfr_dur'].append(flow_this_dur)
        # how much of the fee/revenue this user contributes by receiving/sending their transactions
        users[split_categ][flow_this_user]['_node_fee_in']  += flow_prev_amt-flow_this_amt
        users[split_categ][flow_this_user]['_node_fee_out'] += flow_this_amt*wflow['flow_rev_fracs'][i+1]
        # set it up for the next itteration
        flow_prev_amt  = flow_this_amt
    return users

def combine_users(users,breakdowns):
    for split_categ in list(users):
        for user in users[split_categ]:
            for breakdown in users[split_categ][user]:
                if '_dur' in breakdown and users['TOTAL'][user][breakdown] == 0:
                    users['TOTAL'][user][breakdown] = []
                if users[split_categ][user][breakdown]:
                    users['TOTAL'][user][breakdown] += users[split_categ][user][breakdown]
    return users

def finalize_user(user,dur_breakdown):
    for dur_term in dur_breakdown:
        try:
            user[dur_term].sort()
            amt_cumsum = list(cumsum([x[1] for x in user[dur_term]]))
            amt_mid = next(i for i,v in enumerate(amt_cumsum) if v >= amt_cumsum[-1]/2)
            user[dur_term.split('_dur')[0]+'_median_dur_a'] = user[dur_term][amt_mid][0]
            user[dur_term.split('_dur')[0]+'_mean_dur_a']   = sum([x[0]*x[1] for x in user[dur_term]])/sum([x[1] for x in user[dur_term]])
            nrm_cumsum = list(cumsum([x[2] for x in user[dur_term]]))
            nrm_mid = next(i for i,v in enumerate(nrm_cumsum) if v >= nrm_cumsum[-1]/2)
            user[dur_term.split('_dur')[0]+'_median_dur_d'] = user[dur_term][nrm_mid][0]
            user[dur_term.split('_dur')[0]+'_mean_dur_d']   = sum([x[0]*x[2] for x in user[dur_term]])/sum([x[2] for x in user[dur_term]])
        except:
            user[dur_term.split('_dur')[0]+'_median_dur_a'] = ''
            user[dur_term.split('_dur')[0]+'_mean_dur_a']   = ''
            user[dur_term.split('_dur')[0]+'_median_dur_d'] = ''
            user[dur_term.split('_dur')[0]+'_mean_dur_d']   = ''
        del user[dur_term]
    return user

def write_users(users,users_file,split_categ,breakdown,dur_breakdown):
    this_file = users_file.split(".csv")[0]+"_"+str(split_categ)+".csv"
    with open(this_file,'w') as this_file:
        header_user = ["___acct_ID"]+[term for term in sorted(list(breakdown))]
        writer_user = csv.DictWriter(this_file,header_user,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_user.writerow({term:term.strip('_') for term in header_user})
        # print distribution
        for user in list(users.keys()):
            users[user] = finalize_user(users[user],dur_breakdown)
            users[user]["___acct_ID"] = user
            writer_user.writerow(users[user])
            del users[user]

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
    parser.add_argument('--join', action='append', default=[], help='Transaction types with these terms are joined (takes tuples).')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    users_filename = os.path.join(args.output_directory,args.prefix+"users.csv")
    report_filename = os.path.join(args.output_directory,args.prefix+"users_issues.txt")

    args.join = [x.strip('()').split(',') for x in args.join]

    ######### Creates weighted flow file #################
    find_users(wflow_filename,users_filename,report_filename,infer=args.infer,join=args.join)
    #################################################
