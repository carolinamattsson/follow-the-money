from collections import defaultdict
import traceback
import math

#######################################################################################################
# Define various utility functions
def with_month(wflows):
    for wflow in wflows:
        month_ID = "-".join(wflow['root_timestamp'].split("-")[:-1])
        yield month_ID, wflow

def parse(wflow):
    wflow['flow_categs']    = tuple(wflow['flow_categs'].strip('()').split(','))
    wflow['flow_acct_IDs']  = wflow['flow_acct_IDs'].strip('[]').split(',')
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_txns']      = [float(txn) for txn in wflow['flow_txns'].strip('[]').split(',')]
    wflow['flow_amts']      = [float(amt) for amt in wflow['flow_amts'].strip('[]').split(',')]
    wflow['flow_revs']      = [float(rev) for rev in wflow['flow_revs'].strip('[]').split(',')]
    wflow['flow_durs']      = [] if wflow['flow_durs'] == "[]" else [float(dur) for dur in wflow['flow_durs'].strip('[]').split(',')]
    return wflow

def consolidate_txn_types(wflow, joins):
    for i,txn_type in enumerate(wflow['flow_txn_types']):
        for join in joins:
            if txn_type in joins[join]: wflow['flow_txn_types'][i] = join
    return wflow

def get_total_duration(month_list):
    total_dur = 0
    for yearmonth in month_list:
        year, month = yearmonth.split('-')
        if month in ['01','03','05','07','08','10','12']:
            total_dur += 31*24
        elif month in ['04','06','09','11']:
            total_dur += 30*24
        elif month=='02':
            if not int(year)%100:
                total_dur += 29*24 if int(year)%400 else 28*24
            else:
                total_dur += 29*24 if int(year)%4 else 28*24
    return total_dur

###########################################################################################
# Define how a (weighted) list of durations becomes output values
def cumsum(a_list):
    total = 0
    for x in a_list:
        total += x
        yield total

def duration_calculations(record_dur,cutoffs,total_dur,infer):
    # initialize the duration breakdowns of amount and balance
    record_dict = {'bal_avg':0}
    if cutoffs:
        record_dict.update({'bal_'+str(cutoff):0 for cutoff in cutoffs})
        record_dict.update({'amt_'+str(cutoff):0 for cutoff in cutoffs})
        record_dict.update({'bal_'+str(cutoffs[-1])+'+':0})
        record_dict.update({'amt_'+str(cutoffs[-1])+'+':0})
        if not infer:
            record_dict['bal_unk'] = 0
            record_dict['amt_unk'] = 0
    # note the duration breakdowns of amount and balance
    for x in record_dur:
        amtdur = x['amt']*x['dur']
        record_dict['bal_avg'] += amtdur
        if cutoffs:
            if x['dur']>=cutoffs[-1]:
                record_dict['bal_'+str(cutoffs[-1])+'+'] += amtdur
                record_dict['amt_'+str(cutoffs[-1])+'+'] += x['amt']
            elif not infer and x['inferred']:
                record_dict['bal_unk'] += amtdur
                record_dict['amt_unk'] += x['amt']
            else:
                for cutoff in cutoffs:
                    if x['dur']<cutoff:
                        record_dict['bal_'+str(cutoff)] += amtdur
                        record_dict['amt_'+str(cutoff)] += x['amt']
                        break
    # divide out the time to get the balance component
    for term in [term for term in record_dict if 'bal' in term]:
        record_dict[term] = record_dict[term]/total_dur
    # now find the average and median duration
    record_dur = sorted(record_dur, key = lambda x: x['dur'] if (infer or not x['inferred']) else float('inf'))
    record_amt_cumsum = list(cumsum([x['amt'] for x in record_dur]))
    record_amt_median = next(i for i,v in enumerate(record_amt_cumsum) if v >= record_amt_cumsum[-1]/2)
    record_dict['dur_med'] = record_dur[record_amt_median]['dur'] if (infer or not record_dur[record_amt_median]['inferred']) else float('nan')
    if infer or not any([x['inferred'] for x in record_dur]):
        record_dict['dur_avg'] = sum([x['amt']*x['dur'] for x in record_dur])/record_amt_cumsum[-1]
    else:
        record_dict['dur_avg'] = float('nan')
    return record_dict

###########################################################################################
# Define how the dictionary is written into table format
def write_user_summary(header, output_file, user_summary, month_list, txn_pairs_list, cutoffs=[], infer=False):
    # Calculate the total duration of this summary
    months = len(month_list)
    total_dur = get_total_duration(month_list)
    # Now write the files
    w = csv.DictWriter(output_file,header,delimiter=",",quotechar='"',escapechar="%")
    w.writeheader()
    for user in user_summary:
        try:
            record = {term:'' if 'dur' in term else 0 for term in header}
            record['user_ID'] = user
            record['months'] = months
            record['hours'] = total_dur
            record_amtdurs = {'ALL':[]}
            for txn_pair in txn_pairs_list:
                # make the combined dur list
                record_amtdurs[txn_pair] = []
                # tally up over the months
                for month in month_list:
                    # if there was activity in this month...
                    if month in user_summary[user] and txn_pair in user_summary[user][month]:
                        record[txn_pair+"_txn"] += user_summary[user][month][txn_pair]['txn']
                        record[txn_pair+"_amt"] += user_summary[user][month][txn_pair]['amt']
                        record[txn_pair+"_fee"] += user_summary[user][month][txn_pair]['fee']
                        record_amtdurs[txn_pair].extend(user_summary[user][month][txn_pair]['amtdurs'])
                # if there was activity in this category...
                if record_amtdurs[txn_pair]:
                    # tally up the overall totals
                    if txn_pair == 'inferred~inferred':
                        record['amt_static'] += record[txn_pair+"_amt"]
                    else:
                        record['amt_flow'] += record[txn_pair+"_amt"]
                    record_amtdurs['ALL'].extend(record_amtdurs[txn_pair])
                    # calculate the measures derivative from the durations
                    tmp_dict = duration_calculations(record_amtdurs[txn_pair],cutoffs,total_dur,infer)
                    for term in tmp_dict:
                        record[txn_pair+"_"+term] = tmp_dict[term]
            # calculate the measures derivative from the durations
            if record_amtdurs['ALL']:
                tmp_dict = duration_calculations(record_amtdurs['ALL'],cutoffs,total_dur,infer)
                for term in tmp_dict:
                    record[term] = tmp_dict[term]
            w.writerow(record)
        except:
            print("month_list: "+str(month_list)+"\n"+"user: "+str(user)+"\n"+traceback.format_exc())
    return

###########################################################################################
# Define the user summary per-trajectory updating function
def update_users(user_summary, wflow, month, joins):
    # Parse, flag inferred, and consolidate the transaction types
    wflow = parse(wflow)
    wflow = consolidate_txn_types(wflow, joins)
    # TODO: handle flows that begin or end with a user
    # Keep track of the transaction pairs we see
    txn_pairs = []
    # Now, loop over everything
    for i,this_user in enumerate(wflow['flow_acct_IDs'][1:-1]):
        # get the way in which money is passing through
        txn_pair = '~'.join(wflow['flow_txn_types'][i:i+2])
        # record the amount passing through this node in this way, and the duration list
        user_summary[this_user][month][txn_pair]['txn'] += wflow['flow_txns'][i]
        user_summary[this_user][month][txn_pair]['amt'] += wflow['flow_amts'][i]
        user_summary[this_user][month][txn_pair]['fee'] += wflow['flow_revs'][i+1]
        user_summary[this_user][month][txn_pair]['amtdurs'].append({'dur':wflow['flow_durs'][i],\
                                                                    'amt':wflow['flow_amts'][i],\
                                                                    'inferred':True if 'inferred' in txn_pair else False})
        # record the transaction pair
        txn_pairs.append(txn_pair)
    return user_summary, txn_pairs

#######################################################################################################
# Define the funciton that brings it all together
def users_processing(wflow_file,user_file,joins={},cutoffs=[],months=[],infer=False):
    ##########################################################################################
    # Define the user summary -- user_summary[USER_ID][MONTH][TXN_PAIR]
    user_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda:{'txn':0,'amt':0,'fee':0,'amtdurs':[]})))
    # Define sets of all months and transaction pairs seen
    all_txn_pairs = set()
    all_months = set()
    ##########################################################################################
    with open(wflow_file,'r') as wflow_file:
        reader_wflows = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # populate the users dictionary
        for month, wflow in with_month(reader_wflows):
            try:
                # Keep a record of months
                all_months.add(month)
                # Update the dictionary
                user_summary, txn_pairs = update_users(user_summary,wflow,month,joins)
                # Keep a record of transaction pairs
                all_txn_pairs.update(txn_pairs)
            except:
                print("month: "+str(month)+"\n"+"txn: "+str([wflow[term] for term in wflow]+"\n"+traceback.format_exc()))
    # When done...
    # Create the header
    all_txn_pairs = list(all_txn_pairs)
    header = ['user_ID','months','hours','amt_static','amt_flow','bal_avg','dur_med','dur_avg']
    if cutoffs:
        header = header + ['bal_'+str(hour) for hour in cutoffs]+['bal_'+str(cutoffs[-1])+'+']
        if not infer: header = header + ['bal_unk']
    if cutoffs:
        header = header + ['amt_'+str(hour) for hour in cutoffs]+['amt_'+str(cutoffs[-1])+'+']
        if not infer: header = header + ['amt_unk']
    for txn_pair in all_txn_pairs:
        header = header + [txn_pair+'_'+term for term in ['txn','amt','fee','bal_avg','dur_med','dur_avg']]
        if cutoffs:
            header = header + [txn_pair+'_bal_'+str(hour) for hour in cutoffs]+[txn_pair+'_bal_'+str(cutoffs[-1])+'+']
            if not infer: header = header + [txn_pair+'_bal_unk']
            header = header + [txn_pair+'_amt_'+str(hour) for hour in cutoffs]+[txn_pair+'_amt_'+str(cutoffs[-1])+'+']
            if not infer: header = header + [txn_pair+'_amt_unk']
    # Write the overall total numbers to file
    with open(users_filename, 'w') as users_file:
        month_list = list(all_months)
        write_user_summary(header, users_file, user_summary, month_list, all_txn_pairs, cutoffs, infer)
    # Write the by-group numbers to file
    for group in months:
        groupname = "_".join(group)
        group_filename = users_filename.split('.csv')[0]+'_'+groupname+'.csv'
        with open(group_filename, 'w') as group_file:
            month_list = [month for month in all_months if (month >= group[0] and month <= group[1])]
            write_user_summary(header, group_file, user_summary, month_list, all_txn_pairs, cutoffs, infer)


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
    parser.add_argument('--infer', action="store_true", default=False, help='Infer durations for incomplete flows.')
    parser.add_argument('--join', action='append', default=[], help='Transaction types with these terms are joined (takes tuples).')
    parser.add_argument('--name', action='append', default=[], help='The name to give this group of transaction types.')
    parser.add_argument('--cutoff', action='append', default=[], help='Duration cutoffs, in hours (takes integers).')
    parser.add_argument('--months', action='append', default=[], help='Report activity in these groups of months, takes tuples: (start,end).')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    users_filename = os.path.join(args.output_directory,args.prefix+"users_processing.csv")

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
        raise ValueError("The transaction type 'inferred' cannot be changed, unless the --infer flag is also used:",args.join,args.infer)

    args.cutoff = sorted([int(cutoff) for cutoff in args.cutoff])

    args.months = [tuple(x.strip('()').split(',')) for x in args.months]

    ######### Creates weighted flow file #################
    users_processing(wflow_filename,users_filename,joins=joins,cutoffs=args.cutoff,months=args.months,infer=args.infer)
    #################################################
