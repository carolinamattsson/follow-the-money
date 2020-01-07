##########################################################################################
### Get transaction-based summaries of users ###
##########################################################################################
from collections import defaultdict
import traceback
import csv

#######################################################################################################
def with_month(txns):
    for txn in txns:
        month_ID = "-".join(txn['timestamp'].split("-")[:-1])
        yield month_ID, txn

###########################################################################################
# Define the user summary per-transaction updating function
def update_loop(user_summary, month, txn, txn_categ):
    # Note the user ID
    if txn_categ == 'deposit':
        agent    = txn['src_ID']
        user     = txn['tgt_ID']
        user_bal = float(txn['tgt_balance']) if txn['tgt_balance'] else float('nan')
    elif txn_categ == 'withdraw':
        user     = txn['src_ID']
        user_bal = float(txn['src_balance']) if txn['src_balance'] else float('nan')
        agent    = txn['tgt_ID']
    elif txn_categ == 'transfer':
        sndr     = txn['src_ID']
        sndr_bal = float(txn['src_balance']) if txn['src_balance'] else float('nan')
        rcpt     = txn['tgt_ID']
        rcpt_bal = float(txn['tgt_balance']) if txn['tgt_balance'] else float('nan')
    # Note the transaction type
    txn_amt  = float(txn['amt'])
    txn_fee  = float(txn['fee'])
    # Update the user summary dictionary
    if txn_categ != 'transfer':
        txn_type = txn['type']
        user_summary[user][month][txn_type]['txn'] +=1
        user_summary[user][month][txn_type]['amt'] += txn_amt
        user_summary[user][month][txn_type]['fee'] += txn_fee
        user_summary[user][month][txn_type]['bal'] += user_bal
        user_summary[user][month][txn_type]['alt'].add(agent)
    # If there's a user also on the other side of the transaction, update like this instead
    elif txn_categ == 'transfer':
        txn_type = txn['type']+"_OUT"
        user_summary[sndr][month][txn_type]['txn'] +=1
        user_summary[sndr][month][txn_type]['amt'] += txn_amt
        user_summary[sndr][month][txn_type]['fee'] += txn_fee
        user_summary[sndr][month][txn_type]['bal'] += sndr_bal
        user_summary[sndr][month][txn_type]['alt'].add(rcpt)
        txn_type = txn['type']+"_IN"
        user_summary[rcpt][month][txn_type]['txn'] += 1
        user_summary[rcpt][month][txn_type]['amt'] += txn_amt
        user_summary[rcpt][month][txn_type]['fee'] += txn_fee
        user_summary[rcpt][month][txn_type]['bal'] += rcpt_bal
        user_summary[rcpt][month][txn_type]['alt'].add(sndr)
    else:
        raise ValueError("Transaction is of an unsupported category:",txn)
    return user_summary

###########################################################################################
# Define how the dictionary is written into table format
def write_user_summary(header, output_filename, user_summary, month_list, txn_types_dict):
    # Make also a file with all the active accounts
    accounts_filename = output_filename.split('users_activity.csv')[0]+'accounts.txt'
    # Now write the files
    with open(output_filename,'w') as output_file, open(accounts_filename,'w') as accounts_file:
        w = csv.DictWriter(output_file,header,delimiter=",",quotechar='"',escapechar="%")
        w.writeheader()
        # Get the month range
        months = len(month_list)
        # Aggregate for each user
        for user in user_summary:
            try:
                record = {term: set() if 'alt' in term else 0 for term in header}
                record['user_ID'] = user
                record['months'] = months
                for txn_type in txn_types_dict:
                    for month in month_list:
                        # if there was activity in this category...
                        if month in user_summary[user] and txn_type in user_summary[user][month]:
                            # tally up over the months
                            record[txn_type+"_alt"].update(user_summary[user][month][txn_type]['alt'])
                            record[txn_type+"_txn"] += user_summary[user][month][txn_type]['txn']
                            record[txn_type+"_amt"] += user_summary[user][month][txn_type]['amt']
                            record[txn_type+"_fee"] += user_summary[user][month][txn_type]['fee']
                            record[txn_type+"_bal"] += user_summary[user][month][txn_type]['bal']
                    # if there was activity in this category...
                    # tally up over the transaction types
                    if txn_types_dict[txn_type] == "OUT":
                        record['alt_out'].update(record[txn_type+"_alt"])
                        record['txn_out'] += record[txn_type+"_txn"]
                        record['amt_out'] += record[txn_type+"_amt"]
                        record['fee_out'] += record[txn_type+"_fee"]
                        record['bal_out'] += record[txn_type+"_bal"]
                    elif txn_types_dict[txn_type] == "IN":
                        record['alt_in'].update(record[txn_type+"_alt"])
                        record['txn_in'] += record[txn_type+"_txn"]
                        record['amt_in'] += record[txn_type+"_amt"]
                        record['fee_in'] += record[txn_type+"_fee"]
                        record['bal_in'] += record[txn_type+"_bal"]
                # now turn the alter sets into number of unique alters, and normalize the balance
                for term in [term for term in record if 'alt' in term]:
                    record[term] = len(record[term])
                for term in [term for term in record if 'bal' in term]:
                    record[term] = record[term]/record[term.replace('bal','txn')] if record[term.replace('bal','txn')] else float('nan')
                # Write user summary to file
                w.writerow(record)
                # Write the active users to list
                if record['txn_in'] > 0 or record['txn_out'] > 0:
                    accounts_file.write(record['user_ID']+'\n')
            except:
                print("month_list: "+str(month_list)+"\n"+"user: "+str(user)+"\n"+traceback.format_exc())
    return

###########################################################################################
# Define the function that brings it all together
def user_activity(txns_filenames,config_filenames,users_filename,groups=[]):
    ##########################################################################################
    # Define the user summary -- user_summary[USER_ID][MONTH][TXN_TYPE]
    user_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda:{'txn':0,'amt':0,'fee':0,'alt':set(),'bal':0})))
    # Define sets of all months and transaction types seen
    txn_types = {}
    months = set()
    ##########################################################################################
    for txn_filename, config_filename in zip(txns_filenames, config_filenames):
        # Load the configuration file
        with open(config_filename, 'r') as config_file:
            config = json.load(config_file)
        # Read the transaction file
        with open(txn_filename, 'r') as txn_file:
            txn_reader = csv.DictReader(txn_file,config['transaction_header'],delimiter=config['delimiter'],quotechar=config['quotechar'],escapechar=config['escapechar'])
            for month, txn in with_month(txn_reader):
                # Note the category of transaction
                try:
                    txn_categ = config['transaction_categories'][txn['type']]
                except:
                    continue
                # Keep a record of months and transaction types
                months.add(month)
                if txn_categ == 'transfer':
                    txn_types[txn['type']+'_IN']  = "IN"
                    txn_types[txn['type']+'_OUT'] = "OUT"
                elif txn_categ == 'deposit':
                    txn_types[txn['type']] = "IN"
                elif txn_categ == 'withdraw':
                    txn_types[txn['type']] = "OUT"
                else:
                    raise ValueError("Transaction is of an unsupported category:",txn)
                # Update the summary dictionaries
                try:
                    user_summary = update_loop(user_summary, month, txn, txn_categ)
                except:
                    print("month: "+str(month)+"\n"+"txn: "+str(txn)+"\n"+traceback.format_exc())
    # When done...
    # Create the header
    header = ['user_ID','months']
    header = header + [term+"_"+dir for term in ['alt','txn','amt','fee','bal'] for dir in ['in','out']]
    for txn_type in txn_types:
        header = header + [txn_type+'_'+term for term in ['alt','txn','amt','fee','bal']]
    # Write the overall total numbers to file
    with open(users_filename, 'w') as users_file:
        month_list = list(months)
        write_user_summary(header, users_filename, user_summary, month_list, txn_types)
    # Write the by-group numbers to file
    for group in groups:
        groupname = "_".join(group)
        group_filename = users_filename.split('.csv')[0]+'_'+groupname+'.csv'
        month_list = [month for month in months if (month >= group[0] and month <= group[1])]
        write_user_summary(header, group_filename, user_summary, month_list, txn_types)

###########################################################################################
# Define the progam interface
if __name__ == '__main__':
    import argparse
    import json
    import sys
    import csv
    import os

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input transaction file (.csv)')
    parser.add_argument('config_file', help='The configuration file (.json)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output files')
    parser.add_argument('--file', action='append', default=[], help='Additional transaction files.')
    parser.add_argument('--config', action='append', default=[], help='Additional configuration files')
    parser.add_argument('--group', action='append', default=[], help='Report activity in these groups of months, takes tuples: (start,end).')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isfile(args.config_file):
        raise OSError("Could not find the config file",args.config_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    txns_filenames   = [args.input_file]
    config_filenames = [args.config_file]
    users_filename   = os.path.join(args.output_directory,args.prefix+"users_activity.csv")

    if args.file:
        if len(args.file) == len(args.config):
            txns_filenames   = txns_filenames + args.file
            config_filenames = config_filenames + args.config
        else:
            raise IndexError("Please provide a config file for each additional transaction file:",args.file,args.config)

    args.group = [tuple(x.strip('()').split(',')) for x in args.group]

    ######### Creates user activity files #################
    user_activity(txns_filenames,config_filenames,users_filename,groups=args.group)
    #######################################################
