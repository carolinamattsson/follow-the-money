'''
Follow The Money - main
This is the script to run a basic "follow the money" data transformation.
Author: Carolina Mattsson, Northeastern University, April 2018
'''

if __name__ == '__main__':
    from datetime import datetime, timedelta
    from shutil import copyfile
    import os as os
    import traceback
    import csv

    import ftm as ftm

    ################ Defines the files to use and create #################
    prefix = '/examples/ex4_'
    path = os.path.dirname(os.path.realpath(__file__))
    transaction_file = path+prefix+'input.csv'
    transaction_header = ['txn_ID','src_ID','tgt_ID','timestamp','txn_type','amt','rev']
    timeformat = '%Y-%m-%d %H:%M:%S'
    begin_timestamp = '2014-11-01 00:00:00'
    end_timestamp   = '2014-11-01 03:00:00'
    moneyflow_file = path+prefix+'output.csv'
    moneyflow_header = ['flow_txn_IDs','flow_txn_types','flow_txn_timestamps','flow_acct_IDs','flow_amt','flow_rev','flow_frac_root','flow_duration','flow_durations','flow_tux','flow_tux_wrev']
    issues_file = path+prefix+'issues.csv'
    #copyfile(os.path.realpath(__file__), path+prefix+'code.py')
    ######################################################################

    ######### Defines what a *user* is ###############
    transaction_category = {}
    for transaction_type in ['cash_deposit','check_deposit','direct_deposit']:
        transaction_category[transaction_type] = 'deposit'
    for transaction_type in ['p2p_transfer']:
        transaction_category[transaction_type] = 'transfer'
    for transaction_type in ['cash_withdraw','bill_payment','card_payment','direct_debit']:
        transaction_category[transaction_type] = 'withdraw'
    #################################################

    ##### Defines the smallest amount we follow #####
    resolution_limit = 0.99999
    #################################################

    ##### The dictionary that holds all accounts ####
    accounts = {}
    #################################################

    ########## Read in the file and go! #############
    with open(transaction_file,'r') as transaction_file, open(moneyflow_file,'w') as moneyflow_file, open(issues_file,'w') as issues_file:
        transaction_reader = csv.DictReader(transaction_file,transaction_header,delimiter=",",quotechar="'",escapechar="%")
        moneyflow_writer   = csv.writer(moneyflow_file,delimiter=",",quotechar="'",escapechar="%")
        issue_writer       = csv.writer(issues_file,delimiter=",",quotechar="'",escapechar="%")
        moneyflow_writer.writerow(moneyflow_header)
        for transaction in transaction_reader:
            try:
                moneyflows = ftm.process(transaction,accounts,transaction_category,begin_timestamp,timeformat,resolution_limit)
                if moneyflows:
                    for moneyflow in moneyflows:
                        moneyflow_writer.writerow(moneyflow.to_print())
            except:
                issue_writer.writerow([transaction[x] for x in transaction_header]+[traceback.format_exc()])
        moneyflows = ftm.process_remaining_funds(accounts,end_timestamp,timeformat,resolution_limit)
        for moneyflow in moneyflows:
                moneyflow_writer.writerow(moneyflow.to_print())
