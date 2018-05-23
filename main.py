'''
Follow The Money - main
This is the script to run a basic "follow the money" data transformation within the specified system boundaries.
Author: Carolina Mattsson, Northeastern University, April 2018
'''

def read_account_categories(file):
    account_categories = {'src':{},'tgt':{}}
    with open(file,'rU') as account_categories_file:
        account_type_reader = csv.DictReader(account_categories_file,['transaction_type','sender/recipient','account_category'],delimiter=",",quotechar="'",escapechar="%")
        for account_type in account_type_reader:
            account_categories[account_type['sender/recipient']][account_type['transaction_type']] = account_type['account_category']
    return account_categories

def read_transaction_categories(file):
    transaction_category = {}
    with open(file,'rU') as transaction_types_file:
        transaction_type_reader = csv.DictReader(transaction_types_file,['transaction_type','transaction_category'],delimiter=",",quotechar="'",escapechar="%")
        for transaction_type in transaction_type_reader:
            transaction_category[transaction_type['transaction_type']] = transaction_type['transaction_category']
    return transaction_category

if __name__ == '__main__':
    from shutil import copyfile
    import os as os
    import sys
    import csv

    import ftm as ftm

    ################### ARGUMENTS #####################
    input_filename   = sys.argv[1] # relative to the directory main.py is in
    output_prefix    = sys.argv[2] # relative to the directory main.py is in
    follow_heuristic = sys.argv[3] # "none", "greedy", or "well-mixed"
    time_cutoff      = sys.argv[4] # "none" or a number of hours
    infer            = sys.argv[5] # "none" or "infer"

    ##################### INPUT ########################
    ############ Defines the file to read ##############
    path = os.path.dirname(os.path.realpath(__file__))+'/'
    transaction_file = path+input_filename
    ################ And how to read it ################
    transaction_header = ['txn_ID','src_ID','tgt_ID','timestamp','txn_type','amt','rev']
    timeformat = '%Y-%m-%d %H:%M:%S'
    timewindow = ('2014-11-01 00:00:00','2014-11-01 03:00:00')
    ############ Allows last-minute edits ##############
    def modify_transaction(transaction):
        return transaction
    ############ Defines what a *user* is ##############
    #setup = ftm.setup(follow_heuristic,time_cutoff=time_cutoff,boundary_type="none",resolution_limit=resolution_limit)
    ###################### OR ##########################
    transaction_categories_file = path+'/examples/transaction_categories.csv'
    transaction_categories = read_transaction_categories(transaction_categories_file)
    input = ftm.setup_data(transaction_file,transaction_header,timeformat,boundary_type="transactions",transaction_categories=transaction_categories,timewindow=timewindow,modifier_func=modify_transaction)
    ###################### OR ##########################
    #account_categories_file = path+'/examples/account_categories.csv'
    #account_categories = read_account_categories(account_categories_file)
    #following = set(["user"])
    #setup = ftm.setup(follow_heuristic,time_cutoff=time_cutoff,boundary_type="accounts",account_categories=account_categories,following=following,resolution_limit=resolution_limit)
    ####################################################

    #################### OUTPUT ########################
    ########### Defines how money is tracked ###########
    follow_heuristic = follow_heuristic                 # "none", "greedy", or "well-mixed"
    ######## Defines how long money is tracked #########
    time_cutoff = time_cutoff                           # "none" or a number of hours
    ####### Defines the smallest amount tracked ########
    resolution_limit = 0.99999                          # in monetary units
    ######### Defines handling of finite data ##########
    infer_deposits  = True if infer == "infer" else False
    infer_withdraws = True if infer == "infer" else False
    ############ Defines the output files ##############
    moneyflow_file = path+output_prefix+'output_'+follow_heuristic+infer+str(time_cutoff)+'hr.csv'
    issues_file = path+output_prefix+'issues_'+follow_heuristic+infer+str(time_cutoff)+'hr.csv'
    copyfile(os.path.realpath(__file__), path+output_prefix+'code.py')
    ####################################################

    ############ Read in the file and go! ##############
    ftm.run(input,follow_heuristic,moneyflow_file,issues_file,time_cutoff=time_cutoff,infer_deposits=infer_deposits,infer_withdraws=infer_withdraws,resolution_limit=resolution_limit)
    ####################################################
