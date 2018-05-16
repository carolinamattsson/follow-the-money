'''
Follow The Money - main
This is the script to run a basic "follow the money" data transformation within the specified system boundaries.
Author: Carolina Mattsson, Northeastern University, April 2018
'''
import csv

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

    import ftm as ftm

    ################## Defines the files to read #########################
    prefix = '/tests/ex3_'
    path = os.path.dirname(os.path.realpath(__file__))
    transaction_file = path+prefix+'input.csv'
    moneyflow_file = path+prefix+'output.csv'
    issues_file = path+prefix+'issues.csv'
    #copyfile(os.path.realpath(__file__), path+prefix+'code.py')
    ######################################################################

    ########### Defines the transactions ################
    header = ['txn_ID','src_ID','tgt_ID','timestamp','txn_type','amt','rev']
    timeformat = '%Y-%m-%d %H:%M:%S'
    timewindow = ('2014-11-01 00:00:00','2014-11-01 03:00:00')
    ####################################################

    ######### Defines how money is tracked ##########
    #follow_heuristic = "none"
    ################### OR ##########################
    follow_heuristic = "greedy"
    ################### OR ##########################
    #follow_heuristic = "well-mixed"
    #################################################

    ######### Defines what a *user* is ##############
    #boundary_type = "none"
    #setup = ftm.setup(follow_heuristic,boundary_type,header,timeformat,timewindow=timewindow)
    ################### OR ##########################
    #boundary_type = "transactions"
    #transaction_categories_file = path+'/examples/transaction_categories.csv'
    #transaction_categories = read_transaction_categories(transaction_categories_file)
    #setup = ftm.setup(follow_heuristic,boundary_type,header,timeformat,transaction_categories=transaction_categories,timewindow=timewindow)
    ################### OR ##########################
    boundary_type = "accounts"
    account_categories_file = path+'/examples/account_categories.csv'
    account_categories = read_account_categories(account_categories_file)
    following = set(["user"])
    setup = ftm.setup(follow_heuristic,boundary_type,header,timeformat,account_categories=account_categories,following=following,timewindow=timewindow)
    #################################################

    ######### Defines any last-minute edits #########
    def modify_transaction(transaction):
        return transaction
    #################################################

    ########## Read in the file and go! #############
    ftm.run(transaction_file,moneyflow_file,issues_file,setup,infer_deposits=False,infer_withdraw=False,resolution_limit=0.99999,modifier_func=modify_transaction)
    #################################################
