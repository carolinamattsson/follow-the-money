'''
Follow The Money - main
This is the script to run a basic "follow the money" data transformation within the specified system boundaries.
Author: Carolina Mattsson, Northeastern University, April 2018
'''

if __name__ == '__main__':
    from shutil import copyfile
    import os as os

    import ftm as ftm

    ################ Defines the files to use and create #################
    prefix = '/examples/ex1_'
    path = os.path.dirname(os.path.realpath(__file__))
    transaction_file = path+prefix+'input.csv'
    transaction_header = ['txn_ID','src_ID','tgt_ID','timestamp','txn_type','amt','rev']
    timeformat = '%Y-%m-%d %H:%M:%S'
    begin_timestamp = '2014-11-01 00:00:00'
    end_timestamp   = '2014-11-01 03:00:00'
    moneyflow_file = path+prefix+'output.csv'
    issues_file = path+prefix+'issues.csv'
    #copyfile(os.path.realpath(__file__), path+prefix+'code.py')
    ######################################################################

    ######### Defines what a *user* is ##############
    transaction_category = {}
    for transaction_type in ['cash_deposit','check_deposit','direct_deposit']:
        transaction_category[transaction_type] = 'deposit'
    for transaction_type in ['p2p_transfer']:
        transaction_category[transaction_type] = 'transfer'
    for transaction_type in ['cash_withdraw','bill_payment','card_payment','direct_debit']:
        transaction_category[transaction_type] = 'withdraw'
    ################### OR ##########################
    account_types_file = path+'/examples/account_types.csv'
    account_types      = ftm.read_acct_types(account_types_file)
    following          = set(['user'])
    #################################################

    ##### Defines the smallest amount we follow #####
    resolution_limit = 0.99999
    #################################################

    ######### Defines any last-minute edits #########
    def modify_transaction(transaction):
        return transaction
    #################################################

    ########## Read in the file and go! #############
    #ftm.run(transaction_file,transaction_header,moneyflow_file,issues_file,modify_transaction,transaction_category,begin_timestamp,end_timestamp,timeformat,resolution_limit)
    ################### OR ##########################
    ftm.run_by_acct(transaction_file,transaction_header,moneyflow_file,issues_file,modify_transaction,account_types,following,begin_timestamp,end_timestamp,timeformat,resolution_limit)
    #################################################
