'''
Discover Financial Transaction Data
This code defines classes and functions for a substantial exploration
of a transation dataset, with weighted and directed temporal links. This code
can return a dictionary of all the accounts encountered, used to initialize
Follow the money analysis of the transaction dataset.

Requirements:
The incoming data needs to be time ordered and have at least these columns:
txn_ID (unique ID), src_ID (sending account), tgt_ID (receiving account), amt (transaction amount)
Additional columns that will be analyzed if available are:
timestamp, txn_type (transaction type),rev (fee/revenue incurred), src/tgt_balance (account balances)
'''

from ftm import Account_holder
from datetime import datetime, timedelta
import math

def load_accounts(accounts_file):
    return accounts

def load_account_categories(file):
    import csv
    account_categories = {'src':{},'tgt':{}}
    with open(file,'rU') as account_categories_file:
        account_type_reader = csv.DictReader(
            account_categories_file,
            ['transaction_type','sender/recipient','account_category'],delimiter=",",quotechar='"',escapechar="%")
        for account_type in account_type_reader:
            account_categories[account_type['sender/recipient']][account_type['transaction_type']] =\
                account_type['account_category']
    return account_categories

def load_transaction_categories(file):
    import csv
    transaction_categories = {}
    with open(file,'rU') as transaction_types_file:
        transaction_type_reader = csv.DictReader(
            transaction_types_file,['transaction_type','transaction_category'],delimiter=",",quotechar="'",escapechar="%")
        for transaction_type in transaction_type_reader:
            transaction_categories[transaction_type['transaction_type']] = transaction_type['transaction_category']
    return transaction_categories

def discover_starting_balance(src_acct,tgt_acct,amt,rev=0):
    amt_missing = src_acct.account - amt - rev
    if amt_missing > 0:
        src_acct.starting_balance += amt_missing
    src_acct.account = src_acct.account - amt - rev
    tgt_acct.account = tgt_acct.account + amt
    return src_acct, tgt_acct

def discover_account_categories(src_acct,tgt_acct,amt,rev=0,basics=None,txn_type=None):
    if not txn_type: txn_type = ''
    src_acct.categs.add('src~'+txn_type)
    tgt_acct.categs.add('tgt~'+txn_type)
    # update the account basics
    if basics:
        src_acct.basics.setdefault(txn_type,{'txns_in':0,'txns_out':0,'amt_in':0,'amt_out':0,'rev':0,'alters_in':set(),'alters_out':set()})
        tgt_acct.basics.setdefault(txn_type,{'txns_in':0,'txns_out':0,'amt_in':0,'amt_out':0,'rev':0,'alters_in':set(),'alters_out':set()})
        src_acct.basics[txn_type]['txns_out'] += 1
        src_acct.basics[txn_type]['amt_out']  += float(amt)
        src_acct.basics[txn_type]['rev']      += float(rev)
        src_acct.basics[txn_type]['alters_out'].add(tgt_acct.user_ID)
        tgt_acct.basics[txn_type]['txns_in']  += 1
        tgt_acct.basics[txn_type]['amt_in']   += float(amt)
        tgt_acct.basics[txn_type]['alters_in'].add(src_acct.user_ID)
    return src_acct, tgt_acct

def finalize_basics(acct_holder):   # note, the alters are still sets at this point
    total = {}
    for term in ['txns_in','txns_out','amt_in','amt_out','rev','alters_in','alters_out']:
        total[term] = sum(acct_holder.basics[txn_type][term] for txn_type in acct_holder.basics)
    acct_holder.basics['total'] = total
    acct_holder.txns = acct_holder.basics['total']['txns_in']+acct_holder.basics['total']['txns_out']
    acct_holder.amt  = max(acct_holder.basics['total']['amt_in'],acct_holder.basics['total']['amt_out']+acct_holder.basics['total']['rev'])

def account_properties(
        transaction_file,transaction_header,issues_file,modifier_func=None,starting_balance=None,
        account_categories=None,discover_balance=False,account_basics=False,account_file=None):
    import traceback
    import csv
    # now we can open the transaction and output files!!
    with open(transaction_file,'rU') as transaction_file, open(issues_file,'w') as issues_file:
        transaction_reader = csv.DictReader(transaction_file,transaction_header,delimiter=",",quotechar="'",escapechar="%")
        issue_writer       = csv.writer(issues_file,delimiter=",",quotechar="'")
        # we use a dictionary to keep track of all the account holders in the system
        accounts = {}
        # now we loop through all the transactions and discover what's up with this system!
        for txn in transaction_reader:
            try:
                txn = modifier_func(txn) if modifier_func else txn
                # for every transaction, update the state of the participating accounts
                Account_holder.create_accounts(
                    accounts,txn['src_ID'],txn['tgt_ID'],starting_balance=(txn[starting_balance[0]],txn[starting_balance[1]]) if \
                        starting_balance else (None,None))
                if discover_balance:
                    discover_starting_balance(accounts[txn['src_ID']],accounts[txn['tgt_ID']],txn['amt'],rev=txn['rev'] if 'rev' in txn else 0)
                if not account_categories:
                    discover_account_categories(accounts[txn['src_ID']],accounts[txn['tgt_ID']],txn['amt'],rev=txn['rev'] if 'rev' in txn else 0,
                        txn_type=txn['txn_type'] if 'txn_type' in txn else None,basics=account_basics)
            except:
                issue_writer.writerow(["ISSUE W/ ACCOUNT DISCOVERY",str(txn)]+[traceback.format_exc()])
    if account_basics:
        for acct_holder in accounts.values():
            acct_holder = finalize_basics(acct_holder)
    if account_file:
        with open(account_file,'w') as account_file:
            accounts_writer = csv.writer(accounts_file,delimiter=",",quotechar="'")
            # loop through all account holders, record their information, and close out their 'basic' accounts
            for acct_holder in accounts.values():
                # update the final overall stats
                accounts_writer.writerow(acct_holder.to_print())
    return accounts

def categorize_accounts(accounts,account_categories,order=None):
    for acct_holder in accounts.values():
        new_categs = set()
        for tuple in acct_holder.categs:
            src_tgt  = tuple.split('~')[0]
            txn_type = tuple.split('~')[1]
            new_categs.add(account_categories[src_tgt][txn_type]) if txn_type in account_categories[src_tgt] else new_categs.add("")
        acct_holder.categs = new_categs
        if order:
            for categ in order:
                if categ in acct_holder.categs:
                    acct_holder.categ = categ
                    break
    return accounts

def reset(accounts):
    for acct in accounts.values():
        acct.account = acct.starting_balance
        acct.last_seen = None
        acct.active_balance = {}
    return accounts
