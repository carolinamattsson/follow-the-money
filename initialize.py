'''
Initialize a payment processing system

Up-to-date code: https://github.com/Carromattsson/follow_the_money
Copyright (C) 2018 Carolina Mattsson, Northeastern University
'''

from datetime import datetime, timedelta
import math

class System():
    # A payment system, here, is little more than a dictionary of accounts that keeps track of its boundaries
    def __init__(self,transaction_header,timeformat,timewindow,boundary_type,transaction_categories=None,account_categories=None,category_order=None,category_follow=None):
        self.accounts = {}
        self.txn_header = transaction_header
        self.timeformat = timeformat
        self.timewindow = (datetime.strptime(timewindow[0],self.timeformat),datetime.strptime(timewindow[1],self.timeformat))
        self.boundary_type = boundary_type
        if self.boundary_type:
            if self.boundary_type == "transactions":
                self.txn_categs = transaction_categories
                self.get_txn_categ = lambda txn: self.txn_categs[txn.type]
            elif boundary_type == "accounts":
                self.categ_follow = set(category_follow)
                self.get_txn_categ = lambda txn: self.get_txn_categ_accts(txn.src_categ,txn.tgt_categ)
            elif boundary_type == "inferred_accounts":
                self.acct_categs = account_categories
                self.categ_order = category_order
                self.categ_follow = set(category_follow)
                self.get_txn_categ = lambda txn: self.get_txn_categ_accts(txn.src.categ,txn.tgt.categ)
        else:
            self.get_txn_categ = lambda txn: "transfer"
        self.has_balance = lambda txn: txn.src.balance>=(txn.amt+txn.rev)
    def has_account(self,acct_ID):
        return acct_ID in self.accounts
    def get_account(self,acct_ID):
        return self.accounts[acct_ID]
    def create_account(self,acct_ID):
        self.accounts[acct_ID] = Account(acct_ID)
        return self.accounts[acct_ID]
    def reset(self):
        for acct_ID,acct in self.accounts.items():
            acct.reset()
        return self
    def get_txn_categ_accts(self,src_categ,tgt_categ):
        # this method determines whether a transaction is a 'deposit', 'transfer', or 'withdraw' in cases where accounts are either provider-facing or public-facing, and only the latter reflect "real" use of the ecosystem
        # the determination is based on whether the source and target are on the public-facing or provider-facing side of the ecosystem
        src_follow = src_categ in self.categ_follow
        tgt_follow = tgt_categ in self.categ_follow
        if     src_follow and     tgt_follow: return 'transfer'
        if not src_follow and     tgt_follow: return 'deposit'
        if     src_follow and not tgt_follow: return 'withdraw'
        if not src_follow and not tgt_follow: return 'system'
    def read_balance_pre(self,txn):
        txn.src.balance = txn.src_balance
        txn.tgt.balance = txn.tgt_balance
        return txn.src.balance>=(txn.amt+txn.rev)
    def read_balance_post(self,txn):
        txn.src.balance = txn.src_balance+txn.amt+txn.rev
        txn.tgt.balance = txn.tgt_balance-txn.amt
        return txn.src.balance>=(txn.amt+txn.rev)
    def define_has_balance(self,balance_type):
        if balance_type == "pre":
            self.has_balance = lambda txn: self.read_balance_pre(txn)
        elif balance_type == "post":
            self.has_balance = lambda txn: self.read_balance_post(txn)
        else:
            self.has_balance = lambda txn: txn.src.balance>=(txn.amt+txn.rev)

class Transaction(object):
    # A transaction, here, contains the basic features of a transaction with references to the source and target accounts
    def __init__(self, src, tgt, txn_dict):
        # reference the accounts the transaction moves between
        self.src = src
        self.tgt = tgt
        # make the transaction attributes object properties
        for key, value in txn_dict.items():
            setattr(self, key, value)
        try:
            self.rev_ratio = self.rev/self.amt
        except:
            self.rev = 0
            self.rev_ratio = 0
    def __str__(self):
        # this prints out the basics of the original transaction, which is useful for debugging
        src_ID    = self.src.acct_ID
        tgt_ID    = self.tgt.acct_ID
        timestamp = datetime.strftime(self.timestamp,self.system.timeformat)
        return self.txn_ID+","+src_ID+","+tgt_ID+","+timestamp+","+self.type+","+str(self.amt)+","+str(self.rev)
    def to_print(self):
        return(str(self).split(','))
    @classmethod
    def create(cls,src,tgt,txn_dict,get_categ):
        # This method creates a Transaction object from a dictionary and object references to the source and target accounts
        # The dictionary here is read in from the file, and has System.txn_header as the keys
        txn_dict['timestamp'] = datetime.strptime(txn_dict['timestamp'],cls.system.timeformat)
        txn_dict['amt']       = float(txn_dict['amt'])
        txn_dict['rev']       = float(txn_dict['rev'])
        del txn_dict['src_ID']
        del txn_dict['tgt_ID']
        txn = cls(src,tgt,txn_dict)
        if get_categ: txn.categ = cls.system.get_txn_categ(txn)
        return txn

class Account(dict):
    # An account, here, contains the most important features of accounts and can contain tracking mechanisms
    def __init__(self, acct_ID, starting_balance=None):
        self.acct_ID  = acct_ID
        self.starting_balance = starting_balance if starting_balance else 0
        self.balance = self.starting_balance
        self.tracker = None
        self.categs = set()
        self.categ = None
    def close_out(self):
        self.balance = 0
        self.tracker = None
    def reset(self):
        self.balance = self.starting_balance
        self.tracker = None
    def update_categ(self, src_tgt, txn_type):
        # this collects the categories of account holder we've seen this user be
        if txn_type in self.system.acct_categs:
            self.categs.add(self.system.acct_categs[txn_type][src_tgt])
    def record_type(self, src_tgt, txn_type):
        # if categories are not externally defined we can remember what side of what transaction this holder has been on - helpful exploratory analysis
        self.categs.add(src_tgt+'+'+txn_type)
    def has_tracker(self):
        return True if self.tracker else False
    def track(self,Tracker_Class):
        self.tracker = Tracker_Class(self)
    def infer_balance(self, amt):
        # this function upps the running balance in the account, also adjusting the inferred starting balance
        self.starting_balance += amt
        self.balance += amt
    def add_balance(self,missing):
        # When the balance in an account is insufficient to cover it, we need to do something about it
        # If we are inferring deposit transactions we do so now, and if not we assume that the account actually *does* have enough balance we just didn't know it (they carried a starting_balance at the beginning of our data)
        if isinstance(self.tracker,list) and self.tracker.infer:
            self.tracker.infer_deposit(missing)
        self.infer_balance(missing)
    def deposit(self,txn,track=False):
        # this function deposits a transaction onto the account
        # if the account is tracking, make it happen
        if track:
            self.tracker.deposit(txn)
        # then, adjust the balance accordingly
        txn.src.balance = txn.src.balance-txn.amt-txn.rev
        self.balance += txn.amt
    def transfer(self,txn,track=False):
        # this function transfers a transaction from one account to another
        if track:
            self.tracker.transfer(txn)
        # then, adjust the balances accordingly
        self.balance = self.balance-txn.amt-txn.rev
        txn.tgt.balance += txn.amt
    def withdraw(self,txn,track=False):
        # this function processes a withdraw transaction from this account
        if track:
            yield from self.tracker.withdraw(txn)
        # then, adjust the balance accordingly
        self.balance = self.balance-txn.amt-txn.rev
        txn.tgt.balance += txn.amt

def setup_system(transaction_header,timeformat,timewindow,boundary_type=None,transaction_categories=None,account_columns=None,account_categories=None,category_order=None,category_follow=None):
    ############### Initialize system ##################
    if boundary_type:
        if boundary_type == "transactions":
            system = System(transaction_header,timeformat,timewindow,boundary_type,transaction_categories=transaction_categories)
        elif boundary_type == "accounts":
            system = System(transaction_header,timeformat,timewindow,boundary_type,category_follow=category_follow)
        elif boundary_type == "inferred_accounts":
            system = System(transaction_header,timeformat,timewindow,boundary_type,account_categories=account_categories,category_order=category_order,category_follow=category_follow)
    else:
        system = System(transaction_header,timeformat,timewindow,boundary_type,timewindow)
    ############ Make Classes System-aware #############
    Transaction.system = system
    Account.system = system
    return system

def load_accounts(accounts_file):
    return accounts

def initialize_transactions(txn_reader,system,report_file,get_categ=False):
    import traceback
    # Initialize the transaction. There are two steps:
    #                               1) Ensure the source and target accounts exist
    #                               3) Create the transaction object
    for txn in txn_reader:
        try:
            # define the transaction, creating accounts and trackers if needed
            src = system.get_account(txn['src_ID']) if system.has_account(txn['src_ID']) else system.create_account(txn['src_ID'])
            tgt = system.get_account(txn['tgt_ID']) if system.has_account(txn['tgt_ID']) else system.create_account(txn['tgt_ID'])
            yield Transaction.create(src,tgt,txn,get_categ)
        except:
            report_file.write("ISSUE W/ INITIALIZING: "+str(txn)+"\n"+traceback.format_exc()+"\n")

def get_account_categories(system,transaction_file,report_filename):
    import csv
    with open(transaction_file,'r') as txn_file, open(report_filename,'w') as report_file:
        txn_reader = csv.DictReader(txn_file,system.txn_header,delimiter=",",quotechar="'",escapechar="%")
        transactions = initialize_transactions(txn_reader,system,report_file)
        for txn in transactions:
            txn.src.update_categ('src',txn.type)
            txn.tgt.update_categ('tgt',txn.type)
    for acct_ID,account in system.accounts.items():
        for categ in system.categ_order:
            if categ in account.categs:
                account.categ = categ
                break
    return system

def get_starting_balance(system,transaction_file,report_filename,read_balance=None,boundary=False):
    import csv
    with open(transaction_file,'r') as txn_file, open(report_filename,'a') as report_file:
        txn_reader = csv.DictReader(txn_file,system.txn_header,delimiter=",",quotechar="'",escapechar="%")
        transactions = initialize_transactions(txn_reader,system,report_file)
        for txn in transactions:
            if not txn.system.has_balance(txn,"src"): txn.src.add_balance((txn.amt+txn.rev)-txn.src.balance)
            txn.src.transfer(txn)
    return system

def discover_account_categories(src,tgt,amt,rev=0,basics=None,txn_type=None):
    if not txn_type: txn_type = ''
    src.categs.add('src~'+txn_type)
    tgt.categs.add('tgt~'+txn_type)
    # update the account basics
    if basics:
        src.basics.setdefault(txn_type,{'txns_in':0,'txns_out':0,'amt_in':0,'amt_out':0,'rev':0,'alters_in':set(),'alters_out':set()})
        tgt.basics.setdefault(txn_type,{'txns_in':0,'txns_out':0,'amt_in':0,'amt_out':0,'rev':0,'alters_in':set(),'alters_out':set()})
        src.basics[txn_type]['txns_out'] += 1
        src.basics[txn_type]['amt_out']  += float(amt)
        src.basics[txn_type]['rev']      += float(rev)
        src.basics[txn_type]['alters_out'].add(tgt.acct_ID)
        tgt.basics[txn_type]['txns_in']  += 1
        tgt.basics[txn_type]['amt_in']   += float(amt)
        tgt.basics[txn_type]['alters_in'].add(src.acct_ID)
    return src, tgt

if __name__ == '__main__':
    print("Please run main.py, this file keeps classes and functions.")
