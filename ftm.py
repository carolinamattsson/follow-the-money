'''
Follow The Money
This code defines classes and functions for the basic functionaliy
of "follow the money" -- an algorithm to turn a list of transactions into a
list of money flows, weighted trajectories of money through a system.
Author: Carolina Mattsson, Northeastern University, April 2018
'''
from datetime import datetime, timedelta
import copy

class Transaction:
    # contains the basic features of a transaction, creating references to the source and target accounts

    # class variables for the timeformat, begin_timestamp, and end_timestamp
    header = ['txn_ID','src_ID','tgt_ID','timestamp','txn_type','amt','rev']
    timeformat = "%Y-%m-%d %H:%M:%S"
    begin_timestamp = '2000-01-01 00:00:00'
    end_timestamp   = '2020-01-01 00:00:00'
    # class variable for the possible transaction categories ('deposit','transfer','withdraw')
    txn_categs = None

    def __init__(self, txn_ID, timestamp, categ, src_acct, tgt_acct, amt, rev=0, type=None):
        self.txn_ID    = txn_ID
        self.timestamp = timestamp
        self.categ     = categ
        self.type      = type
        self.src_acct  = src_acct
        self.tgt_acct  = tgt_acct
        self.amt       = amt
        self.rev       = rev
        self.rev_ratio = self.rev/self.amt
    def __str__(self):
        src_ID    = self.src_acct.acct_ID
        tgt_ID    = self.tgt_acct.acct_ID
        timestamp = datetime.strftime(self.timestamp,self.timeformat)
        return self.txn_ID+","+src_ID+","+tgt_ID+","+timestamp+","+self.type+","+str(self.amt)+","+str(self.rev)
    @classmethod
    def from_file(cls,txn,accts_dict):
        timestamp = datetime.strptime(txn['timestamp'],cls.timeformat)
        categ     = cls.get_txn_categ(txn)
        src_acct  = accts_dict[txn['src_ID']].account
        tgt_acct  = accts_dict[txn['tgt_ID']].account
        amt       = float(txn['amt'])
        rev       = float(txn['rev'])
        return cls(txn['txn_ID'],timestamp,categ,src_acct,tgt_acct,amt,rev=rev,type=txn['txn_type'])
    @classmethod
    def get_txn_categ(cls,txn):
        return cls.txn_categs[txn['txn_type']]
    @classmethod
    def infer_deposit(cls,txn):
        inferred_ID  = 'i'
        inferred_amt = txn.amt+txn.rev-txn.src_acct.balance
        return cls(inferred_ID,cls.begin_timestamp,'deposit',txn.src_acct,txn.src_acct,inferred_amt,rev=0,type='inferred')
    @classmethod
    def infer_withdraw(cls,acct):
        inferred_ID = 'i'
        inferred_amt = acct.balance
        return cls(inferred_ID,cls.end_timestamp,'withdraw',acct,acct,inferred_amt,rev=0,type='inferred')
    @classmethod
    def new(cls,txn,categ,src_acct,tgt_acct):
        timestamp = datetime.strptime(txn['timestamp'],cls.timeformat)
        amt       = float(txn['amt'])
        rev       = float(txn['rev'])
        return cls(txn['txn_ID'],timestamp,categ,src_acct,tgt_acct,amt,rev=rev,type=txn['txn_type'])

class Branch:
    # this class allows for chaining together transactions, or parts of those transactions
    def __init__(self, prev_branch, current_txn, amt):
        # "branches" reference the transaction they are a part of (branch.txn), and how much of that transaction they represent (branch.amt)
        # "root branches" reference deposit transactions, and are special in that their branch.prev references None
        # "leaf branches" reference withdraw transactions, and are special only in that they are treated differently by the account classes
        # subsequent transactions build a "tree" of regular "branches" that reference back to the "root branch" using branch.prev
        self.prev = prev_branch
        self.txn  = current_txn
        self.amt  = amt
    def decrement(self, amt):
        if amt > self.amt: # raise accounting exception - to be implemented in the future
            pass
        self.amt  = self.amt - amt
    def depreciate(self, factor):
        if factor > 1 or factor < 0: # raise accounting exception - to be implemented in the future
            pass
        self.amt  = factor * self.amt
    def follow_back(self, amt):
        # this function takes a chain of "branches", beginning with a "leaf branch", and works its way back to the "root branch"
        # on its way back, this builds a "money flow" that represents a unique trajectory that money followed through the system
        rev = amt*self.txn.rev_ratio
        if self.prev:
            # this is recursive... regular "branches" asks their previous "branch" for its flow, of a given amount, then adds its own
            flow = self.prev.follow_back(amt+rev)
            flow.extend(self, amt, rev)
        else:
            # "root branches" begin building the flow with the amount given to it
            flow = Flow(self, amt, rev)
        return flow

class Flow:
    # this class allows us to represent unique trajectories that specific amounts of money follow through the system
    # "money flows" allow for useful aggregations at the system level where monetary units are never double-counted

    # class variable defines what flow.to_print() currently outputs
    header = ['flow_timestamp','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_txn_timestamps','flow_durations','flow_amt','flow_rev','flow_frac_root','flow_tux','flow_tux_wrev','flow_duration']

    def __init__(self, branch, amt, rev):
        # "money flows" have a size (flow.amt), a length (flow.tux), and a duration of time that they remained in the system (flow.duration)
        # the specific trajectory is described by a list of transactions, through a list of accounts, where the money stayed for a list of durations
        # when aggregating over "money flows", they can be weighted by their size or by their root transactions using flow.frac_root
        self.timestamp = branch.txn.timestamp
        self.txn_IDs   = [branch.txn.txn_ID]
        self.txn_types = [branch.txn.type]
        self.timestamps= [branch.txn.timestamp]
        self.acct_IDs  = [branch.txn.src_acct.acct_ID,branch.txn.tgt_acct.acct_ID]
        self.amt       = amt+rev
        self.rev       = rev
        self.frac_root = (amt+rev)/(branch.txn.amt+branch.txn.rev)
        self.duration  = timedelta(0)
        self.durations = []
        self.tux       = 0  # "Transactions Until eXit" - deposited money begins at step 0, and any subsequent transaction adds 1 to this measure
        self.tux_wrev  = 0  #                           - strictly speaking, this measure aught to be adjusted by any revenue/fees incurred at each step
    def extend(self, branch, amt, rev):
        # this funciton builds up a "money flow" by incorporating the information in a subsequent "branch"
        # this is called inside the recursive function branch.follow_back(amt)
        self.txn_IDs.append(branch.txn.txn_ID)
        self.acct_IDs.append(branch.txn.tgt_acct.acct_ID)
        self.txn_types.append(branch.txn.type)
        self.timestamps.append(branch.txn.timestamp)
        self.rev       += rev
        branch_duration = branch.txn.timestamp - branch.prev.txn.timestamp
        self.duration  += branch_duration
        self.durations.append(branch_duration)
        self.tux       += 1
        self.tux_wrev  += amt/self.amt
    def to_print(self):
        # this returns a version of this class that can be exported to a file using writer.writerow()
        return [str(self.timestamp),'['+','.join(id for id in self.acct_IDs)+']','['+','.join(id for id in self.txn_IDs)+']','['+','.join(type for type in self.txn_types)+']',\
                '['+','.join(str(time) for time in self.timestamps)+']','['+','.join(str(dur.total_seconds()/3600.0) for dur in self.durations)+']',\
                self.amt,self.rev,self.frac_root,self.tux,self.tux_wrev,self.duration.total_seconds()/3600.0]

class Account(list):
    type = "basic"
    def __init__(self, acct_ID):
        self.acct_ID = acct_ID
        self.balance = 0
        self.tracked = 0
    def balance_check(self, txn):
        # this returns True if there is enough in the account to process the transaction and False if not
        return True if txn.amt+txn.rev <= self.balance else False
    def add_branches(self, branches):
        self.extend(branches)
        self.tracked += sum(branch.amt for branch in branches)
    def get_branches(self,this_txn,resolution_limit=0.01):
        new_branches = [Branch(None,this_txn,this_txn.amt)]
        return new_branches
    def deposit(self,this_txn):
        self.add_branches([Branch(None,this_txn,this_txn.amt)])
        self.balance += this_txn.amt                             # adjust the overall balance
    def transfer(self,this_txn,resolution_limit=0.01):
        new_branches = self.get_branches(this_txn,resolution_limit)
        this_txn.tgt_acct.add_branches(new_branches)
        self.balance = self.balance-this_txn.amt-this_txn.rev    # adjust the overall balance
        this_txn.tgt_acct.balance += this_txn.amt
    def withdraw(self,this_txn,resolution_limit=0.01):
        new_branches = self.get_branches(this_txn,resolution_limit)
        flows = [branch.follow_back(branch.amt) for branch in new_branches]
        self.balance = self.balance-this_txn.amt-this_txn.rev   # adjust the overall balance
        return flows
    def stop_tracking(self,timestamp=None,time_cutoff=None):
        if timestamp and time_cutoff:
            flows = []
            for branch in self:
                if (timestamp-branch.txn.timestamp)>time_cutoff:
                    flows.append(branch.follow_back(branch.amt))
                    self.remove(branch)
                    self.tracked -= branch.amt
        else:
            flows        = [branch.follow_back(branch.amt) for branch in self]
            self[:]      = []
            self.tracked = 0
        return flows

class LIFO_account(Account):
    # this class keeps track of transactions within an account holder's account
    # it chains outgoing transactions (well, parts of those transactions) to earlier incoming ones
    # specifically, it chains outgoing transactions to the *most recent* incoming transactions
    # this heuristic -- last in first out (LIFO) -- has the pleasing property of preserving local patterns
    type = "LIFO_account"
    def get_branches(self,this_txn,resolution_limit=0.01):
        # according to the LIFO heuristic, outgoing transactions mean "branches" are removed from the end of the account.stack
        amt = min(this_txn.amt+this_txn.rev,self.tracked)
        amt_missing = this_txn.amt+this_txn.rev-self.tracked if this_txn.amt+this_txn.rev>self.tracked else None
        branches = []
        while amt > resolution_limit:
            # "branches" are removed from the end of the account list until the amount of the transaction is reached
            branch = self[-1]
            if branch.amt <= amt:
                branches.append(self.pop())
                amt = amt - branch.amt
                self.tracked = self.tracked - branch.amt
            else:
                # If the last "branch" is larger than the amount to be removed from the account, it is split into two: one remains in this account and the other is sent along
                branches.append(Branch(branch.prev,branch.txn,amt))
                self.tracked = self.tracked - amt
                branch.decrement(amt)
                amt = 0
        new_stack   = [Branch(branch, this_txn, branch.amt/(1.0+this_txn.rev_ratio)) for branch in reversed(branches) if branch.amt > resolution_limit] # the list is reversed to preserve the newest branches at the end
        if amt_missing and amt_missing > resolution_limit: new_stack.append(Branch(None,this_txn,amt_missing/(1.0+this_txn.rev_ratio)))
        return new_stack

class Mixing_account(Account):
    # this class keeps track of transactions within an account holder's account
    # it chains outgoing transactions (well, parts of those transactions) to earlier incoming ones
    # specifically, it chains outgoing transactions to *an equal fraction of all remaining* incoming transactions
    # this heuristic -- the well-mixed or max-entropy heuristic -- takes the perfectly fungible nature of money seriously
    type = "Mixing_account"
    def get_branches(self,this_txn,resolution_limit=0.01):
        # this splits the entire pool of incoming transaction into two, with a fraction remaining and (amt) returned
        # the old pool becomes a fraction (balance-amt-rev)/balance smaller with all the same branches
        # the new pool is of size (amt) and has all new "branches" in it, extending all the "trees" in the old pool and creating new "roots" if need be
        # note that if any resulting branches are less than the minimum we're tracking, they are not extended
        # build the new pool
        split_factor = this_txn.amt/self.balance
        new_pool     = [Branch(branch, this_txn, split_factor*branch.amt) for branch in self if split_factor*branch.amt >= resolution_limit]
        amt_tracked  = sum(branch.amt for branch in new_pool)
        if (this_txn.amt-amt_tracked) > resolution_limit:
            new_pool.append(Branch(None,this_txn,(this_txn.amt-amt_tracked)))
        # shrink the old pool
        if (self.tracked-amt_tracked) < resolution_limit:
            self[:]      = []
            self.tracked = 0
        else:
            stay_factor  = (self.balance-this_txn.amt-this_txn.rev)/self.balance
            for branch in self:
                branch.depreciate(stay_factor)
            self.tracked = stay_factor*self.tracked
        return new_pool

class Account_holder:
    # this class defines accounts in the dataset and holds features of them

    # class variable defining the possible account categories, and those being 'followed'
    acct_categs = None
    follow_set = set()

    def __init__(self, acct_ID, acct_Class):
        self.acct_ID = acct_ID
        self.account = acct_Class(acct_ID)
        self.categ   = set()
        #self.txns    = 0      # in the future, this class will hold optional metrics calculated
        #self.amt     = 0      #                in an ongoing manner that can be retrieved system-wide
        #self.volume  = 0      #                at specified intervals
        #self.active  = 0
        self.discover = False
        self.starting_balance = 0 # in the future, this will be used to loop through *twice* with the Mixing_account version - once to calculate starting_balance and once to "follow" money
    def close_out(self):
        del self.account
    def update_categ(self, src_tgt, txn_type, generate=False):
        if txn_type in self.acct_categs[src_tgt]:
            self.categ.add(self.acct_categs[src_tgt][txn_type])
        elif generate:
            self.categ.add(src_tgt+'~'+txn_type)
    def set_balance(self,amt,infer=True):
        # this function infers a notional balance on the account, recording the inferred starting balance if directed
        if infer: self.starting_balance += amt
        self.account.balance   = amt
    @classmethod
    def get_txn_categ(cls,txn,accts_dict):
        src_follow = accts_dict[txn['src_ID']].categ.issubset(cls.follow_set)
        tgt_follow = accts_dict[txn['tgt_ID']].categ.issubset(cls.follow_set)
        if not src_follow and tgt_follow: return 'deposit'
        if src_follow and tgt_follow:     return 'transfer'
        if src_follow and not tgt_follow: return 'withdraw'
    @classmethod
    def update_accounts(cls,txn,accts_dict,acct_Class,discover_acct_categs=None):
        # make sure both the sender and recipient are account_holders with accounts
        accts_dict.setdefault(txn['src_ID'],Account_holder(txn['src_ID'],acct_Class))
        accts_dict.setdefault(txn['tgt_ID'],Account_holder(txn['tgt_ID'],acct_Class))
        # if we are keeping track of account categories, update those now
        if cls.acct_categs or discover_acct_categs:
            accts_dict[txn['src_ID']].update_categ('src',txn['txn_type'],generate=True)
            accts_dict[txn['tgt_ID']].update_categ('tgt',txn['txn_type'],generate=True)
        return accts_dict[txn['src_ID']].account, accts_dict[txn['tgt_ID']].account

def get_acct_Class(follow_heuristic):
    # Based on the follow_heuristic, define the type of account we're giving our account_holders
    if follow_heuristic == "none":
        return Account
    if follow_heuristic == "greedy":
        return LIFO_account
    elif follow_heuristic == "well-mixed":
        return Mixing_account
    else:
        print("Please define a valid follow_heuristic -- options are: 'none', 'greedy', and 'well-mixed'")

def get_txn_categ(boundary_type,txn,Transaction,accts_dict,Account_holder):
    # Based on the boundary type, define where to look for the transaction category
    if boundary_type == "none":
        return 'transfer'
    elif boundary_type == "transactions":
        return Transaction.get_txn_categ(txn)
    elif boundary_type == "accounts":
        return Account_holder.get_txn_categ(txn,accts_dict)
    else:
        print("Please define a valid boundary_type -- options are: 'none', 'transactions', and 'accounts'")

def setup(follow_heuristic,boundary_type,header,timeformat,transaction_categories=None,account_categories=None,following=None,discover_account_categories=False,timewindow=None):
    ################### SETUP! #########################
    # Set class variables for the Transaction class
    Transaction.header          = header
    Transaction.timeformat      = timeformat
    Transaction.begin_timestamp = datetime.strptime(timewindow[0],timeformat) if timewindow else None
    Transaction.end_timestamp   = datetime.strptime(timewindow[1],timeformat) if timewindow else None
    Transaction.txn_categs      = transaction_categories
    # Set class variables for the Account_holder class
    Account_holder.acct_categs  = account_categories
    Account_holder.follow_set   = following
    Account_holder.discover     = discover_account_categories
    acct_Class                  = get_acct_Class(follow_heuristic)
    return boundary_type, acct_Class, Transaction, Account_holder

def adjust_balance(txn,accounts,infer_deposits=True):
    if not txn.src_acct.balance_check(txn):
        if infer_deposits:
            txn.src_acct.deposit(txn.infer_deposit(txn))
        else:
            accounts[txn.src_acct.acct_ID].set_balance(txn.amt+txn.rev,infer=True)

def process(txn,accounts,time_cutoff,resolution_limit=0.01,infer_deposits=True):
    # process the transaction!
    if txn.categ == 'deposit':
        flows = txn.tgt_acct.stop_tracking(txn.timestamp,time_cutoff)
        txn.tgt_acct.deposit(txn)
    elif txn.categ == 'transfer':
        adjust_balance(txn,accounts,infer_deposits)
        flows = txn.src_acct.stop_tracking(txn.timestamp,time_cutoff) + txn.tgt_acct.stop_tracking(txn.timestamp,time_cutoff)
        txn.src_acct.transfer(txn,resolution_limit)
    elif txn.categ == 'withdraw':
        adjust_balance(txn,accounts,infer_deposits)
        flows = txn.src_acct.stop_tracking(txn.timestamp,time_cutoff)
        flows = flows + txn.src_acct.withdraw(txn,resolution_limit)
    else:
        flows = []
    return flows

def process_remaining_funds(accts_dict,infer_withdraw=False,resolution_limit=0.01,txn_Class=None):
    # loop through all accounts, and note the amount remaining
    for acct_ID in accts_dict:
        moneyflows = []
        acct = accts_dict[acct_ID].account
        if infer_withdraw and acct.balance > resolution_limit:
            inferred_withdraw = txn_Class.infer_withdraw(acct)
            moneyflows.extend(acct.withdraw(inferred_withdraw))
        elif acct.tracked > resolution_limit:
            moneyflows.extend(acct.stop_tracking())
        accts_dict[acct_ID].close_out()
        for moneyflow in moneyflows:
            yield moneyflow

def run(txn_file,moneyflow_file,issues_file,setup_func,time_cutoff=timedelta.max,modifier_func=None,resolution_limit=0.01,infer_deposits=False,infer_withdraw=False,discover_account_categories=False,discover_file=None):
    import traceback
    import csv
    #################### SETUP! #########################
    boundary_type, acct_Class, Transaction, Account_holder = setup_func
    ##################### RUN! ##########################
    # now we can open the transaction and output files!!
    with open(txn_file,'rU') as txn_file, open(moneyflow_file,'w') as moneyflow_file, open(issues_file,'w') as issues_file:
        txn_reader   = csv.DictReader(txn_file,Transaction.header,delimiter=",",quotechar="'",escapechar="%")
        moneyflow_writer  = csv.writer(moneyflow_file,delimiter=",",quotechar="'")
        issue_writer = csv.writer(issues_file,delimiter=",",quotechar="'")
        moneyflow_writer.writerow(Flow.header)
        # this dictionary holds all of the accounts
        accounts = {}
        # now we loop through all the transactions and process them!
        for txn in txn_reader:
            txn = modifier_func(txn) if modifier_func else txn
            try:
                src_acct, tgt_acct = Account_holder.update_accounts(txn,accounts,acct_Class,discover_account_categories)
                txn_categ = get_txn_categ(boundary_type,txn,Transaction,accounts,Account_holder)
                txn = Transaction.new(txn,txn_categ,src_acct,tgt_acct)
                moneyflows = process(txn,accounts,time_cutoff,resolution_limit,infer_deposits)
                if moneyflows:
                    for moneyflow in moneyflows:
                        moneyflow_writer.writerow(moneyflow.to_print())
            except:
                issue_writer.writerow([str(txn)]+[traceback.format_exc()])
        moneyflows = process_remaining_funds(accounts,infer_withdraw,resolution_limit,Transaction)
        for moneyflow in moneyflows:
            moneyflow_writer.writerow(moneyflow.to_print())

if __name__ == '__main__':
    print("Please run main.py, this file keeps the classes and functions.")
