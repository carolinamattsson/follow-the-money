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
    def inferred_deposit(cls,txn):
        inferred_ID  = 'i'
        inferred_amt = txn.amt+txn.rev-txn.src_acct.balance
        return cls(inferred_ID,cls.begin_timestamp,'deposit',txn.src_acct,txn.src_acct,inferred_amt,rev=0,type='inferred')
    @classmethod
    def inferred_withdraw(cls,acct):
        inferred_ID = 'i'
        inferred_amt = acct.balance
        return cls(inferred_ID,cls.end_timestamp,'withdraw',acct,acct,inferred_amt,rev=0,type='inferred')
    @classmethod
    def from_file(cls,txn,categ,accts_dict):
        timestamp = datetime.strptime(txn['timestamp'],cls.timeformat)
        src_acct  = accts_dict[txn['src_ID']].account
        tgt_acct  = accts_dict[txn['tgt_ID']].account
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

class LIFO_account:
    # this class keeps track of transactions within an account holder's account
    # it chains outgoing transactions (well, parts of those transactions) to earlier incoming ones
    # specifically, it chains outgoing transactions to the *most recent* incoming transactions
    # this heuristic -- last in first out (LIFO) -- has the pleasing property of preserving local patterns
    account_type = "LIFO_account"
    def __init__(self, acct_ID):
        # this account initialization ensures there will always be a "root branch" in an account - useful for handling finite data - will be further developed using inferred deposit transactions
        self.stack   = []
        self.acct_ID = acct_ID
        self.balance = 0
    def balance_check(self, txn):
        # this returns True if there is enough in the account to process the transaction and False if not
        return True if txn.amt+txn.rev <= self.balance else False
    def add_branches(self, branches):
        # according to the LIFO heuristic, incoming transactions mean "branches" get added to the end of the account.stack
        self.stack.extend(branches)
        self.balance += sum(branch.amt for branch in branches)
    def pop_branches(self,this_txn,resolution_limit=0.01):
        # according to the LIFO heuristic, outgoing transactions mean "branches" are removed from the end of the account.stack
        amt = this_txn.amt+this_txn.rev
        if amt > self.balance: pass # raise accounting exception - to be implemented in the future
        branches = []
        while amt > 0:
            # "branches" are removed from the end of the account.stack until the amount of the transaction is reached
            branch = self.stack[-1]
            if branch.amt <= amt:
                branches.append(self.stack.pop())
                amt = amt - branch.amt
                self.balance = self.balance - branch.amt
            else:
                # If the last "branch" is larger than the amount to be removed from the account, it is split into two: one remains in this account and the other is sent along
                branches.append(Branch(branch.prev,branch.txn,amt))
                self.balance = self.balance - amt
                branch.decrement(amt)
                amt = 0
        new_branches = [Branch(branch, this_txn, branch.amt/(1.0+this_txn.rev_ratio)) for branch in reversed(branches) if branch.amt >= resolution_limit] # the list is reversed to preserve the newest branches at the end
        return new_branches
    def deposit(self,this_txn):
        # accounts process deposit transactions by adding a new "root branch" to the end of their account.stack
        self.add_branches([Branch(None,this_txn,this_txn.amt)])
    def transfer(self,this_txn,resolution_limit=0.01):
        # accounts process transfer transactions by:
        #     - removing branches up to the amount of the transaction from this account.stack
        #     - creating the subsequent "branches" (extending the "tree")
        #     - adding these "branches" to the end of the account.stack of the account receiving the transaction
        new_branches = self.pop_branches(this_txn,resolution_limit)
        this_txn.tgt_acct.add_branches(new_branches)
    def withdraw(self,this_txn,resolution_limit=0.01):
        # accounts process withdraw transactions by:
        #     - removing branches up to the amount of the transaction from this account.stack
        #     - creating the subsequent "leaf branches"
        #     - returning the "money flows" that end because of this withdraw transaction (by calling branch.follow_back() on the "leaf branches")
        #     - removing the "leaf branches" from memory
        new_branches = self.pop_branches(this_txn,resolution_limit)
        flows = [branch.follow_back(branch.amt) for branch in new_branches]
        del new_branches # branches will disappear when there are no accounts who still reference their upstream branches
                         # transactions will disappear when there are no branches left that reference them
        return flows

class Mixing_account:
    # this class keeps track of transactions within an account holder's account
    # it chains outgoing transactions (well, parts of those transactions) to earlier incoming ones
    # specifically, it chains outgoing transactions to *an equal fraction of all remaining* incoming transactions
    # this heuristic -- the well-mixed or max-entropy heuristic -- takes the perfectly fungible nature of money seriously
    account_type = "Mixing_account"
    def __init__(self, acct_ID):
        self.pool    = []
        self.acct_ID = acct_ID
        self.balance = 0
    def balance_check(self, txn):
        # this returns True if there is enough in the account to process the transaction and False if not
        return True if txn.amt+txn.rev <= self.balance else False
    def add_pool(self, new_pool):
        self.pool.extend(new_pool)
        self.balance += sum(branch.amt for branch in new_pool)
    def deposit(self, this_txn):
        # accounts process deposit transactions by adding a new "root branch" to their account.pool
        self.add_pool([Branch(None,this_txn,this_txn.amt)])
    def split_pool(self,this_txn,resolution_limit=0.01):
        # this splits the entire pool of incoming transaction into two, with (balance-(amt+rev)) remaining and (amt) returned
        # the old pool becomes of size balance-(amt+rev) with all the same branches
        # the new pool is of size (amt) and has all new "branches" in it, extending all the "trees" in the old pool
        # if the resulting branches are less than the minimum we're tracking, they are ignored
        if this_txn.amt > self.balance:
            pass # raise accounting exception - to be implemented in the future
        elif (self.balance-this_txn.amt) < resolution_limit:
            new_pool     = [Branch(branch, this_txn, branch.amt) for branch in self.pool]
            self.pool    = []
            self.balance = 0
        else:
            split_factor = this_txn.amt/self.balance
            new_pool     = [Branch(branch, this_txn, split_factor*branch.amt) for branch in self.pool if split_factor*branch.amt >= resolution_limit]
            stay_factor  = (self.balance-this_txn.amt-this_txn.rev)/self.balance
            for branch in self.pool:
                branch.depreciate(stay_factor)
            self.balance = self.balance-this_txn.amt-this_txn.rev
        return new_pool
    def transfer(self,this_txn,resolution_limit=0.01):
        # accounts process transfer transactions by:
        #     - splitting the existing pool of incoming transactions into a pool that stays and another that goes
        #     - adding the new pool to the account.pool of the account receiving the transaction
        new_pool = self.split_pool(this_txn,resolution_limit)
        this_txn.tgt_acct.add_pool(new_pool)
    def withdraw(self,this_txn,resolution_limit=0.01):
        # accounts process transfer transactions by:
        #     - splitting the existing pool of incoming transactions into a pool that stays and another that goes
        #     - adding the new pool to the account.pool of the account receiving the transaction
        new_pool = self.split_pool(this_txn,resolution_limit)
        flows = [branch.follow_back(branch.amt) for branch in new_pool]
        del new_pool # branches will disappear when there are no accounts who still reference their upstream branches
                     # transactions will disappear when there are no branches left that reference them
        return flows

class Account_holder:
    # this class defines accounts in the dataset and holds features of them

    # class variable define what account holders' transactions are being "followed" and how
    boundary_type = "transactions"
    txn_categs = {"deposit":"deposit","transfer":"transfer","withdraw":"withdraw"}
    # also the account categories
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
    def close_out(self):
        # this function infers a transaction that would bring the account down to zero, then withdraws it
        inferred_withdraw = Transaction.inferred_withdraw(self.account)
        return self.account.withdraw(inferred_withdraw)
    def update_categ(self, src_tgt, txn_type, generate=False):
        if txn_type in self.acct_categs[src_tgt]:
            self.categ.add(self.acct_categs[src_tgt][txn_type])
        elif generate:
            self.categ.add(src_tgt+'~'+txn_type)
        return None
    @classmethod
    def get_txn_categ(cls,txn,accts_dict):
        if cls.boundary_type == "none":
            return 'transfer'
        if cls.boundary_type == "transactions":
            return cls.txn_categs[txn['txn_type']] if txn['txn_type'] in cls.txn_categs else 'withdraw'
        if cls.boundary_type == "accounts":
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
        txn_categ = cls.get_txn_categ(txn,accts_dict)
        return accts_dict, txn_categ

def process(txn,resolution_limit=0.01,infer_deposits=True):
    # process the transaction!
    if txn.categ == 'deposit':
        return txn.tgt_acct.deposit(txn)
    elif txn.categ == 'transfer':
        if infer_deposits and not txn.src_acct.balance_check(txn): txn.src_acct.deposit(txn.inferred_deposit(txn))
        return txn.src_acct.transfer(txn,resolution_limit)
    elif txn.categ == 'withdraw':
        if infer_deposits and not txn.src_acct.balance_check(txn): txn.src_acct.deposit(txn.inferred_deposit(txn))
        return txn.src_acct.withdraw(txn,resolution_limit)

def process_remaining_funds(accts_dict,resolution_limit=0.01,infer=True):
    # loop through all accounts, and note the amount remaining as inferred withdraws
    for acct in accts_dict:
        if accts_dict[acct].account.balance > resolution_limit:
            moneyflows = accts_dict[acct].close_out()
            for moneyflow in moneyflows:
                yield moneyflow

def setup(header,timeformat,boundary_type,boundary_categories,following=None,timewindow=None):
    ################### SETUP! #########################
    # Set class variables for the Transaction class
    Transaction.header          = header
    Transaction.timeformat      = timeformat
    Transaction.begin_timestamp = datetime.strptime(timewindow[0],timeformat) if timewindow else None
    Transaction.end_timestamp   = datetime.strptime(timewindow[1],timeformat) if timewindow else None
    # Set class variables for the Account_holder class regarding the boundary of the network
    if boundary_type == "none":
        Account_holder.boundary_type = "none"
        Account_holder.txn_categs = None
        Account_holder.acct_categs = None # for now
    if boundary_type == "transactions":
        Account_holder.boundary_type = "transactions"
        Account_holder.txn_categs = boundary_categories
        Account_holder.acct_categs = None # for now
    elif boundary_type == "accounts":
        Account_holder.boundary_type = "accounts"
        Account_holder.txn_categs = None # for now
        Account_holder.acct_categs = boundary_categories
        Account_holder.follow_set = following
    else:
        print("Please define a valid boundary_type -- options are: 'none', 'transactions', and 'accounts'")
    return Transaction, Account_holder

def run(txn_file,moneyflow_file,issues_file,follow_heuristic,setup_func,modifier_func,resolution_limit=0.01,infer_deposits=True,infer_withdraw=True,discover_account_categories=False):
    import traceback
    import csv

    Transaction, Account_holder = setup_func

    # Based on the follow_heuristic, define the type of account we're giving our account_holders
    if follow_heuristic == "greedy":
        acct_Class = LIFO_account
    elif follow_heuristic == "well-mixed":
        acct_Class = Mixing_account
    else:
        print("Please define a valid follow_heuristic -- options are: 'greedy' and 'well-mixed'")
    ##################### RUN! ##########################
    # now we can open the transaction and output files!!
    with open(txn_file,'rU') as txn_file, open(moneyflow_file,'w') as moneyflow_file, open(issues_file,'w') as issues_file:
        txn_reader   = csv.DictReader(txn_file,Transaction.header,delimiter=",",quotechar="'",escapechar="%")
        moneyflow_writer  = csv.writer(moneyflow_file,delimiter=",",quotechar="'")
        issue_writer = csv.writer(issues_file,delimiter=",",quotechar="'")
        moneyflow_writer.writerow(Flow.header)
        # this dictionary holds all of the accounts
        accounts = {}
        # if we're discovering account categories, we need a dict to hold them
        discover_account_categories = {} if discover_account_categories else None
        # now we loop through all the transactions and process them!
        for txn in txn_reader:
            txn = modifier_func(txn)
            try:
                accounts, txn_categ = Account_holder.update_accounts(txn,accounts,acct_Class,discover_account_categories)
                # if we are imposing a delta_t, update those now (this will return flows! ok, do later, in own step)
                txn = Transaction.from_file(txn,txn_categ,accounts)
                moneyflows = process(txn,resolution_limit,infer_deposits)
                if moneyflows:
                    for moneyflow in moneyflows:
                        moneyflow_writer.writerow(moneyflow.to_print())
            except:
                issue_writer.writerow([str(txn)]+[traceback.format_exc()])
        moneyflows = process_remaining_funds(accounts,resolution_limit)
        for moneyflow in moneyflows:
            moneyflow_writer.writerow(moneyflow.to_print())

if __name__ == '__main__':
    print("Please run main.py, this file keeps the classes and functions.")
