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

    # Class variables for the file, file header, timeformat, begin_timestamp, and end_timestamp
    # These are modified using the setup_data() function
    file            = None
    header          = ['txn_ID','src_ID','tgt_ID','timestamp','txn_type','amt','rev']
    timeformat      = "%Y-%m-%d %H:%M:%S"
    begin_timestamp = '2000-01-01 00:00:00'
    end_timestamp   = '2020-01-01 00:00:00'
    # Class variable for the mapping that delineates the network boundary when it is defined by transactions of a specific type
    # Specifically, it is a dictionary mapping the data's transaction types to one of the three transaction categories: 'deposit','transfer', or 'withdraw'
    # Transaction types found in the data that do not have a mapping are presumed to be 'withdraw's
    # This dictionary is defined in the setup() function
    txn_categs = None

    def __init__(self, txn_ID, timestamp, src_acct, tgt_acct, amt, rev=0, type=None, categ=None):
        # define the object properties
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
        # this prints out the basics of the original transaction, which is useful for debugging
        src_ID    = self.src_acct.acct_ID
        tgt_ID    = self.tgt_acct.acct_ID
        timestamp = datetime.strftime(self.timestamp,self.timeformat)
        return self.txn_ID+","+src_ID+","+tgt_ID+","+timestamp+","+self.type+","+str(self.amt)+","+str(self.rev)
    @classmethod
    def create(cls,txn_dict,src_acct,tgt_acct):
        # This method creates a Transaction object from a dictionary and object references to the source and target accounts
        # The dictionary here is read in from the file, and has Transaction.header as the keys
        timestamp = datetime.strptime(txn_dict['timestamp'],cls.timeformat)
        src_acct  = src_acct
        tgt_acct  = tgt_acct
        amt       = float(txn_dict['amt'])
        rev       = float(txn_dict['rev'])
        return cls(txn_dict['txn_ID'],timestamp,src_acct,tgt_acct,amt,rev=rev,type=txn_dict['txn_type'])
    @classmethod
    def get_txn_categ(cls,txn):
        return cls.txn_categs[txn.type]

class Branch:
    # this class allows for chaining together transactions, or parts of those transactions
    def __init__(self, prev_branch, current_txn, amt):
        # "branches" reference the transaction they are a part of (branch.txn), and how much of that transaction they represent (branch.amt)
        # "root branches" are special in that their branch.prev references None - deposits are necessarily "root branches"
        # "leaf branches" are special in that they are treated differently by the Account class - withdraws are necessarily "leaf branches"
        # subsequent transactions build a "tree" of regular "branches" that reference back to the "root branch" using branch.prev
        self.prev = prev_branch
        self.txn  = current_txn
        self.amt  = amt
    def decrement(self, amt):
        if amt > self.amt: # a good place to raise an accounting exception...
            pass
        self.amt  = self.amt - amt
    def depreciate(self, factor):
        if factor > 1 or factor < 0: # a good place to raise an accounting exception...
            pass
        self.amt  = factor * self.amt
    def follow_back(self, amt):
        # This is called by the Account class on "leaf branches"
        # This function follows a chain of "branches", beginning with a "leaf branch", and works its way back to the "root branch"
        # On its way up again it builds a "money flow" that represents a unique trajectory that money followed through the system
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
    # This Class allows us to represent unique trajectories that specific amounts of money follow through the system
    # These "money flows" allow for useful aggregations at the system level where monetary units are never double-counted

    # Class variable defines what flow.to_print() currently outputs
    header = ['flow_timestamp','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_txn_timestamps','flow_durations','flow_amt','flow_rev','flow_frac_root','flow_tux','flow_tux_wrev','flow_duration']

    def __init__(self, branch, amt, rev):
        # "money flows" have a size (flow.amt), a length within the system (flow.tux), and a duration of time that they remained in the system (flow.duration)
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
        self.tux       = 1 if branch.txn.categ == 'transfer' else 0                                              # "Transfers Until eXit" - deposited money begins at step 0, and any subsequent 'transfer' adds 1 to this measure
        self.tux_wrev  = branch.txn.amt/(branch.txn.amt+branch.txn.rev) if branch.txn.categ == 'transfer' else 0 #                        - strictly speaking, this measure aught to be adjusted by any revenue/fees incurred at each step
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
        self.tux       += 1 if branch.txn.categ == 'transfer' else 0             # neither 'deposit' nor 'withdraw' transactions are included in the "Transfer Until eXit" measure, only transaction within the system itself
        self.tux_wrev  += amt/self.amt if branch.txn.categ == 'transfer' else 0
    def to_print(self):
        # this returns a version of this class that can be exported to a file using writer.writerow()
        return [str(self.timestamp),'['+','.join(id for id in self.acct_IDs)+']','['+','.join(id for id in self.txn_IDs)+']','['+','.join(type for type in self.txn_types)+']',\
                '['+','.join(str(time) for time in self.timestamps)+']','['+','.join(str(dur.total_seconds()/3600.0) for dur in self.durations)+']',\
                self.amt,self.rev,self.frac_root,self.tux,self.tux_wrev,self.duration.total_seconds()/3600.0]

class Account(list):
    # Contains the basic features of an account that keeps track of transactions moving through it
    # Accounts always remember their overall balance, and specifically track transactions that entered the account recently

    # Class variable defines how Accounts are tracking money, for how long an account will remember where money came from, and down to what amount it will keep track
    type = "basic"
    time_cutoff = timedelta.max
    resolution_limit = 0.01
    txn_Class = None

    def __init__(self, holder, acct_ID):
        # Accounts are initialized to reference:
        self.holder  = holder                     # The Account_holder instance that owns them
        self.acct_ID = acct_ID                    # An account number (unique ID)
        self.balance = holder.starting_balance    # The running balance on the account
        self.tracked = 0                          # The amount of money currently being tracked - this is at most equal to the running balance
    def balance_check(self, txn):
        # this returns True if there is enough in the account to process the outgoing transaction and False if not
        return True if txn.amt+txn.rev <= self.balance else False
    def infer_balance(self,amt):
        # this function ups the running balance in the account, also updating the inferred starting balance of the account_holder
        self.balance += amt
        self.holder.starting_balance += amt
    def add_branches(self, branches):
        # this function adds a list of branches to the account, and updates the tracked balance accordingly
        self.extend(branches)
        self.tracked += sum(branch.amt for branch in branches)
    def extend_branches(self,this_txn):
        # this function extends the branches in this account by the outgoing transaction, and returns a list of these new branches
        # how the account extends the branches that it's tracking is governed by the tracking heuristic noted in the Account.type
        #    note that if branches are removed from the account in this function, that must be reflected in the tracked balance
        # this "basic" version offers no tracking at all
        #    only one branch is returned, which is a new "root branch" that corresponds directly to the transaction itself
        new_branches = [Branch(None,this_txn,this_txn.amt)]
        return new_branches
    def stop_tracking(self,timestamp=None):
        # this function finds the "leaf branches" in this account, builds the "money flows" that thus end at this account, returns those "money flows", and stops tracking those "leaf branches"
        #    if a timestamp is given, flows that are older than Account.time_cutoff are considered "leaf branches"
        #    if no timestamp is given, all flows are considered "leaf branches"
        # the tracked balance is adjusted accordingly
        if timestamp:
            flows = []
            for branch in self:
                if (timestamp-branch.txn.timestamp)>self.time_cutoff:
                    flows.append(branch.follow_back(branch.amt))
                    self.remove(branch)
                    self.tracked -= branch.amt
        else:
            flows        = [branch.follow_back(branch.amt) for branch in self]
            self[:]      = []
            self.tracked = 0
        return flows
    def deposit(self,this_txn):
        # this function deposits a transaction onto the account
        #    it adjusts the account balance accordingly and begins to track a "root branch" that corresponds directly to the deposit transaction
        self.add_branches([Branch(None,this_txn,this_txn.amt)])
        self.balance += this_txn.amt                             # adjust the overall balance
    def infer_deposit(self,amt):
        # this function creates an inferred Transaction object and deposits it onto the account
        self.deposit(self.txn_Class('i',self.txn_Class.begin_timestamp,self,self,amt,rev=0,type='inferred',categ='deposit'))
    def transfer(self,this_txn):
        # this function processes an outgoing transaction from this account onto a receiving account
        #    it adjusts both account balances accordingly, extends the branches in the account by this transaction, and adds these new branches onto the receiving account
        new_branches = self.extend_branches(this_txn)
        this_txn.tgt_acct.add_branches(new_branches)
        self.balance = self.balance-this_txn.amt-this_txn.rev    # adjust the overall balance
        this_txn.tgt_acct.balance += this_txn.amt
    def withdraw(self,this_txn):
        # this function processes a withdraw transaction from this account
        #    it adjusts the account balance accordingly, extends the branches in the account by this transaction, builds the "money flows" that leave the system via this transaction, and returns the completed "money flows"
        new_branches = self.extend_branches(this_txn)
        flows = [branch.follow_back(branch.amt) for branch in new_branches]
        self.balance = self.balance-this_txn.amt-this_txn.rev   # adjust the overall balance
        return flows
    def infer_withdraw(self,amt):
        # this function creates an inferred Transaction object and withdraws it from the account
        return self.withdraw(self.txn_Class('i',self.txn_Class.end_timestamp,self,self,amt,rev=0,type='inferred',categ='deposit'))

class LIFO_account(Account):
    type = "LIFO_account"
    # this account type keeps track of transactions within an account in time order -- a last in first out (LIFO) heuristic
    # intuitively, each account is a stack where incoming money lands on top and outgoing money gets taken off the top
    # specifically, it extends the *most recent* incoming branches by the outgoing transaction up to the value of that transaction
    # this heuristic has the pleasing property of preserving local patterns
    def extend_branches(self,this_txn):
        # according to the LIFO heuristic, the "branches" to be extended are removed from the end of the account
        # the tracked balance is adjusted accordingly
        amt = min(this_txn.amt+this_txn.rev,self.tracked)
        amt_untracked = this_txn.amt+this_txn.rev-self.tracked if this_txn.amt+this_txn.rev>self.tracked else None
        branches = []
        while amt > self.resolution_limit:
            # "branches" are removed from the end of the account list until the amount of the transaction is reached
            branch = self[-1]
            if branch.amt <= amt:
                branches.append(self.pop())
                amt = amt - branch.amt
                self.tracked = self.tracked - branch.amt
            else:
                # If the last "branch" is larger than the amount to be removed from the account, it is split into two: one remains in this account and the other is extended
                branches.append(Branch(branch.prev,branch.txn,amt))
                self.tracked = self.tracked - amt
                branch.decrement(amt)
                amt = 0
        # the removed branches are extended, note that the list is reversed to preserve the newest branches at the end, note also that if any resulting branches are less than the minimum we're tracking, they are not extended
        new_stack = [Branch(branch, this_txn, branch.amt/(1.0+this_txn.rev_ratio)) for branch in reversed(branches) if branch.amt > self.resolution_limit]
        # if the outgoing transaction is larger than the amount being tracked by this account, a new "root branch" is created that corresponds to the transaction itself and the untracked amount (not including the untracked fee/revenue)
        if amt_untracked and amt_untracked > self.resolution_limit: new_stack.append(Branch(None,this_txn,amt_untracked/(1.0+this_txn.rev_ratio)))
        return new_stack

class Mixing_account(Account):
    type = "Mixing_account"
    # this account type keeps track of transactions within an account entirely agnostically -- a well-mixed or max-entropy heuristic
    # intuitively, each account is a pool of indistinguishable money
    # specifically, it extends *an equal fraction of all remaining branches* by the outgoing transaction
    # this heuristic takes the perfectly fungible nature of money literally
    def extend_branches(self,this_txn):
        # according to the well-mixed heuristic, all the "branches" in an account are to be extended, and this depreciates their remaining value
        # the tracked balance is adjusted accordingly
        split_factor = this_txn.amt/self.balance                               # note that this_txn.rev/self.balance dissappears into the ether...
        stay_factor  = (self.balance-this_txn.amt-this_txn.rev)/self.balance
        # all the "branches" in an account are extended by the outgoing transaction, note that if any resulting branches are less than the minimum we're tracking, they are not extended
        new_pool     = [Branch(branch, this_txn, split_factor*branch.amt) for branch in self if split_factor*branch.amt >= self.resolution_limit]
        # when there is untracked money also in the account this new_pool will not cover the amount of the transaction - the transaction also sends untracked money!
        # so, a new "root branch" is created with the balance that references this transaction itself begins to re-track this untracked money again - this branch corresponds to the transaction itself and the newly tracked amount
        amt_untracked = this_txn.amt-sum(branch.amt for branch in new_pool)
        if amt_untracked > self.resolution_limit:
            new_pool.append(Branch(None,this_txn,amt_untracked))
        # the old pool is emptied or shrunk to reflect the amount removed
        if stay_factor*self.tracked < self.resolution_limit:
            self[:]      = []
            self.tracked = 0
        else:
            for branch in self:
                branch.depreciate(stay_factor)
            self.tracked = stay_factor*self.tracked
        return new_pool

class Account_holder:
    # This class defines account holders in the dataset and collects features of them, including a reference to their account

    # Class variable defining the possible account holder categories, and those being 'followed'
    holder_categs = None
    follow_set = set()

    def __init__(self, user_ID, acct_Class, starting_balance = None):
        self.user_ID  = user_ID
        self.starting_balance = starting_balance if starting_balance else 0     # The balance that an account had the first time we see it - this can be defined if we know it at initialization, and is inferred if we do not
        self.account  = acct_Class(self,user_ID)
        self.categ    = set()
    def close_out(self):
        # this removes the top-down reference to the actual account
        del self.account
    def update_categ(self, src_tgt, txn_type, discover):
        # this collects the categories of account holder we've seen this user be
        # if categories are not externally defined it can be told to remember what side of what transaction this holder has been on - helpful exploratory analysis
        if txn_type in self.holder_categs[src_tgt]:
            self.categ.add(self.holder_categs[src_tgt][txn_type])
        elif discover:
            self.categ.add(src_tgt+'~'+txn_type)
    @classmethod
    def get_txn_categ(cls,src_acct_holder,tgt_acct_holder):
        # this method determines whether a transaction is a 'deposit', 'transfer', or 'withdraw' in cases where accounts are either provider-facing or public-facing, and only the latter reflect "real" use of the ecosystem
        # the determination is based on whether the source and target are on the public-facing or provider-facing side of the ecosystem
        src_follow = src_acct_holder.categ.issubset(cls.follow_set)
        tgt_follow = tgt_acct_holder.categ.issubset(cls.follow_set)
        if not src_follow and tgt_follow: return 'deposit'
        if src_follow and tgt_follow:     return 'transfer'
        if src_follow and not tgt_follow: return 'withdraw'
    @classmethod
    def update_accounts(cls,txn,accts_dict,acct_Class,discover=False):
        # make sure both the sender and recipient are account_holders with accounts
        accts_dict.setdefault(txn['src_ID'],Account_holder(txn['src_ID'],acct_Class))
        accts_dict.setdefault(txn['tgt_ID'],Account_holder(txn['tgt_ID'],acct_Class))
        # if we are keeping track of account holder categories, update those now
        if cls.holder_categs or discover:
            accts_dict[txn['src_ID']].update_categ('src',txn['txn_type'],discover)
            accts_dict[txn['tgt_ID']].update_categ('tgt',txn['txn_type'],discover)
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

def get_txn_categ(txn,boundary_type):
    # Based on the boundary type, define where to look for the transaction category
    if boundary_type == "none":
        return 'transfer'
    elif boundary_type == "transactions":
        return txn.get_txn_categ(txn)
    elif boundary_type == "accounts":
        return txn.src_acct.holder.get_txn_categ(txn.src_acct.holder,txn.tgt_acct.holder)
    else:
        print("Please define a valid boundary_type -- options are: 'none', 'transactions', and 'accounts'")

def setup_data(txn_file,txn_header,timeformat,boundary_type="none",transaction_categories=None,account_categories=None,following=None,timewindow=None,modifier_func=None):
    #################### Basics ########################
    Transaction.file            = txn_file
    Transaction.header          = txn_header
    Transaction.timeformat      = timeformat
    Transaction.begin_timestamp = datetime.strptime(timewindow[0],timeformat) if timewindow else None
    Transaction.end_timestamp   = datetime.strptime(timewindow[1],timeformat) if timewindow else None
    #################### Boundary ######################
    # Payment processing systems already have the boundary terminology we need: "deposit", "transfer", "withdraw"
    #     "deposit"      transactions add money to the public-facing system, crossing the network boundary
    #     "transfer"     transactions move money within the system itself
    #     "withdraw"     transactions remove money from the public-facing system, crossing the network boundary
    # To define the network boundary we must determine what transactions are "deposits", "transfers", and "withdraws"
    #     "none"         by default we assume that we see only "transfers" and thus all transactions are within the system
    #     "transactions" for some systems the transaction records themselves include the category -- the mapping is then a property of the Transaction class
    #     "accounts"     for other systems it is the nature of the account holders that determines the category (ie. accounts are "users", "agents", "tellers", or "atms")
    #                                                                                             -- the mapping is then a property of the Account Holder class
    Transaction.txn_categs       = transaction_categories
    Account_holder.holder_categs = account_categories
    Account_holder.follow_set    = following
    return boundary_type, modifier_func, Transaction, Account_holder

def check_balance(txn,infer_deposit=False):
    # When the balance in an account is insufficient to cover it, we need to do something about it
    # If we are inferring deposit transations we do so now, and if not we assume that the account actually *does* have enough balance we just didn't know it (they carried a starting_balance at the beginning of our data)
    # TODO - add a condition for whether the starting_balance was defined or is being inferred, if it was defined and we still enter this function the program should thrown an accounting exception
    if not txn.src_acct.balance_check(txn):
        balance_missing = txn.amt+txn.rev-txn.src_acct.balance
        if infer_deposit:
            txn.src_acct.infer_deposit(balance_missing)
        else:
            txn.src_acct.infer_balance(balance_missing)

def process(txn,infer_deposit=False):
    # This function processes a transaction! There are three steps:
    #    1) We ensure that the account has enough balance to process the transaction
    #    2) We let the account forget where sufficiently old money came from
    #    3) We deposit, transfer, or withdraw the transaction
    # The "money flows" that steps 2 and 3 may generate are returned
    if txn.categ == 'deposit':
        flows = txn.tgt_acct.stop_tracking(txn.timestamp)
        txn.tgt_acct.deposit(txn)
    elif txn.categ == 'transfer':
        check_balance(txn,infer_deposit)
        flows = txn.src_acct.stop_tracking(txn.timestamp) + txn.tgt_acct.stop_tracking(txn.timestamp)
        txn.src_acct.transfer(txn)
    elif txn.categ == 'withdraw':
        check_balance(txn,infer_deposit)
        flows = txn.src_acct.stop_tracking(txn.timestamp)
        flows = flows + txn.src_acct.withdraw(txn)
    else:
        flows = []
    return flows

def process_remaining_funds(acct,infer_withdraw=False):
    # This function removes all the remaining money from the account, either by inferring a withdraw that brings the balance down to zero or by letting the account forget everything
    moneyflows = []
    if infer_withdraw and acct.balance > acct.resolution_limit:
        moneyflows = acct.stop_tracking(acct.txn_Class.end_timestamp) + acct.infer_withdraw(acct.balance)
    elif acct.tracked > acct.resolution_limit:
        moneyflows = acct.stop_tracking()
    for moneyflow in moneyflows:
        yield moneyflow

def run(input_data,follow_heuristic,moneyflow_file,issues_file,time_cutoff=None,infer_deposits=False,infer_withdraws=False,resolution_limit=0.01):
    import traceback
    import csv
    ###################### DATA! ########################
    boundary_type, modifier_func, Transaction, Account_holder = input_data
    ##################### SYSTEM! #######################
    # The follow heuristic determines which Account Class will populate the system, the length of time we track funds and the smallest ammount we track are properties of the Account class
    Account                      = get_acct_Class(follow_heuristic)
    Account.time_cutoff          = timedelta(hours=float(time_cutoff)) if time_cutoff and time_cutoff!='none' else timedelta.max
    Account.resolution_limit     = resolution_limit
    Account.txn_Class            = Transaction
    ###################### RUN! #########################
    # now we can open the transaction and output files!!
    with open(Transaction.file,'rU') as transaction_file, open(moneyflow_file,'w') as moneyflow_file, open(issues_file,'w') as issues_file:
        txn_reader       = csv.DictReader(transaction_file,Transaction.header,delimiter=",",quotechar="'",escapechar="%")
        moneyflow_writer = csv.writer(moneyflow_file,delimiter=",",quotechar="'")
        issue_writer     = csv.writer(issues_file,delimiter=",",quotechar="'")
        moneyflow_writer.writerow(Flow.header)
        # we use a dictionary to keep track of all the account holders in the system
        accounts = {}
        # now we loop through all the transactions and process them!
        for txn in txn_reader:
            txn = modifier_func(txn) if modifier_func else txn
            try:
                src_acct, tgt_acct = Account_holder.update_accounts(txn,accounts,Account)
                txn = Transaction.create(txn,src_acct,tgt_acct)
                txn.categ = get_txn_categ(txn,boundary_type)
                moneyflows = process(txn,infer_deposits)
                if moneyflows:
                    for moneyflow in moneyflows:
                        moneyflow_writer.writerow(moneyflow.to_print())
            except:
                issue_writer.writerow(["ISSUE W/ PROCESSING",str(txn)]+[traceback.format_exc()])
        # loop through all accounts, and process the remaining funds
        for acct_ID,acct_holder in accounts.items():
            try:
                moneyflows = process_remaining_funds(acct_holder.account,infer_withdraws)
                for moneyflow in moneyflows:
                    moneyflow_writer.writerow(moneyflow.to_print())
            except:
                issue_writer.writerow(["ISSUE W/ REMAINING FUNDS",acct_ID,acct_holder]+[traceback.format_exc()])
            acct_holder.close_out()

if __name__ == '__main__':
    print("Please run main.py, this file keeps the classes and functions.")
