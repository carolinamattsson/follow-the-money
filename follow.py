'''
Follow The Money
This code defines classes and functions for the basic functionaliy
of "follow the money" -- an algorithm to turn a list of transactions into a
list of money flows, representing weighted trajectories of money through a
payment system.
Up-to-date code: https://github.com/Carromattsson/follow_the_money
Copyright (C) 2018 Carolina Mattsson, Northeastern University
'''
from datetime import datetime, timedelta
import traceback
import copy

from initialize import initialize_transactions

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
            flow.extend(self, amt)
        else:
            # "root branches" begin building the flow with the amount given to it
            flow = Flow(self, amt, rev)
        return flow

class Flow:
    # This Class allows us to represent unique trajectories that specific amounts of money follow through the system
    # These "money flows" allow for useful aggregations at the system level where monetary units are never double-counted

    # Class variable defines what flow.to_print() currently outputs
    header = ['flow_timestamp','flow_amt','flow_frac_root','flow_length','flow_length_wrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_durations','flow_rev_fracs','flow_categs']

    def __init__(self, branch, amt, rev):
        # "money flows" have a size (flow.amt), a length within the system (flow.tux), and a duration of time that they remained in the system (flow.duration)
        # the specific trajectory is described by a list of transactions, through a list of accounts, where the money stayed for a list of durations
        # when aggregating over "money flows", they can be weighted by their size or by their root transactions using flow.frac_root
        self.timestamp = datetime.strftime(branch.txn.timestamp,branch.txn.system.timeformat)
        self.txn_IDs   = [branch.txn.txn_ID]
        self.txn_types = [branch.txn.type]
        self.beg_categ = branch.txn.categ
        self.end_categ = branch.txn.categ
        self.acct_IDs  = [branch.txn.src.acct_ID,branch.txn.tgt.acct_ID]
        self.amt       = amt+rev
        self.rev_fracs = [rev/(amt+rev)]
        self.frac_root = (amt+rev)/(branch.txn.amt+branch.txn.rev)
        self.duration  = timedelta(0)
        self.durations = []
        self.length    = 1 if branch.txn.categ == 'transfer' else 0                                              # "Transfers Until eXit" - deposited money begins at step 0, and any subsequent 'transfer' adds 1 to this measure
        self.length_wrev = branch.txn.amt/(branch.txn.amt+branch.txn.rev) if branch.txn.categ == 'transfer' else 0 #                        - strictly speaking, this measure aught to be adjusted by any revenue/fees incurred at each step
    def extend(self, branch, amt):
        # this funciton builds up a "money flow" by incorporating the information in a subsequent "branch"
        # this is called inside the recursive function branch.follow_back(amt)
        self.txn_IDs.append(branch.txn.txn_ID)
        self.acct_IDs.append(branch.txn.tgt.acct_ID)
        self.txn_types.append(branch.txn.type)
        self.end_categ = branch.txn.categ
        self.rev_fracs.append(1-(amt/self.amt))
        branch_duration = branch.txn.timestamp - branch.prev.txn.timestamp
        self.duration += branch_duration
        self.durations.append(branch_duration)
        self.length += 1 if branch.txn.categ == 'transfer' else 0             # neither 'deposit' nor 'withdraw' transactions are included in the "Transfer Until eXit" measure, only transaction within the system itself
        self.length_wrev += amt/self.amt if branch.txn.categ == 'transfer' else 0
    def to_print(self):
        # this returns a version of this class that can be exported to a file using writer.writerow()
        return [self.timestamp,self.amt,self.frac_root,self.length,self.length_wrev,self.duration.total_seconds()/3600.0,\
                '['+','.join(id for id in self.acct_IDs)+']','['+','.join(id for id in self.txn_IDs)+']','['+','.join(type for type in self.txn_types)+']',\
                '['+','.join(str(dur.total_seconds()/3600.0) for dur in self.durations)+']','['+','.join(str(rev_frac) for rev_frac in self.rev_fracs)+']',\
                '('+','.join([self.beg_categ,self.end_categ])+')']

class Tracker(list):
    # Contains the basic features of an account that keeps track of transactions moving through it
    # Accounts always remember their overall balance, and specifically track transactions that entered the account recently
    from initialize import Transaction
    # Class variable defines how Accounts are tracking money, for how long an account will remember where money came from, and down to what amount it will keep track
    type = "no-tracking"
    time_cutoff = None
    resolution_limit = 0.01
    infer = False
    def __init__(self, account):
        # Accounts are initialized to reference:
        self.account = account                    # The Account instance that owns them
        if self.infer and self.account.balance >= self.resolution_limit:
            self.infer_deposit(self.account.balance)
    def add_branches(self, branches):
        # this function adds a list of branches to the account
        self.extend(branches)
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
        if timestamp:
            flows = []
            for branch in self:
                if (timestamp-branch.txn.timestamp)>self.time_cutoff:
                    if branch.amt > self.resolution_limit: flows.append(branch.follow_back(branch.amt))
                    self.remove(branch)
        else:
            flows        = [branch.follow_back(branch.amt) for branch in self]
            self[:]      = []
        return flows
    def deposit(self,this_txn):
        # this function deposits a transaction onto the account
        #    it begins to track a "root branch" that corresponds directly to the deposit transaction
        self.add_branches([Branch(None,this_txn,this_txn.amt)])
    def infer_deposit(self,amt):
        # this function creates an inferred Transaction object and deposits it onto the account
        if amt > self.resolution_limit:
            self.deposit(self.Transaction(self.account,self.account,{"txn_ID":'i',"timestamp":self.system.timewindow[0],"amt":amt,"rev":0,"type":'inferred',"categ":'deposit'}))
    def transfer(self,this_txn):
        # this function processes an outgoing transaction from this account onto a receiving account
        #    it extends the branches in the account by this transaction, and adds these new branches onto the receiving account
        new_branches = self.extend_branches(this_txn)
        this_txn.tgt.tracker.add_branches(new_branches)
    def withdraw(self,this_txn):
        # this function processes a withdraw transaction from this account
        #    it extends the branches in the account by this transaction, builds the "money flows" that leave the system via this transaction, and returns the completed "money flows"
        new_branches = self.extend_branches(this_txn)
        flows = [branch.follow_back(branch.amt) for branch in new_branches]
        return flows
    def infer_withdraw(self,amt):
        # this function creates an inferred Transaction object and withdraws it from the account
        if amt > self.resolution_limit:
            return self.withdraw(self.Transaction(self.account,self.account,{"txn_ID":'i',"timestamp":self.system.timewindow[1],"amt":amt,"rev":0,"type":'inferred',"categ":'withdraw'}))
        else:
            return []
    def pseudo_withdraw(self,txn,amt):
        # this function creates a pseudo-inferred Transaction object and withdraws it from the account
        # useful for when a "deposit" transaction or an uncategorized transaction is actually pulling from tracked funds
        if amt > self.resolution_limit:
            return self.withdraw(self.Transaction(txn.src,txn.tgt,{"txn_ID":txn.txn_ID,"timestamp":txn.timestamp,"amt":amt,"rev":0,"type":txn.type,"categ":'withdraw'}))
        else:
            return []

class Greedy_tracker(Tracker):
    type = "greedy"
    # this account type keeps track of transactions within an account in time order -- a last in first out (LIFO) heuristic
    # intuitively, each account is a stack where incoming money lands on top and outgoing money gets taken off the top
    # specifically, it extends the *most recent* incoming branches by the outgoing transaction up to the value of that transaction
    # this heuristic has the pleasing property of preserving local patterns
    def extend_branches(self,this_txn):
        # according to the LIFO heuristic, the "branches" to be extended are removed from the end of the account
        tracked = sum(branch.amt for branch in self)
        # the tracked balance is adjusted accordingly
        amt = min(this_txn.amt+this_txn.rev,tracked)
        amt_untracked = this_txn.amt+this_txn.rev-tracked if this_txn.amt+this_txn.rev>tracked else None
        branches = []
        while amt > self.resolution_limit:
            # "branches" are removed from the end of the account list until the amount of the transaction is reached
            branch = self[-1]
            if branch.amt <= amt:
                branches.append(self.pop())
                amt = amt - branch.amt
            else:
                # If the last "branch" is larger than the amount to be removed from the account, it is split into two: one remains in this account and the other is extended
                branches.append(Branch(branch.prev,branch.txn,amt))
                branch.decrement(amt)
                amt = 0
        # the removed branches are extended, note that the list is reversed to preserve the newest branches at the end, note also that if any resulting branches are less than the minimum we're tracking, they are not extended
        new_stack = [Branch(branch, this_txn, branch.amt/(1.0+this_txn.rev_ratio)) for branch in reversed(branches) if branch.amt > self.resolution_limit]
        # if the outgoing transaction is larger than the amount being tracked by this account, a new "root branch" is created that corresponds to the transaction itself and the untracked amount (not including the untracked fee/revenue)
        if amt_untracked and amt_untracked > self.resolution_limit: new_stack.append(Branch(None,this_txn,amt_untracked/(1.0+this_txn.rev_ratio)))
        return new_stack

class Well_mixed_tracker(Tracker):
    type = "well-mixed"
    # this account type keeps track of transactions within an account entirely agnostically -- a well-mixed or max-entropy heuristic
    # intuitively, each account is a pool of indistinguishable money
    # specifically, it extends *an equal fraction of all remaining branches* by the outgoing transaction
    # this heuristic takes the perfectly fungible nature of money literally
    def extend_branches(self,this_txn):
        # according to the well-mixed heuristic, all the "branches" in an account are to be extended, and this depreciates their remaining value
        split_factor = this_txn.amt/self.account.balance                               # note that this_txn.rev/self.balance dissappears into the ether...
        stay_factor  = (self.account.balance-this_txn.amt-this_txn.rev)/self.account.balance
        # all the "branches" in an account are extended by the outgoing transaction, note that if any resulting branches are less than the minimum we're tracking, they are not extended
        new_pool     = [Branch(branch, this_txn, split_factor*branch.amt) for branch in self if split_factor*branch.amt >= self.resolution_limit]
        # when there is untracked money also in the account this new_pool will not cover the amount of the transaction - the transaction also sends untracked money!
        # so, a new "root branch" is created with the balance that references this transaction itself begins to re-track this untracked money again - this branch corresponds to the transaction itself and the newly tracked amount
        amt_untracked = this_txn.amt-sum(branch.amt for branch in new_pool)
        if amt_untracked > self.resolution_limit:
            new_pool.append(Branch(None,this_txn,amt_untracked))
        # the old pool is emptied or shrunk to reflect the amount removed
        if stay_factor*sum(branch.amt for branch in self) < self.resolution_limit:
            self[:]      = []
        else:
            for branch in self:
                branch.depreciate(stay_factor)
        return new_pool

def define_tracker(follow_heuristic,time_cutoff,resolution_limit,infer):
    # Based on the follow_heuristic, define the type of trackers we're giving our accounts
    if follow_heuristic == "no-tracking":
        Tracker_class = Tracker
    if follow_heuristic == "greedy":
        Tracker_class = Greedy_tracker
    if follow_heuristic == "well-mixed":
        Tracker_class = Well_mixed_tracker
    # Define also how we handle cutoffs and special cases
    Tracker_class.time_cutoff          = timedelta(hours=float(time_cutoff)) if time_cutoff else None
    Tracker_class.resolution_limit     = resolution_limit
    Tracker_class.infer                = infer
    return Tracker_class

def track_transactions(txns,Tracker,report_file):
    # Track the transaction. There are three steps:
    #                               1) Ensure adequeate balance in the source account
    #                               2) Let the tracker forget sufficiently old money
    #                               3) Deposit, transfer, or withdraw the transaction
    for txn in txns:
        try:
            if txn.categ == 'deposit':
                if not txn.tgt.has_tracker(): txn.tgt.track(Tracker)
                if txn.tgt.tracker.time_cutoff: yield from txn.tgt.tracker.stop_tracking(txn.timestamp)
                if txn.src.tracker: yield from txn.src.tracker.pseudo_withdraw(txn,min(sum(branch.amt for branch in txn.src.tracker),txn.amt))
                txn.tgt.deposit(txn,track=True)
            elif txn.categ == 'transfer':
                if not txn.src.has_tracker(): txn.src.track(Tracker)
                if not txn.tgt.has_tracker(): txn.tgt.track(Tracker)
                txn.src.check_balance(txn.amt+txn.rev)
                if txn.src.tracker.time_cutoff: yield from txn.src.tracker.stop_tracking(txn.timestamp)
                if txn.tgt.tracker.time_cutoff: yield from txn.tgt.tracker.stop_tracking(txn.timestamp)
                txn.src.transfer(txn,track=True)
            elif txn.categ == 'withdraw':
                if not txn.src.has_tracker(): txn.src.track(Tracker)
                txn.src.check_balance(txn.amt+txn.rev)
                if txn.src.tracker.time_cutoff: yield from txn.src.tracker.stop_tracking(txn.timestamp)
                yield from txn.src.withdraw(txn,track=True)
            else:
                if txn.src.tracker: yield from txn.src.tracker.pseudo_withdraw(txn,min(sum(branch.amt for branch in txn.src.tracker),txn.amt))
        except:
            report_file.write("ISSUE W/ PROCESSING: "+str(txn)+"\n"+traceback.format_exc()+"\n")

def track_remaining_funds(system,report_file):
    # This function removes all the remaining money from the system, either by inferring a withdraw that brings the balance down to zero or by letting the account forget everything
    for acct_ID,acct in system.accounts.items():
        try:
            if acct.tracker:
                if acct.tracker.time_cutoff: yield from acct.tracker.stop_tracking(system.timewindow[1])
                if acct.tracker.infer:
                    yield from acct.tracker.infer_withdraw(acct.balance)
                else:
                    yield from acct.tracker.stop_tracking()
        except:
            report_file.write("ISSUE W/ REMAINING FUNDS: "+acct_ID+"\n"+traceback.format_exc()+"\n")
        acct.close_out()

def start_report(report_filename,args):
    import os
    with open(report_filename,'a') as report_file:
        report_file.write("Running 'follow the money' for: "+os.path.abspath(args.input_file)+"\n")
        report_file.write("Using the configuration file: "+os.path.abspath(args.config_file)+"\n")
        report_file.write("Output is going here:"+os.path.join(os.path.abspath(args.output_directory),args.prefix)+"\n")
        report_file.write("Options:"+"\n")
        if args.greedy: report_file.write("    Weighted flows with 'greedy' heuristic saved with extension: wflows_greedy.csv"+"\n")
        if args.well_mixed: report_file.write("    Weighted flows with 'well-mixed' heuristic saved with extension: wflows_well-mixed.csv"+"\n")
        if args.no_tracking: report_file.write("    Weighted flows with 'no-tracking' heuristic saved with extension: wflows_no-tracking.csv"+"\n")
        if args.cutoff: report_file.write("    Stop tracking funds after "+str(args.cutoff)+" hours."+"\n")
        if args.smallest: report_file.write("    Stop tracking funds below "+str(args.smallest)+" in value."+"\n")
        if args.infer: report_file.write("    Record inferred deposits and withdrawals as transactions."+"\n")
        if args.balance: report_file.write("    Before running, infer the starting balance of all accounts."+"\n")
        #if args.read_balance: report_file.write("    Read balance information from columns of the transaction file."+"\n")
        report_file.write("\n")
        report_file.write("\n")

def run(system,transaction_filename,wflow_filename,report_filename,follow_heuristic,cutoff,smallest,infer):
    import traceback
    import csv
    ################# Reset the system ##################
    system = system.reset()
    ############# Define the tracker class ##############
    Tracker = define_tracker(follow_heuristic,cutoff,smallest,infer)
    ###################### RUN! #########################
    with open(transaction_filename,'r') as txn_file, open(wflow_filename,'w') as wflow_file, open(report_filename,'a') as report_file:
        txn_reader  = csv.DictReader(txn_file,system.txn_header,delimiter=",",quotechar='"',escapechar="%")
        wflow_writer = csv.writer(wflow_file,delimiter=",",quotechar='"')
        wflow_writer.writerow(Flow.header)
        # loop through all transactions, and initialize in reference to the system
        transactions = initialize_transactions(txn_reader,system,report_file,get_categ=True)
        # now process according to the defined tracking procedure
        for wflow in track_transactions(transactions,Tracker,report_file):
            wflow_writer.writerow(wflow.to_print())
        # loop through all accounts, and process the remaining funds
        for wflow in track_remaining_funds(system,report_file):
            wflow_writer.writerow(wflow.to_print())
    return system

if __name__ == '__main__':
    print("Please run main.py, this file keeps classes and functions.")
