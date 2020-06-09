'''
Follow The Money
This code defines classes and functions for the basic functionaliy
of "follow the money" -- an algorithm to turn a list of transactions into a
list of money flows, representing weighted trajectories of money through a
payment system.
'''
from datetime import datetime, timedelta
import traceback
import copy

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
    def follow_back(self, amt, fee=None):
        # This is called by the Account class on "leaf branches"
        # This function follows a chain of "branches", beginning with a "leaf branch", and works its way back to the "root branch"
        # On its way up again it builds a "money flow" that represents a unique trajectory that money followed through the system
        #print(self.txn.txn_ID,self.txn.fee_scaling,amt,fee)
        fee = fee if fee else amt*self.txn.fee_scaling
        if self.prev:
            # this is recursive... regular "branches" asks their previous "branch" for its flow, of a given amount, then adds its own
            flow = self.prev.follow_back(amt+fee)
            flow.extend(self, amt, fee)
        else:
            # "root branches" begin building the flow with the amount given to it
            flow = Flow(self, amt, fee)
        return flow

class Flow:
    # This Class allows us to represent unique trajectories that specific amounts of money follow through the system
    # These "money flows" allow for useful aggregations at the system level where monetary units are never double-counted

    # Class variable defines what flow.to_print() currently outputs
    header = ['root_timestamp','root_amt','root_txn','flow_length','flow_length_nrev','flow_duration','flow_acct_IDs','flow_txn_IDs','flow_txn_types','flow_amts','flow_revs','flow_txns','flow_durs','flow_categs']

    def __init__(self, branch, amt, fee):
        # "money flows" have a size (flow.amt), a length within the system (flow.tux), and a duration of time that they remained in the system (flow.duration)
        # the specific trajectory is described by a list of transactions, through a list of accounts, where the money stayed for a list of durations
        # when aggregating over "money flows", they can be weighted by their size or by their transactions using flow.frac_root
        self.timestamp = datetime.strftime(branch.txn.timestamp,"%Y-%m-%d %H:%M:%S")
        self.root_amt  = amt+fee
        self.root_txn  = (amt+fee)/(branch.txn.amt_sent)
        self.amts      = [amt]
        self.revs      = [fee]
        self.txns      = [(amt+fee)/(branch.txn.amt_sent)]
        self.txn_IDs   = [branch.txn.txn_ID]
        self.txn_types = [branch.txn.type]
        self.acct_IDs  = [branch.txn.src.acct_ID,branch.txn.tgt.acct_ID]
        self.beg_categ = branch.txn.categ
        self.end_categ = branch.txn.categ
        self.duration  = timedelta(0)
        self.durations = []
        self.length    = 1 if branch.txn.categ == 'transfer' else 0                                              # "Transfers Until eXit" - deposited money begins at step 0, and any subsequent 'transfer' adds 1 to this measure
        self.length_nrev = branch.txn.amt_rcvd/(branch.txn.amt_sent) if branch.txn.categ == 'transfer' else 0 #                        - strictly speaking, this measure aught to be adjusted by any revenue/fees incurred at each step
    def extend(self, branch, amt, fee):
        # this funciton builds up a "money flow" by incorporating the information in a subsequent "branch"
        # this is called inside the recursive function branch.follow_back(amt)
        self.amts.append(amt)
        self.revs.append(fee)
        self.txns.append((amt+fee)/(branch.txn.amt_sent))
        self.txn_IDs.append(branch.txn.txn_ID)
        self.acct_IDs.append(branch.txn.tgt.acct_ID)
        self.txn_types.append(branch.txn.type)
        self.end_categ = branch.txn.categ
        branch_duration = branch.txn.timestamp - branch.prev.txn.timestamp
        self.duration += branch_duration
        self.durations.append(branch_duration)
        self.length += 1 if branch.txn.categ == 'transfer' else 0             # neither 'deposit' nor 'withdraw' transactions are included in the "Transfer Until eXit" measure, only transaction within the system itself
        self.length_nrev += amt/self.root_amt if branch.txn.categ == 'transfer' else 0
    def to_print(self):
        # this returns a version of this class that can be exported to a file using writer.writerow()
        return [self.timestamp,self.root_amt,self.root_txn,self.length,self.length_nrev,self.duration.total_seconds()/3600.0,\
                '['+','.join(id for id in self.acct_IDs)+']','['+','.join(id for id in self.txn_IDs)+']','['+','.join(type for type in self.txn_types)+']',\
                '['+','.join(str(amt) for amt in self.amts)+']','['+','.join(str(rev) for rev in self.revs)+']','['+','.join(str(txn) for txn in self.txns)+']',\
                '['+','.join(str(dur.total_seconds()/3600.0) for dur in self.durations)+']',\
                '('+','.join([self.beg_categ,self.end_categ])+')']

class Tracker(list):
    # Contains the basic features of an account that keeps track of transactions moving through it
    # Accounts always remember their overall balance, and specifically track transactions that entered the account recently
    from initialize import Transaction
    # Class variable defines how Accounts are tracking money, for how long an account will remember where money came from, and down to what amount it will keep track
    type = "no-tracking"
    time_cutoff = None
    resolution_limit = 0.01
    infer = True
    def __init__(self, account, init):
        # Trackers are initialized to reference:
        self.account = account                    # The Account instance that owns them
        if init:
            self.infer_deposit(account.balance,account.system.time_begin,id="initial")
        else:
            self.infer_deposit(account.balance,account.system.time_current,id="untracked")
    def add_branches(self, branches):
        # this function adds a list of branches to the account
        self.extend(branches)
    def extend_branches(self,this_txn):
        # this function extends the branches in this account by the outgoing transaction, and returns a list of these new branches
        # how the account extends the branches that it's tracking is governed by the tracking heuristic noted in the Account.type
        #    note that if branches are removed from the account in this function, that must be reflected in the tracked balance
        # this "basic" version offers no tracking at all
        #    only one branch is returned, which is a new "root branch" that corresponds directly to the transaction itself
        if this_txn.amt_sent > self.resolution_limit:
            new_branch = Branch(None,this_txn,this_txn.amt_rcvd)
            if this_txn.amt_rcvd > self.resolution_limit:
                new_branches = [new_branch]
                new_flows = []
            else:
                new_branches = []
                new_flows = new_branch.follow_back(new_branch.amt,fee=this_txn.amt_sent-this_txn.amt_rcvd)
        return new_branches, new_flows
    def stop_tracking(self,timestamp):
        # this function makes "leaf branches" out of those in this account older than Account.time_cutoff
        # if builds the "money flows" that thus end at this account <with that duration of time>
        # it returns these "money flows" and stops tracking the corresponding "leaf branches"
        flows = []
        for branch in self:
            if (timestamp-branch.txn.timestamp)>self.time_cutoff:
                flow = branch.follow_back(branch.amt)
                flow.durations.append(self.time_cutoff)
                flow.duration += self.time_cutoff
                flow.end_categ = "savings"
                flows.append(flow)
                self.remove(branch)
        return flows
    def infer_deposit(self,amt,timestamp,id='unknown'):
        if amt > self.resolution_limit:
            # this function creates an inferred Transaction object and deposits it onto the account
            if self.infer:
                inferred_deposit = self.Transaction(self.account,self.account,{"txn_ID":id,"timestamp":timestamp,"amt":amt,"src_fee":0,"tgt_fee":0,"type":'inferred',"categ":'deposit'})
                new_branch = Branch(None,inferred_deposit,inferred_deposit.amt_rcvd)
                self.add_branches([new_branch])
    def infer_withdraw(self,amt,timestamp,fee=0,id='unknown'):
        if amt+fee > self.resolution_limit:
            # this function creates an inferred Transaction object and withdraws it from the account
            inferred_withdraw = self.Transaction(self.account,self.account,{"txn_ID":id,"timestamp":timestamp,"amt":amt,"src_fee":fee,"tgt_fee":0,"type":'inferred',"categ":'withdraw'})
            new_branches, new_flows = self.extend_branches(inferred_withdraw)
            yield from new_flows
            if self.infer:
                yield from [branch.follow_back(branch.amt) for branch in new_branches]
            else:
                yield from [branch.prev.follow_back(branch.amt) for branch in new_branches]
        else:
            yield from []
    def adjust_tracker(self,amt):
        if amt > self.resolution_limit:
            self.infer_deposit(amt,self.account.system.time_current)
            yield from []
        elif amt < -self.resolution_limit:
            yield from self.infer_withdraw(-amt,self.account.system.time_current)

    @classmethod
    def process(cls,txn,src_track=True,tgt_track=True):
        if txn.amt_rcvd < 0:
            # correct for this eventuality... it means over 100% of the transaction went towards the fee
            if tgt_track:
                yield from txn.tgt.tracker.infer_withdraw(0,system.time_current,fee=-txn.amt_rcvd,id='fee')
            txn.tgt.balance = txn.tgt.balance+txn.amt_rcvd
            txn.fee_scaling = 1
            txn.amt_rcvd = 0
        if txn.amt_sent > cls.resolution_limit:
            if src_track:
                new_branches, new_flows = txn.src.tracker.extend_branches(txn)
                yield from new_flows
            else:
                if txn.src.has_tracker():
                    new_branches, new_flows = txn.src.tracker.extend_branches(txn)
                    yield from new_flows
                    yield from [branch.follow_back(branch.amt) for branch in new_branches]
                if tgt_track:
                    new_branch = Branch(None,txn,txn.amt_rcvd)
                    if txn.amt_rcvd > cls.resolution_limit:
                        new_branches = [new_branch]
                    else:
                        new_branches = []
                        yield from [new_branch.follow_back(new_branch.amt,fee=txn.amt_sent-txn.amt_rcvd)]
                else:
                    new_branches = []
            if tgt_track:
                txn.tgt.tracker.add_branches(new_branches)
            else:
                yield from [branch.follow_back(branch.amt) for branch in new_branches]

class Greedy_tracker(Tracker):
    type = "greedy"
    # this account type keeps track of transactions within an account in time order -- a last in first out (LIFO) heuristic
    # intuitively, each account is a stack where incoming money lands on top and outgoing money gets taken off the top
    # specifically, it extends the *most recent* incoming branches by the outgoing transaction up to the value of that transaction
    # this heuristic has the pleasing property of preserving local patterns
    def extend_branches(self,this_txn):
        # according to the LIFO heuristic, the "branches" to be extended are removed from the end of the account
        tracked = sum(branch.amt for branch in self)
        amt = min(this_txn.amt_sent,tracked)
        branches = []
        while amt > self.resolution_limit:
            # "branches" are removed from the end of the account list until the amount of the transaction is reached
            branch = self[-1]
            if branch.amt < amt+self.resolution_limit:
                branches.append(self.pop())
                amt = amt - branch.amt
            else:
                # If the last "branch" is larger than the amount to be removed from the account (by more than the resolution_limit), it is split into two: one remains in this account and the other is extended
                branches.append(Branch(branch.prev,branch.txn,amt))
                branch.decrement(amt)
                amt = 0
        # the removed branches are extended
            # note that the list is reversed to preserve the newest branches at the end
            # note that if any resulting branches are less than the minimum we're tracking, they are not extended and instead followed back
        new_stack, new_flows = [], []
        continues = this_txn.amt_rcvd/this_txn.amt_sent
        for branch in reversed(branches):
            new_branch = Branch(branch,this_txn,branch.amt*continues)
            if new_branch.amt > self.resolution_limit:
                new_stack.append(new_branch)
            else:
                new_flows.append(new_branch.follow_back(new_branch.amt,fee=branch.amt-new_branch.amt))
        # if the outgoing transaction is larger than the amount being tracked, a new "root branch" is created that corresponds to the transaction itself and the untracked amount
            # note that the fee/revenue for this portion is accounted for later, within follow_back, using the fee information in the transaction itself
            # note that if the resulting branch is less than the minimum we're tracking but only because of the fee, it is instead followed back
        amt_untracked = this_txn.amt_rcvd - sum(branch.amt for branch in new_stack)
        if amt_untracked > self.resolution_limit:
            new_stack.append(Branch(None,this_txn,amt_untracked))
        else:
            tot_untracked = this_txn.amt_sent-tracked
            if tot_untracked > self.resolution_limit:
                new_branch = Branch(None,this_txn,tot_untracked*continues)
                new_flows.append(new_branch.follow_back(new_branch.amt,fee=tot_untracked-new_branch.amt))
        return new_stack, new_flows

class Well_mixed_tracker(Tracker):
    type = "well-mixed"
    # this account type keeps track of transactions within an account entirely agnostically -- a well-mixed or max-entropy heuristic
    # intuitively, each account is a pool of indistinguishable money
    # specifically, it extends *an equal fraction of all remaining branches* by the outgoing transaction
    # this heuristic takes the perfectly fungible nature of money literally
    def extend_branches(self,this_txn):
        # according to the well-mixed heuristic, all the "branches" in an account are to be extended, and this depreciates their remaining value
        track_factor = this_txn.amt_sent/self.account.balance
        split_factor = this_txn.amt_rcvd/self.account.balance
        stay_factor  = (self.account.balance-this_txn.amt_sent)/self.account.balance
        new_pool, new_flows = [], []
        # all the "branches" in an account are extended by the outgoing transaction
            # note that if any resulting branches are less than the minimum we're tracking, they are not transferred to the target account
            # if this is because of fees, the branch is immediately followed back instead
        for branch in self:
            if track_factor*branch.amt > self.resolution_limit:
                new_branch = Branch(branch,this_txn,split_factor*branch.amt)
                if new_branch.amt > self.resolution_limit:
                    new_pool.append(new_branch)
                else:
                    new_flows.append(new_branch.follow_back(new_branch.amt,fee=track_factor*branch.amt-new_branch.amt))
        # when there is untracked money also in the account this new_pool will not cover the amount of the transaction - the transaction also sends untracked money!
        # so, a new "root branch" is created with the balance that references this transaction itself begins to re-track this untracked money again - this branch corresponds to the transaction itself and the newly tracked amount
            # note that the fee/revenue for this portion is accounted for later, within follow_back, using the fee information in the transaction itself
            # note that if the resulting branch is less than the minimum we're tracking but only because of the fee, it is instead followed back
        amt_untracked = this_txn.amt_rcvd  - sum(branch.amt for branch in new_pool)
        if amt_untracked > self.resolution_limit:
            new_pool.append(Branch(None,this_txn,amt_untracked))
        else:
            tot_untracked = this_txn.amt_sent - sum(branch.amt for branch in self)
            if tot_untracked > self.resolution_limit:
                new_branch = Branch(None,this_txn,tot_untracked*(this_txn.amt_rcvd/this_txn.amt_sent))
                new_flows.append(new_branch.follow_back(new_branch.amt,fee=tot_untracked-new_branch.amt))
        # the old pool is emptied or shrunk to reflect the amount removed
            # note that if any resulting are less than the minimum we're tracking, they are removed and followed back
        for branch in self:
            if stay_factor*branch.amt < self.resolution_limit:
                self.remove(branch)
            else:
                branch.depreciate(stay_factor)
        return new_pool, new_flows

def define_tracker(follow_heuristic,time_cutoff,resolution_limit,no_infer):
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
    Tracker_class.infer                = not no_infer
    return Tracker_class

def check_timestamp(acct,timestamp):
    if acct.tracker: yield from acct.tracker.stop_tracking(timestamp)

def check_balances(txn,report_file):
    # retrieve pre-transaction account balances
    src_init, tgt_init = txn.system.known_balances(txn)
    if src_init is None: src_init = txn.src.balance
    if tgt_init is None: tgt_init = txn.tgt.balance
    # compute balances required by basic accounting
    src_need, tgt_need = max(src_init,txn.amt_sent), max(tgt_init,-txn.amt_rcvd)
    # adjust if necessary, and report accounting discrepancies
    for acct, acct_need in [(txn.src,src_need), (txn.tgt,tgt_need)]:
        if acct.tracked and acct_need != acct.balance:
            discrepancy = acct_need - acct.balance
            yield from acct.adjust_balance(discrepancy)
            abs_discrepancy = -1*discrepancy if discrepancy < 0 else discrepancy
            if abs_discrepancy > acct.tracker.resolution_limit:
                report_file.write("WARNING: BALANCE ADJUSTED for "+acct.acct_ID+" at "+txn.txn_ID+" BY "+str(discrepancy)+"\n")

def check_initialized(txn,Tracker_class,inconsistents):
    # check source account
    if txn.src.tracked is None: # first time we're seeing source account
        if txn.categ in ['transfer','withdraw']:
            txn.src.track(Tracker_class,init=True)
        else:
            txn.src.tracked = False
    elif txn.src.tracked is False: # not first time, previously untracked
        if txn.categ in ['transfer','withdraw']: # inconsistent boundary
            txn.src.track(Tracker_class,init=False)
            inconsistents.add(txn.src.acct_ID)
    elif txn.src.tracked is True:  # not first time, previously tracked
        if txn.categ not in ['transfer','withdraw']: # inconsistent boundary
            inconsistents.add(txn.src.acct_ID)
    # check target account
    if txn.tgt.tracked is None: # first time we're seeing target account
        if txn.categ in ['deposit','transfer']:
            txn.tgt.track(Tracker_class,init=True)
        else:
            txn.tgt.tracked = False
    elif txn.tgt.tracked is False: # not first time, previously untracked
        if txn.categ in ['deposit','transfer']: # inconsistent boundary
            txn.tgt.track(Tracker_class,init=False)
            inconsistents.add(txn.tgt.acct_ID)
    elif txn.tgt.tracked is True:  # not first time, previously tracked
        if txn.categ not in ['deposit','transfer']: # inconsistent boundary
            inconsistents.add(txn.tgt.acct_ID)
    return inconsistents

def track_transactions(system,txns,Tracker,report_file,untracked_file,inconsistents_file):
    # Track the transaction. There are three steps:
    #                               1) Check the accounts for old money, tracking consistency
    #                               2) Check for the prior existence of required balance
    #                               3) Deposit, transfer, or withdraw the transaction
    inconsistents = set()
    untracked_file.write("UNTRACKED TRANSACTIONS:\n")
    for txn in txns:
        try:
            if Tracker.time_cutoff:
                yield from check_timestamp(txn.src,system.time_current)
                yield from check_timestamp(txn.tgt,system.time_current)
            inconsistents = check_initialized(txn,Tracker,inconsistents)
            yield from check_balances(txn,report_file)
        except:
            report_file.write("FAILED: PRE-CHECKING: "+str(txn)+"\n"+traceback.format_exc()+"\n")
            report_file.flush()
        try:
            if txn.categ == 'deposit':
                yield from Tracker.process(txn,src_track=False,tgt_track=True) if Tracker else []
            elif txn.categ == 'transfer':
                yield from Tracker.process(txn,src_track=True,tgt_track=True) if Tracker else []
            elif txn.categ == 'withdraw':
                yield from Tracker.process(txn,src_track=True,tgt_track=False) if Tracker else []
            else:
                untracked_file.write(txn.txn_ID+"\n")
                yield from Tracker.process(txn,src_track=False,tgt_track=False) if Tracker else []
        except:
            report_file.write("FAILED: PROCESSING: "+str(txn)+"\n"+traceback.format_exc()+"\n")
            report_file.flush()
        txn.system.process(txn)
    if inconsistents:
        for account in inconsistents:
            inconsistents_file.write(account+"\n")
        inconsistents_file.flush()

def track_remaining_funds(system,report_file):
    # This function removes all the remaining money from the system, either by inferring a withdraw that brings the balance down to zero or by letting the account forget everything
    for acct_ID,acct in system.accounts.items():
        try:
            if acct.has_tracker():
                if acct.tracker.time_cutoff: yield from acct.tracker.stop_tracking(acct.system.time_end)
                yield from acct.tracker.infer_withdraw(acct.balance,system.time_end,id="final")
        except:
            report_file.write("FAILED: REMAINING FUNDS: "+acct_ID+"\n"+traceback.format_exc()+"\n")
        acct.close_out()

def update_report(report_filename,args):
    import os
    with open(report_filename,'a') as report_file:
        report_file.write("\n")
        report_file.write("Output is going here:"+os.path.join(os.path.abspath(args.output_directory),args.prefix)+"\n")
        report_file.write("Tracking options:"+"\n")
        if args.no_infer: report_file.write("    Avoid inferring unseen deposit and withdrawal transactions."+"\n")
        if args.cutoff: report_file.write("    Stop tracking funds after "+str(args.cutoff)+" hours."+"\n")
        if args.smallest: report_file.write("    Stop tracking funds below "+str(args.smallest)+" in value."+"\n")
        report_file.write("Running:"+"\n")
        if args.greedy: report_file.write("    Weighted flows with 'greedy' heuristic saved with extension: wflows_greedy.csv"+"\n")
        if args.well_mixed: report_file.write("    Weighted flows with 'well-mixed' heuristic saved with extension: wflows_well-mixed.csv"+"\n")
        if args.no_tracking: report_file.write("    Weighted flows with 'no-tracking' heuristic saved with extension: wflows_no-tracking.csv"+"\n")
        report_file.write("\n\n")
        report_file.flush()


def run(system,transaction_filename,wflow_filename,report_filename,follow_heuristic,cutoff,smallest,no_infer):
    from initialize import timewindow_transactions
    from initialize import initialize_transactions
    import csv
    ################# Reset the system ##################
    system = system.reset()
    ############# Define the tracker class ##############
    Tracker = define_tracker(follow_heuristic,cutoff,smallest,no_infer)
    ############## Redefine report files ################
    untracked = report_filename.split(".txt")[0]+".untracked"
    inconsistents = report_filename.split(".txt")[0]+".inconsistents"
    ###################### RUN! #########################
    with open(transaction_filename,'r') as txn_file, open(wflow_filename,'w') as wflow_file, \
         open(report_filename,'a') as report_file, open(inconsistents,'a') as inconsistents_file, open(untracked,'a') as untracked_file:
        transactions = csv.DictReader(txn_file,system.txn_header,delimiter=",",quotechar='"',escapechar="%")
        wflow_writer = csv.writer(wflow_file,delimiter=",",quotechar='"')
        wflow_writer.writerow(Flow.header)
        # loop through all transactions, and initialize in reference to the system
        transactions = timewindow_transactions(transactions,system,report_file,get_categ=True)
        transactions = initialize_transactions(transactions,system,report_file,get_categ=True)
        # now process according to the defined tracking procedure
        for wflow in track_transactions(system,transactions,Tracker,report_file,untracked_file,inconsistents_file):
            wflow_writer.writerow(wflow.to_print())
        # loop through all accounts, and process the remaining funds
        for wflow in track_remaining_funds(system,report_file):
            wflow_writer.writerow(wflow.to_print())
    return system

if __name__ == '__main__':
    print("Please run main.py, this file keeps classes and functions.")
