'''
Follow The Money
This code defines classes and functions for the basic functionaliy
of "follow the money" -- an algorithm to turn a list of transactions into a
list of money flows, representing weighted trajectories of money through a
payment system.
'''
from datetime import datetime, timedelta
import random
import traceback
import copy

class Branch:
    # this class allows for chaining together transactions, or parts of those transactions
    def __init__(self, prev_branch, current_txn, amt_sent):
        # "branches" reference the transaction they are a part of (branch.txn), and how much of that transaction they represent (branch.amt)
        # "root branches" are special in that their branch.prev references None - deposits are necessarily "root branches"
        # "leaf branches" are special in that they are treated differently by the Account class - withdraws are necessarily "leaf branches"
        # subsequent transactions build a "tree" of regular "branches" that reference back to the "root branch" using branch.prev
        self.prev = prev_branch
        self.txn  = current_txn
        self.amt  = amt_sent*self.txn.continues
    def decrement(self, amt):
        if amt > self.amt:
            raise ValueError('Accounting exception -- decrement branch by more than amt')
        self.amt  = self.amt - amt
    def depreciate(self, factor):
        if factor > 1 or factor < 0:
            raise ValueError('Accounting exception -- depreciate branch by impossible factor')
        self.amt  = factor * self.amt
    def txn_timestamp(self):
        # timestamp of the transaction this branch is a part of
        return self.txn.timestamp
    def root_timestamp(self):
        # timestamp of the root transaction this branch points back to
        if self.prev:
            timestamp = self.prev.root_timestamp()
        else:
            timestamp = self.txn.timestamp
        return timestamp
    def follow_back(self, amt):
        # This is called by the Account class on "leaf branches"
        # This function follows a chain of "branches", beginning with a "leaf branch", and works its way back to the "root branch"
        # On its way up again it builds a "money flow" that represents a unique trajectory that money followed through the system
        #print(self.txn.txn_ID,self.txn.fee_scaling,amt,fee)
        fee = amt*self.txn.fee_scaling
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
    header = ['trj_timestamp','trj_amt','trj_txn','trj_categ','trj_len','trj_dur','txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']
    def __init__(self, branch, amt, fee):
        # "money flows" have a size (flow.amt), a length within the system (flow.tux), and a duration of time that they remained in the system (flow.duration)
        # the specific trajectory is described by a list of transactions, through a list of accounts, where the money stayed for a list of durations
        # when aggregating over "money flows", they can be weighted by their size or by their transactions using flow.frac_root
        self.timestamp = branch.txn.timestamp
        self.root_amt  = amt+fee
        self.root_txn  = (amt+fee)/(branch.txn.amt_sent)
        self.amts      = [amt]
        self.revs      = [fee]
        self.txns      = [(amt+fee)/(branch.txn.amt_sent)]
        self.txn_IDs   = [branch.txn.txn_ID]
        self.txn_types = [branch.txn.type]
        self.acct_IDs  = [branch.txn.src.acct_ID if branch.txn.src is not None else branch.txn.src_ID,branch.txn.tgt.acct_ID if branch.txn.tgt is not None else branch.txn.tgt_ID]
        self.beg_categ = branch.txn.categ if branch.txn.categ == 'deposit' else 'untracked'
        self.end_categ = branch.txn.categ
        self.duration  = None
        self.durations = []
        self.length    = 1 if branch.txn.categ == 'transfer' else 0
    def extend(self, branch, amt, fee):
        # this funciton builds up a "money flow" by incorporating the information in a subsequent "branch"
        # this is called inside the recursive function branch.follow_back(amt)
        self.amts.append(amt)
        self.revs.append(fee)
        self.txns.append((amt+fee)/(branch.txn.amt_sent))
        self.txn_IDs.append(branch.txn.txn_ID)
        self.acct_IDs.append(branch.txn.tgt.acct_ID if branch.txn.tgt is not None else branch.txn.tgt_ID)
        self.txn_types.append(branch.txn.type)
        self.end_categ = branch.txn.categ
        branch_duration = branch.txn.timestamp - branch.prev.txn.timestamp
        self.duration = self.duration + branch_duration if self.duration is not None else branch_duration
        self.durations.append(branch_duration)
        self.length += 1 if branch.txn.categ == 'transfer' else 0
    def cut(self, duration, total):
        if total:
            tot_duration = duration
            try:
                rem_duration = [tot_duration - self.duration]
            except Exception:
                rem_duration = []
        else:
            rem_duration = [duration]
            try:
                tot_duration = rem_duration + self.duration
            except Exception:
                tot_duration = None
        self.end_categ = "untracked"
        self.duration = tot_duration
        self.durations = self.durations + rem_duration
        return self
    def to_print(self,timeformat,digits):
        # this returns a version of this class that can be exported to a file using writer.writerow()
        try:
            self.duration = str(round(self.duration.total_seconds()/3600.0,digits))
            self.durations = [str(round(dur.total_seconds()/3600.0,digits)) for dur in self.durations]
        except Exception:
            pass
        finally:
            return [datetime.strftime(self.timestamp,timeformat),\
                    round(self.root_amt,digits),\
                    round(self.root_txn,digits),\
                    '('+','.join([self.beg_categ,self.end_categ])+')',\
                    self.length,\
                    self.duration,\
                    '['+','.join(id for id in self.txn_IDs)+']',\
                    '['+','.join(type for type in self.txn_types)+']',\
                    '['+','.join(str(round(amt,digits)) for amt in self.amts)+']',\
                    '['+','.join(str(round(rev,digits)) for rev in self.revs)+']',\
                    '['+','.join(str(round(txn,digits)) for txn in self.txns)+']',\
                    '['+','.join(id for id in self.acct_IDs)+']',\
                    '['+','.join(self.durations)+']']

class Tracker(list):
    # Contains the basic features of an account that keeps track of transactions moving through it
    # Accounts always remember their overall balance, and specifically track transactions that entered the account recently
    from initialize import Transaction
    # Class variable defines how Accounts are tracking money, for how long an account will remember where money came from, and down to what amount it will keep track
    type = "none"
    size_limit = None
    hr_cutoff = None
    absolute = True
    def __init__(self, account, init):
        # Trackers are initialized to reference the Account instance that owns them
        self.account = account
    def add_branches(self, branches):
        # this function adds a list of branches to the account
        self.extend(branches)
    def extend_branches(self,this_txn):
        # this function extends the branches in this account by the outgoing transaction, and returns a list of these new branches
        # how the account extends the branches that it's tracking is governed by the tracking heuristic noted in the Account.type
        # this "basic" version offers no tracking at all
        #    only one branch is returned, which is a new "root branch" that corresponds directly to the transaction itself
        #    any branches from before are returned untracked
        untracked = []
        for branch in self:
            untracked.append(branch)
            self.remove(branch)
        extended = [Branch(None,this_txn,this_txn.amt_sent)]
        return extended, untracked
    def untrack_branches(self,this_txn):
        # funds allocated to a trasaction going to an untracked account
        new_branches, untracked_branches = self.extend_branches(this_txn)
        untracked_branches = untracked_branches + [branch.prev for branch in new_branches if branch.prev is not None]
        return untracked_branches
    def txn_timestamp(self,branch):
        return branch.txn_timestamp()
    def root_timestamp(self,branch):
        return branch.root_timestamp()
    def overstayed_branches(self,timestamp):
        # removes and returns the overstayed branches
        leaf_branches = []
        for branch in self:
            duration = timestamp - self.prev_timestamp(branch)
            if duration > self.hr_cutoff:
                leaf_branches.append(branch)
                self.remove(branch)
        return leaf_branches
    @classmethod
    def start_tracking(cls,this_txn,amt_sent):
        if amt_sent >= cls.size_limit:
            new_branch = Branch(None,this_txn,amt_sent)
            return [new_branch]
        else:
            return []
    @classmethod
    def stop_tracking(cls,leaf_branches,complete=True,timestamp=None,duration=None,total=True): # not sure what this default should be
        for branch in leaf_branches:
            flow = branch.follow_back(branch.amt)
            if complete:
                yield flow
            else:
                if timestamp is not None:
                    duration = timestamp - flow.timestamp
                    total = True
                yield flow.cut(duration,total)
    @classmethod
    def check_cutoffs(cls,acct,timestamp):
        leaf_branches = acct.tracker.overstayed_branches(timestamp)
        yield from cls.stop_tracking(leaf_branches,complete=False,duration=acct.tracker.hr_cutoff,total=acct.tracker.absolute)
    @classmethod
    def process(cls,txn,src_track=True,tgt_track=True):
        if txn.amt_rcvd < 0:
            raise ValueError('Accounting exception -- negative amount recieved')
        if txn.amt_sent > cls.float_zero:
            if src_track:
                new_branches, untracked_branches = txn.src.tracker.extend_branches(txn)
                yield from cls.stop_tracking(untracked_branches,complete=False,timestamp=txn.timestamp)
            else:
                if txn.src is not None and txn.src.has_tracker():
                    untracked_branches = txn.src.tracker.untrack_branches(txn)
                    yield from cls.stop_tracking(untracked_branches,complete=False,timestamp=txn.timestamp)
                if tgt_track:
                    new_branches = cls.start_tracking(txn,txn.amt_sent)
                else:
                    new_branches = []
            if tgt_track:
                txn.tgt.tracker.add_branches(new_branches)
            else:
                yield from cls.stop_tracking(new_branches)

class LIFO_tracker(Tracker):
    type = "lifo"
    # This  Tracker type allocates funds within an account in time order, using a last in first out (LIFO) heuristic
    # intuitively, each account is a stack where incoming money lands on top and outgoing money gets taken off the top
    # specifically, it extends the *most recent* incoming branches by the outgoing transaction up to the value of that transaction
    # this heuristic has the pleasing property of preserving local patterns
    def extend_branches(self,this_txn):
        amt = this_txn.amt_sent
        amt_tracked = sum(branch.amt for branch in self)
        untracked = []
        # If we're tracking less than we send, the whole list is allocated & extended
        if amt_tracked < amt+self.float_zero:
            extended = [Branch(branch,this_txn,branch.amt) for branch in self]
            self.clear()
        # Otherwise we loop through to get what's allocated & extended
        else:
            extended = []
            # Branches are removed from the end of the account list until the amount of the transaction is reached
            while amt >= self.size_limit:
                # Start with just looking at the branch
                branch = self[-1]
                # If it fits, the whole Branch is allocated & extended
                if branch.amt < amt+self.float_zero:
                    branch = self.pop()
                    extended.append(Branch(branch,this_txn,branch.amt))
                    amt = amt - branch.amt
                # Otherwise, only a part of the Branch is extended
                else:
                    extended.append(Branch(branch,this_txn,amt))
                    branch.decrement(amt)
                    amt = 0
                    # If the remaining branch got too small, untrack it
                    if branch.amt < self.size_limit:
                        branch = self.pop()
                        if branch.amt >= self.float_zero:
                            untracked.append(branch)
        # Fill in with a new branch if we aren't tracking enough
        amt_untracked = this_txn.amt_sent-sum(branch.amt for branch in extended)
        new_branch = self.start_tracking(this_txn,amt_untracked)
        extended = extended + new_branch
        # reverse the list to preserve the newest branches at the end
        return reversed(extended), untracked

class Mixed_tracker(Tracker):
    type = "mixed"
    # This Tracker type allocates funds within an account entirely agnostically, using a mixed or max-entropy heuristic
    # intuitively, each account is a pool of indistinguishable funds (taking the perfectly fungible nature of money literally)
    # specifically, it extends *an equal fraction of all existing branches* by the outgoing transaction
    def extend_branches(self,this_txn):
        # The Tracker will allocate from all Branches an amount proportional to this Transactions' fraction of the total balance
        send_factor = this_txn.amt_sent/self.account.balance
        to_remove = []
        untracked = []
        extended = []
        for branch in self:
            # All the "branches" in an account are extended by the outgoing transaction
            amt = send_factor*branch.amt
            if amt >= self.size_limit:
                extended.append(Branch(branch,this_txn,amt))
            # unless it would go below the threshold
            elif amt >= self.float_zero:
                untracked.append(Branch(branch.prev,branch.prev.txn,amt))
            else:
                print(0) # if this shows up we're creating/keeping poorly
            # All the "branches" in an account are decremented to suit
            branch.decrement(amt)
            # If the remaining branch got too small, it goes on the chopping block
            if branch.amt < self.size_limit:
                to_remove.append(branch)
        # Remove the tiny branches, and if not so tiny also record it
        for branch in to_remove:
            self.remove(branch)
            if branch.amt >= self.float_zero:
                untracked.append(branch)
        # Fill in with a new branch if we aren't tracking enough
        amt_untracked = this_txn.amt_sent-sum(branch.amt for branch in extended)
        new_branch = self.start_tracking(this_txn,amt_untracked)
        extended = extended + new_branch
        return extended, untracked

def define_tracker(follow_heuristic,hr_cutoff,absolute,size_limit,rounding):
    # Based on the follow_heuristic, define the type of trackers we're giving our accounts
    if follow_heuristic == "none":
        Tracker_class = Tracker
    if follow_heuristic == "lifo":
        Tracker_class = LIFO_tracker
    if follow_heuristic == "mixed":
        Tracker_class = Mixed_tracker
    # Define also how we handle cutoff,absolutes and special cases
    Tracker_class.hr_cutoff = timedelta(hours=float(hr_cutoff)) if hr_cutoff else timedelta(days=999999999, seconds=86399, microseconds=999999)
    Tracker_class.prev_timestamp = Tracker_class.root_timestamp if absolute else Tracker_class.txn_timestamp
    Tracker_class.absolute = absolute
    Tracker_class.size_limit  = size_limit
    Tracker_class.float_zero  = 5*10**(-(rounding+1))
    return Tracker_class

def check_balances(txn,inferred_file):
    # retrieve pre-transaction account balances
    src_init, tgt_init = txn.system.known_balances(txn)
    if src_init is None: src_init = txn.src.balance
    if tgt_init is None: tgt_init = txn.tgt.balance
    # compute balances required by basic accounting
    src_need, tgt_need = max(src_init,txn.amt_sent), max(tgt_init,-txn.amt_rcvd)
    # adjust if necessary, and infer if that is
    for acct, acct_need in [(txn.src,src_need), (txn.tgt,tgt_need)]:
        if inferred_file and acct.has_tracker():
            if acct_need > acct.balance:
                yield from infer_deposit(acct,acct_need-acct.balance,"accounting",inferred_file)
            if acct_need < acct.balance:
                yield from infer_withdraw(acct,acct.balance-acct_need,"accounting",inferred_file)
        acct.balance = acct_need

def infer_deposit(acct,amt,type,inferred_file):
    # infer a deposit transaction of the given type, give it a 12-digit hash
    from initialize import Transaction
    if amt and amt >= acct.tracker.size_limit:
        timestamp = acct.system.time_begin-timedelta(milliseconds=0.001) if type == 'initial' else acct.system.time_current
        inferred_txn = Transaction.create(None,acct,{'txn_ID':'i_%x' % random.getrandbits(48),
                                                     'src_ID':"inferred",
                                                     'tgt_ID':acct.acct_ID,
                                                     'timestamp':timestamp,
                                                     'amt':amt,
                                                     'src_fee':0,
                                                     'tgt_fee':0,
                                                     'type':type,
                                                     'categ':"deposit"},get_categ=False)
        yield from acct.tracker.process(inferred_txn,src_track=False,tgt_track=True)
        inferred_file.write(str(inferred_txn)+"\n")
        inferred_file.flush()

def infer_withdraw(acct,amt,type,inferred_file):
    # infer a withdrawal transaction of the given type, give it a 12-digit hash
    from initialize import Transaction
    if amt >= acct.tracker.size_limit:
        timestamp = acct.system.time_end if type == 'final' else acct.system.time_current
        inferred_txn = Transaction.create(acct,None,{'txn_ID':'i_%x' % random.getrandbits(48), # 12-digit hash
                                                     'src_ID':acct.acct_ID,
                                                     'tgt_ID':"inferred",
                                                     'timestamp':timestamp,
                                                     'amt':amt,
                                                     'src_fee':0,
                                                     'tgt_fee':0,
                                                     'type':type,
                                                     'categ':"withdraw"},get_categ=False)
        yield from acct.tracker.process(inferred_txn,src_track=True,tgt_track=False)
        inferred_file.write(str(inferred_txn)+"\n")
        inferred_file.flush()

def check_initialized(txn,Tracker_class,inferred_file):
    # check source account
    if txn.src.tracked is None: # first time we're seeing source account
        if txn.categ in ['transfer','withdraw']:
            txn.src.track(Tracker_class)
            if inferred_file: yield from infer_deposit(txn.src,txn.src.starting_balance,"initial",inferred_file)
        else:
            txn.src.tracked = False
    elif txn.src.tracked is False: # not first time, previously untracked
        if txn.categ in ['transfer','withdraw']: # inconsistent boundary
            txn.src.track(Tracker_class)
    elif txn.src.tracked is True:  # not first time, previously tracked
        if txn.categ not in ['transfer','withdraw']: # inconsistent boundary
            pass
    # check target account
    if txn.tgt.tracked is None: # first time we're seeing target account
        if txn.categ in ['deposit','transfer']:
            txn.tgt.track(Tracker_class)
            if inferred_file: yield from infer_deposit(txn.tgt,txn.tgt.starting_balance,"initial",inferred_file)
        else:
            txn.tgt.tracked = False
    elif txn.tgt.tracked is False: # not first time, previously untracked
        if txn.categ in ['deposit','transfer']: # inconsistent boundary
            txn.tgt.track(Tracker_class)
    elif txn.tgt.tracked is True:  # not first time, previously tracked
        if txn.categ not in ['deposit','transfer']: # inconsistent boundary
            pass

def track_transactions(system,txns,Tracker,report_file,untracked_file,inferred_file):
    # Track the transaction.
    for txn in txns:
        system.time_current = txn.timestamp
        try:
            yield from check_initialized(txn,Tracker,inferred_file)
            yield from check_balances(txn,inferred_file)
            for acct in [txn.src,txn.tgt]:
                if acct.has_tracker():
                    yield from Tracker.check_cutoffs(acct,txn.timestamp)
        except Exception:
            report_file.write("FAILED: CHECKING: "+str(txn)+"\n"+traceback.format_exc()+"\n")
            report_file.flush()
        try:
            if txn.categ == 'deposit':
                yield from Tracker.process(txn,src_track=False,tgt_track=True) if Tracker else []
            elif txn.categ == 'transfer':
                yield from Tracker.process(txn,src_track=True,tgt_track=True) if Tracker else []
            elif txn.categ == 'withdraw':
                yield from Tracker.process(txn,src_track=True,tgt_track=False) if Tracker else []
            else:
                yield from Tracker.process(txn,src_track=False,tgt_track=False) if Tracker else []
                untracked_file.write(str(txn)+"\n")
                untracked_file.flush()
        except Exception:
            report_file.write("FAILED: PROCESSING: "+str(txn)+"\n"+traceback.format_exc()+"\n")
            report_file.flush()
        txn.system.process(txn)

def track_remaining_funds(system,Tracker,report_file,inferred_file):
    # This function removes all the remaining money from the system, either by inferring a withdraw that brings the balance down to zero or by letting the account forget everything
    for acct_ID, acct in system.accounts.items():
        try:
            if acct.has_tracker():
                yield from Tracker.check_cutoffs(acct,system.time_end)
                if inferred_file and (system.boundary_type=="transactions" or acct.categ in system.categ_follow):
                    yield from infer_withdraw(acct,acct.balance,"final",inferred_file)
                else:
                    yield from Tracker.stop_tracking(acct.tracker,complete=False,timestamp=system.time_end)
                    acct.tracker.clear()
        except Exception:
            report_file.write("FAILED: REMAINING FUNDS: "+acct_ID+"\n"+traceback.format_exc()+"\n")
            report_file.flush()
        acct.close_out()

def update_report(report_filename,args,heuristic=None):
    import os
    with open(report_filename,'a') as report_file:
        if heuristic is None:
            report_file.write("\n")
            report_file.write("Output is going here:"+os.path.join(os.path.abspath(args.output_directory),args.prefix)+"\n")
            report_file.write("Tracking options:"+"\n")
            report_file.write("    Untracked transaction recorded with extension: untracked.csv"+"\n")
            if args.no_infer:
                report_file.write("    Avoid inferring unseen deposit and withdrawal transactions."+"\n")
            else:
                report_file.write("    Inferred transaction recorded with extension: inferred.csv"+"\n")
            modifier = "since tracking began" if args.absolute else "in an account"
            if args.hr_cutoff: report_file.write("    Stop tracking funds after "+str(args.hr_cutoff)+" hours "+modifier+"."+"\n")
            if args.smallest: report_file.write("    Stop tracking funds below "+str(args.smallest)+" in value."+"\n")
            report_file.write("Running:"+"\n")
        elif heuristic == 'lifo':
            if args.lifo:  report_file.write("    Weighted flows with 'lifo' heuristic saved with extension: flows_lifo.csv"+"\n")
        elif heuristic == 'mixed':
            if args.mixed: report_file.write("    Weighted flows with 'mixed' heuristic saved with extension: flows_mixed.csv"+"\n")
        elif heuristic == 'none':
            if args.none:  report_file.write("    Weighted flows with 'none' heuristic saved with extension: flows_none.csv"+"\n")
        report_file.flush()

def run(system,txn_filename,flow_filename,report_filename,follow_heuristic,cutoff,absolute,smallest,rounding,no_infer):
    from initialize import timewindow_transactions
    from initialize import initialize_transactions
    import os
    import csv
    ################# Reset the system ##################
    system = system.reset()
    ############# Define the tracker class ##############
    Tracker = define_tracker(follow_heuristic,cutoff,absolute,smallest,rounding)
    ############## Redefine report files ################
    untracked_filename = report_filename.replace("report.txt","untracked.csv")
    inferred_filename = report_filename.replace("report.txt","inferred.csv")
    ###################### RUN! #########################
    with open(txn_filename,'r') as txn_file, \
         open(flow_filename,'w') as flow_file, \
         open(report_filename,'a') as report_file, \
         open(inferred_filename,'w') as inferred_file, \
         open(untracked_filename,'w') as untracked_file:
        if no_infer: inferred_file = None
        transactions = csv.DictReader(txn_file,system.txn_header,delimiter=",",quotechar='"',escapechar="%")
        flow_writer = csv.writer(flow_file,delimiter=",",quotechar='"')
        flow_writer.writerow(Flow.header)
        # loop through all transactions, and initialize in reference to the system
        transactions = timewindow_transactions(transactions,system,report_file)
        transactions = initialize_transactions(transactions,system,report_file)
        # now process according to the defined tracking procedure
        for flow in track_transactions(system,transactions,Tracker,report_file,untracked_file,inferred_file):
            flow_writer.writerow(flow.to_print(system.timeformat,rounding))
        # loop through all accounts, and process the remaining funds
        for flow in track_remaining_funds(system,Tracker,report_file,inferred_file):
            flow_writer.writerow(flow.to_print(system.timeformat,rounding))
    if no_infer: os.remove(inferred_filename)

if __name__ == '__main__':
    print("Please run main.py, this file keeps classes and functions.")
