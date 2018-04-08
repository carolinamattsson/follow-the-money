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
    def __init__(self, txn, accts_dict, timeformat):
        self.txn_ID    = txn['txn_ID']
        self.timestamp = datetime.strptime(txn['timestamp'],timeformat)
        self.type      = txn['txn_type']
        self.src_acct  = accts_dict[txn['src_ID']].account
        self.tgt_acct  = accts_dict[txn['tgt_ID']].account
        self.amt       = float(txn['amt'])
        self.rev       = float(txn['rev'])
        self.rev_ratio = self.rev/self.amt
    def infer_deposit(self):
        inf_txn = copy.copy(self)
        inf_txn.txn_ID    = 'i'
        inf_txn.type      = 'inferred'
        inf_txn.tgt_acct  = self.src_acct
        inf_txn.amt       = self.amt-self.src_acct.balance
        inf_txn.rev       = 0
        inf_txn.rev_ratio = 0
        self.src_acct.deposit(inf_txn)
        return None

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
    def __init__(self, branch, amt, rev):
        # "money flows" have a size (flow.amt), a length (flow.tux), and a duration of time that they remained in the system (flow.duration)
        # the specific trajectory is described by a list of transactions, through a list of accounts, where the money stayed for a list of durations
        # when aggregating over "money flows", they can be weighted by their size or by their root transactions using flow.frac_root
        self.txn_IDs   = [branch.txn.txn_ID]
        self.txn_types = [branch.txn.type]
        self.acct_IDs  = [branch.txn.src_acct.acct_ID,branch.txn.tgt_acct.acct_ID]
        self.amt       = amt+rev
        self.rev       = rev
        self.frac_root = (amt+rev)/(branch.txn.amt+branch.txn.rev)
        self.timestamp = branch.txn.timestamp
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
        self.rev       += rev
        branch_duration = branch.txn.timestamp - branch.prev.txn.timestamp
        self.duration  += branch_duration
        self.durations.append(branch_duration)
        self.tux       += 1
        self.tux_wrev  += amt/self.amt
    def to_print(self):
        # this returns a version of this class that can be exported to a file using writer.writerow()
        return [str(self.txn_IDs),str(self.txn_types),str(self.acct_IDs),self.amt,self.rev,self.frac_root,str(self.timestamp),str(self.duration),str([str(dur) for dur in self.durations]),self.tux,self.tux_wrev]

class LIFO_account:
    # this class keeps track of transactions within an account holder's account
    # it chains outgoing transactions (well, parts of those transactions) to earlier incoming ones
    # specifically, it chains outgoing transactions to the *most recent* incoming transactions
    # this heuristic -- last in first out (LIFO) -- has the pleasing property of preserving local patterns
    def __init__(self, acct_ID):
        # this account initialization ensures there will always be a "root branch" in an account - useful for handling finite data - will be further developed using inferred deposit transactions
        self.stack   = [Branch(None, None, float('inf'))]
        self.acct_ID = acct_ID
        self.balance = 0
    def balance_check(self, txn):
        # this returns True if there is enough in the account to process the transaction and False if not
        return True if txn.amt <= self.balance else False
    def add_branch(self, branch):
        # according to the LIFO heuristic, incoming transactions mean "branches" get added to the end of the account.stack
        self.stack.append(branch)
        self.balance += branch.amt
    def pop_branches(self, amt):
        # according to the LIFO heuristic, outgoing transactions mean "branches" are removed from the end of the account.stack
        if amt > self.balance: pass # raise accounting exception - to be implemented in the future
        branches = []
        while amt > 0:
            # "branches" are removed from the end of the account.stack until the amount of the transaction is reached
            branch = self.stack[-1]
            if branch.amt <= amt:
                branches.append(self.stack.pop())
                amt = amt - branch.amt
            else:
                # If the last "branch" is larger than the amount to be removed from the account, it is split into two: one remains in this account and the other is sent along
                branches.append(Branch(branch.prev,branch.txn,amt))
                branch.decrement(amt)
                amt = 0
        return reversed(branches) # this list is reversed to preserve the newest branches at the end
    def deposit(self, this_txn):
        # accounts process deposit transactions by adding a new "root branch" to the end of their account.stack
        self.add_branch(Branch(None,this_txn,this_txn.amt))
    def transfer(self, this_txn):
        # accounts process transfer transactions by:
        #     - removing branches up to the amount of the transaction from this account.stack
        #     - creating the subsequent "branches" (extending the "tree")
        #     - adding these "branches" to the end of the account.stack of the account receiving the transaction
        amt_to_follow = this_txn.amt+this_txn.rev
        for prev_branch in self.pop_branches(amt_to_follow):
            if prev_branch.amt < min_branch_amt:
                del prev_branch
                continue
            this_branch = Branch(prev_branch, this_txn, prev_branch.amt/(1.0+this_txn.rev_ratio))
            this_txn.tgt_acct.add_branch(this_branch)
    def withdraw(self, this_txn):
        # accounts process withdraw transactions by:
        #     - removing branches up to the amount of the transaction from this account.stack
        #     - creating the subsequent "leaf branches"
        #     - returning the "money flows" that end because of this withdraw transaction (by calling branch.follow_back() on the "leaf branches")
        #     - removing the "leaf branches" from memory
        amt_to_follow = this_txn.amt+this_txn.rev
        flows = []
        for prev_branch in self.pop_branches(amt_to_follow):
            if prev_branch.amt < min_branch_amt:
                del prev_branch # floating point errors etc. can lead to teeny tiny branches that we will ignore
                continue
            this_branch = Branch(prev_branch, this_txn, prev_branch.amt/(1.0+this_txn.rev_ratio))
            flows.append(this_branch.follow_back(this_branch.amt))
            del this_branch
            # branches will disappear when there are no accounts who still reference their upstream branches
            # transactions will disappear when there are no branches left that reference them
        return flows

#class MaxEntropy_account:
#  other account classes, that reflect different money-tracking heuristics, are possible and this one will be implemented in the future

class Account_holder:
    # this class defines accounts in the dataset and holds features of them
    def __init__(self, account):
        self.account = account
        self.type    = 'user'
        #self.txns    = 0      # in the future, this class will hold optional metrics calculated
        #self.amt     = 0      #                in an ongoing manner that can be retrieved system-wide
        #self.volume  = 0      #                at specified intervals
        #self.active  = 0
    # the functions below will catch if transaction processing is asked of the account holder
    # rather than the account itself, in the future they will also throw a warning
    def deposit(self, transaction):
        return self.account.deposit(transaction)
    def transfer(self, transaction):
        return self.account.transfer(transaction)
    def withdraw(self, transaction):
        return self.account.withdraw(transaction)

def process(txn,accts_dict,txn_categ,timeformat,resolution_limit,infer=True):
    global min_branch_amt
    min_branch_amt = resolution_limit
    # make sure both the sender and recipient are account_holders with accounts
    accts_dict.setdefault(txn['src_ID'],Account_holder(LIFO_account(txn['src_ID'])))
    accts_dict.setdefault(txn['tgt_ID'],Account_holder(LIFO_account(txn['tgt_ID'])))
    # convert the transaction into its type, passing the transaction and the accounts involved
    txn = Transaction(txn,accts_dict,timeformat)
    # process the transaction!
    if txn_categ[txn.type] == 'deposit':
        return txn.tgt_acct.deposit(txn)
    if txn_categ[txn.type] == 'transfer':
        if infer and not txn.src_acct.balance_check(txn): txn.infer_deposit()
        return txn.src_acct.transfer(txn)
    if txn_categ[txn.type] == 'withdraw':
        if infer and not txn.src_acct.balance_check(txn): txn.infer_deposit()
        return txn.src_acct.withdraw(txn)

if __name__ == '__main__':
    print("Please run main.py, this file only keeps the classes and functions.")
