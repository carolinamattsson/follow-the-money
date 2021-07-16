##################################################################
###### Helper functions used in many of the anlysis scripts ######
##################################################################

from datetime import datetime, timedelta

def parse(wflow,timeformat):
    # timestamp, category, and within-system length
    wflow['trj_timestamp'] = datetime.strptime(wflow['trj_timestamp'],timeformat)
    wflow['trj_categ'] = tuple(wflow['trj_categ'].strip('()').split(','))
    wflow['trj_len']   = int(wflow['trj_len']) # where was this len('txn_IDs')?
    # convert whole-trajectory values
    for term in ['trj_amt','trj_txn','trj_dur']:
        wflow[term] = None if wflow[term]=="" else float(wflow[term])
    # unpack lists
    for term in ['txn_IDs','txn_types','txn_amts','txn_revs','txn_txns','acct_IDs','acct_durs']:
        wflow[term] = [] if wflow[term]=="[]" else wflow[term].strip('[]').split(',')
    # convert within-trajectory values
    for term in ['txn_amts','txn_revs','txn_txns','acct_durs']:
        wflow[term] = [float(val) for val in wflow[term]]
    # return!
    return wflow

def timewindow_trajectories(wflows,timewindow,timeformat):
    '''
    This generator yields all trajectories that began within the timewindow.
    '''
    for wflow in wflows:
        if timewindow[0] or timewindow[-1]:
            timestamp = datetime.strptime(wflow['trj_timestamp'],timeformat)
        if timewindow[0] and timestamp < timewindow[0]:
            continue
        if timewindow[-1] and timestamp >= timewindow[-1]:
            continue
        yield wflow

def partial_trajectories(wflows,fees=False):
    '''
    This generator yields the partial trajectories that ended in fees/revenue,
    as well as the remainder of the given trajectories.
    '''
    for wflow in wflows:
        if not fees:
            yield wflow
        else:
            #TODO
            yield wflow

def timewindow_accounts(wflow, timewindow, timeformat):
    '''
    This creates a boolean property for the trajectory denoting for each account
    whether or not it recieved funds within the given timewindow.
    '''
    offset_min = (timewindow[0]  - wflow['trj_timestamp']).total_seconds()/60/60 if timewindow[0] else -float('inf')
    offset_max = (timewindow[-1] - wflow['trj_timestamp']).total_seconds()/60/60 if timewindow[-1] else float('inf')
    mask = [False]+[offset_min <= offset < offset_max for offset in [0.0]+wflow['acct_durs']]
    return mask

def consolidate_txn_types(wflow, joins):
    for i,txn_type in enumerate(wflow['txn_types']):
        for join in joins:
            if txn_type in joins[join]: wflow['txn_types'][i] = join
    return wflow

def get_categ(wflow):
    # Return the category-combo
    return "~".join(wflow['trj_categ'])

def get_length(wflow,max_transfers=None):
    # Handle the max length
    transfers = wflow["trj_len"]
    if max_transfers and wflow["trj_len"] > max_transfers:
        transfers = str(max_transfers+1)+"+"
    # Return the trajectory length
    return transfers

def get_motif(wflow,max_transfers=None):
    txn_types = wflow['txn_types'].copy()
    # Handle the start of trajectories
    if wflow['trj_categ'][0]=='deposit':
        enter = txn_types.pop(0)
    elif wflow['trj_categ'][0]=='untracked':
        enter = ""
    else:
        raise ValueError("Bad trj_categ:",wflow['trj_categ'][0])
    # Handle the end of trajectories
    if wflow['trj_categ'][1]=='withdraw':
        exit = txn_types.pop()
    elif wflow['trj_categ'][1]=='untracked':
        exit = ""
    else:
        raise ValueError("Bad trj_categ:",wflow['trj_categ'][1])
    # Handle the middle of trajectories
    circ  = "~".join(txn_types)
    if max_transfers and len(txn_types) > max_transfers:
        circ = ">"+str(max_transfers)+"transfers"
    # Return the motif
    return "~".join([enter]+[circ]+[exit]) if circ else "~".join([enter]+[exit])

def cumsum(a_list):
    total = 0
    for x in a_list:
        total += x
        yield total

#######################################################################################################
