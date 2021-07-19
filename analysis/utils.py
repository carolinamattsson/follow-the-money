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

def consolidate_txn_types(txn_types, joins):
    for i,txn_type in enumerate(txn_types):
        for join in joins:
            if txn_type in joins[join]: txn_types[i] = join
    return txn_types

def get_categ(wflow):
    # Return the category-combo
    return "~".join(wflow['trj_categ'])

def get_length(wflow,max_transfers=None):
    # Handle the max length (defined as # of transfers)
    transfers = wflow["trj_len"]
    if max_transfers is not None and wflow["trj_len"] >= max_transfers:
        transfers = str(max_transfers)+"+"
    # Return the trajectory length
    return transfers

def get_duration(wflow,cutoffs=[],lower=False):
    # Handle instantaneous trajectories
    if wflow["trj_dur"] is None: return "0"
    # Handle the upper/lower bound; upper is the default
    if not lower and wflow['trj_categ'][1]=='untracked': return "inf"
    if not lower and wflow['txn_types'][-1]=='final': return "inf"
    # Handle the cutoffs or lack thereof
    for start,end in zip([0]+cutoffs,cutoffs+[float("inf")]):
        if start < wflow["trj_dur"] <= end:
            return "("+str(start)+","+str(end)+"]" if end<float("inf") else "("+str(start)+","+str(end)+")"

def get_motif(wflow,consolidate=None,max_transfers=None):
    txn_types = wflow['txn_types'].copy()
    # consolidate transaction types
    if consolidate is not None:
        txn_types = consolidate_txn_types(txn_types,consolidate)
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
    if max_transfers is not None and len(txn_types) >= max_transfers:
        circ = str(max_transfers)+"+_transfers"
    # Return the motif
    return "~".join([enter]+[circ]+[exit]) if circ else "~".join([enter]+[exit])

def get_month(wflow,timeformat="%Y-%m-%d %H:%M:%S"):
    if timeformat[:6]=="%Y-%m-":
        month_ID = "-".join(wflow['trj_timestamp'].split("-")[:-1])
        return month_ID
    else:
        month_ID = datetime.strftime(datetime.strptime(wflow['trj_timestamp'],timeformat),"%Y-%m")
        return month_ID

def cumsum(a_list):
    total = 0
    for x in a_list:
        total += x
        yield total

#######################################################################################################
