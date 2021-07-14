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
        try:
            wflow[term] = float(wflow[term])
        except:
            None
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

def consolidate_txn_types(wflow, joins):
    for i,txn_type in enumerate(wflow['txn_types']):
        for join in joins:
            if txn_type in joins[join]: wflow['txn_types'][i] = join
    return wflow

def cumsum(a_list):
    total = 0
    for x in a_list:
        total += x
        yield total
