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

def bin_duration(duration,bound=float("inf"),cutoffs=[]):
    '''
    bin the given duration by the given cutoffs
    '''
    # Handle the cutoff points, or lack thereof
    for start,end in zip([float("-inf")]+cutoffs,cutoffs+[bound]):
        if start < duration <= end:
            return "("+str(start)+","+str(end)+"]" if end<bound else "("+str(start)+","+str(end)+")"
    # If that fails
    return float("nan")

def cumsum(a_list):
    total = 0
    for x in a_list:
        total += x
        yield total

#######################################################################################################

def finalize_summary(summary,split_bys,sets=[],flows=True):
    '''
    finalize the summary dictionary, given this list of split_by terms
    '''
    import math
    from utils import cumsum
    for split in list(summary.keys()):
        # for each split of the trajectory data
        split_summary = summary[split]
        # generate a column for each term used to split the data
        for term,value in zip(split_bys,split):
            split_summary[term] = value
        # retrieve the number of unique entry points, exit points, and users
        for set in sets:
            split_summary[set] = len(split_summary[set])
        # normalize the weighted average durations
        split_summary["avg_dur_f"] = split_summary["avg_dur_f"]/split_summary["flows"]
        split_summary["avg_dur_a"] = split_summary["avg_dur_a"]/split_summary["amount"]
        try:
            split_summary["avg_dur_d"] = split_summary["avg_dur_d"]/split_summary["deposits"]
        except:
            split_summary["avg_dur_d"] = float("nan")
        # normalize the fraction complete trajectories
        split_summary["frc_cpl_f"] = split_summary["frc_cpl_f"]/split_summary["flows"]
        split_summary["frc_cpl_a"] = split_summary["frc_cpl_a"]/split_summary["amount"]
        try:
            split_summary["frc_cpl_d"] = split_summary["frc_cpl_d"]/split_summary["deposits"]
        except:
            split_summary["frc_cpl_d"] = float("nan")
        # summarize the duration distribution, if there was one
        #if split_summary["durations"]:
        #    split_summary["durations"].sort()
        #    #flow_cumsum = list(cumsum([1 for x in split_summary["durations"]]))
        #    #flow_mid = next(i for i,v in enumerate(flow_cumsum) if v >= flow_cumsum[-1]/2)
        #    if flows:
        #        flow_mid = math.ceil(len(split_summary["durations"])/2) - 1
        #        split_summary["median_dur_f"] = split_summary["durations"][flow_mid][0]
        #    amt_cumsum = list(cumsum([x[1] for x in split_summary["durations"]]))
        #    amt_mid = next(i for i,v in enumerate(amt_cumsum) if v >= amt_cumsum[-1]/2)
        #    split_summary["median_dur_a"] = split_summary["durations"][amt_mid][0]
        #    nrm_cumsum = list(cumsum([x[2] for x in split_summary["durations"]]))
        #    nrm_mid = next(i for i,v in enumerate(nrm_cumsum) if v >= nrm_cumsum[-1]/2)
        #    split_summary["median_dur_d"] = split_summary["durations"][nrm_mid][0]
        #else:
        #    if flows:
        #        split_summary["median_dur_f"] = ""
        #    split_summary["median_dur_a"] = ""
        #    split_summary["median_dur_d"] = ""
        # relieve some memory pressure
        #del split_summary["durations"]
        # update this entry in the above dictionary
        summary[split] = split_summary
    return summary

def write_summary(summary,output_file,summary_header):
    import csv
    with open(output_file,'w') as output_file:
        writer_summary = csv.DictWriter(output_file,summary_header,delimiter=",",quotechar="'",escapechar="%")
        # print header
        writer_summary.writeheader()
        # print distribution
        for split in summary:
            writer_summary.writerow(summary[split])

#######################################################################################################
