from collections import defaultdict
from datetime import datetime, timedelta
import traceback
import math

#######################################################################################################
# Define various utility functions
def parse(wflow):
    wflow['flow_categs']    = tuple(wflow['flow_categs'].strip('()').split(','))
    wflow['flow_acct_IDs']  = wflow['flow_acct_IDs'].strip('[]').split(',')
    wflow['flow_txn_types'] = wflow['flow_txn_types'].strip('[]').split(',')
    wflow['flow_txns']      = [float(txn) for txn in wflow['flow_txns'].strip('[]').split(',')]
    wflow['flow_amts']      = [float(amt) for amt in wflow['flow_amts'].strip('[]').split(',')]
    wflow['flow_revs']      = [float(rev) for rev in wflow['flow_revs'].strip('[]').split(',')]
    wflow['flow_durs']      = [] if wflow['flow_durs'] == "[]" else [float(dur) for dur in wflow['flow_durs'].strip('[]').split(',')]
    return wflow

def timewindow_mask(wflow,timewindow,timeformat):
    '''
    This creates a boolean property for the flow, denoting whether or not each
    transaction occured within the given timewindow.
    '''
    timestamp = datetime.strptime(wflow['flow_timestamp'],timeformat)
    offset_min = (timewindow[0]  - timestamp).total_seconds()/60/60 if timewindow[0] else -float('inf')
    offset_max = (timewindow[-1] - timestamp).total_seconds()/60/60 if timewindow[-1] else float('inf')
    mask = [offset_min <= offset < offset_max for offset in [0.0]+wflow['flow_durs']]
    return mask

###########################################################################################
# Define the savings summary per-trajectory updating function
def update_savings(savings_dist, wflow, max_days):
    # Loop over the users who held money for some period of time
    for i,this_user in enumerate(wflow['flow_acct_IDs'][1:-1]):
        # so long as money entered this account within the timewindow we have
        if wflow['timewindow'][i]:
            # get the number of days this money stayed within this account
            days = int(wflow['flow_durs'][i]//24)
            days = days if days <= max_days else max_days+1
            # update the duration distribution
            savings_dist[this_user][days]['txn'] += wflow['flow_txns'][i]
            savings_dist[this_user][days]['amt'] += wflow['flow_amts'][i]
            savings_dist[this_user][days]['flw'] += 1
    # kick it back
    return savings_dist

###########################################################################################
# Define how the dictionary is written into table format
def cumulative_savings(savings_dist,max_days):
    for user in savings_dist:
        try:
            # fill in so eveyone has a total entry
            if not 0 in savings_dist[user]:
                savings_dist[user][0] = {'amt':0,'txn':0,'flw':0,'amt_c':0,'txn_c':0,'flw_c':0,'amt_cr':0,'txn_cr':0,'flw_cr':0}
            # could up the cumulative values
            prv_dict = {'amt':0,'txn':0,'flw':0,'amt_c':0,'txn_c':0,'flw_c':0,'amt_cr':0,'txn_cr':0,'flw_cr':0}
            for day in range(max_days+1,-1,-1):
                if day in savings_dist[user]:
                    savings_dist[user][day]['amt_c'] = prv_dict['amt_c']+savings_dist[user][day]['amt']
                    savings_dist[user][day]['txn_c'] = prv_dict['txn_c']+savings_dist[user][day]['txn']
                    savings_dist[user][day]['flw_c'] = prv_dict['flw_c']+savings_dist[user][day]['flw']
                    prv_dict = savings_dist[user][day]
            # and divide out by the total turnover
            for day in range(max_days+1,-1,-1):
                if day in savings_dist[user]:
                    savings_dist[user][day]['amt_cr'] = savings_dist[user][day]['amt_c']/savings_dist[user][0]['amt_c']
                    savings_dist[user][day]['txn_cr'] = savings_dist[user][day]['txn_c']/savings_dist[user][0]['txn_c']
                    savings_dist[user][day]['flw_cr'] = savings_dist[user][day]['flw_c']/savings_dist[user][0]['flw_c']
        except:
            print(str(savings_dist[user]))
    return savings_dist

#######################################################################################################
# Define the function that brings it all together
def users_savings(wflow_filename, savings_filename, timewindow=(None,None), timeformat=None, max_days=None):
    ##########################################################################################
    # Define the user summary -- savings_dist[USER_ID][DAYS][TERM]
    savings_dist = defaultdict(lambda: defaultdict(lambda: {'amt':0,'txn':0,'flw':0,'amt_c':0,'txn_c':0,'flw_c':0,'amt_cr':0,'txn_cr':0,'flw_cr':0}))
    ##########################################################################################
    with open(wflow_filename,'r') as wflow_file:
        reader_wflows = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # populate the users dictionary
        for wflow in reader_wflows:
            # Update the dictionary
            try:
                wflow = parse(wflow)
                wflow['timewindow'] = timewindow_mask(wflow,timewindow,timeformat)
                savings_dist = update_savings(savings_dist,wflow,max_days)
            except:
                print(str([wflow[term] for term in wflow])+"\n"+traceback.format_exc())
    # Calculate also the cumulative values
    savings_dist = cumulative_savings(savings_dist,max_days)
    # Create the header
    header = ['user_ID','days','amt','txn','flw','amt_c','txn_c','flw_c','amt_cr','txn_cr','flw_cr']
    # Write the overall total numbers to file
    with open(savings_filename, 'w') as savings_file:
        w = csv.DictWriter(savings_file,header,delimiter=",",quotechar='"',escapechar="%")
        w.writeheader()
        for user in savings_dist:
            for day in range(0,max_days+1):
                try:
                    if day in savings_dist[user]:
                        record = savings_dist[user][day]
                        record['user_ID'] = user
                        record['days'] = day
                        w.writerow(record)
                except:
                    print("user: "+str(user)+"\n"+"day: "+str(day)+"\n"+traceback.format_exc())


if __name__ == '__main__':
    import argparse
    import sys
    import csv
    import os

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input weighted flow file (created by follow_the_money.py)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output files')
    parser.add_argument('--timewindow', default='(,)', help='Include funds that entered accounts within this time window.')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format to read the --timewindow tuple, if different.')
    parser.add_argument('--max_days', default=None, help='Aggregate by day up to this number of days. It is most useful to cooridinate this with the timewindow.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    savings_filename = os.path.join(args.output_directory,args.prefix+"users_savings.csv")

    timewindow = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow.strip('()').split(',')])
    max_days = int(args.max_days)

    ######### Creates weighted flow file #################
    users_savings(wflow_filename, savings_filename, timewindow=timewindow, timeformat=args.timeformat, max_days=max_days)
    #################################################
