from collections import defaultdict
from datetime import datetime, timedelta
import traceback
import math

from utils import parse, timewindow_trajectories, timewindow_accounts

###########################################################################################
# Define the savings summary per-trajectory updating function
def update_savings(savings_dist, wflow, max_days):
    # Loop over the accounts that recieved money, and the transactions recieved
    # Note: wflow['trj_durs'] will limit the loop except for "untracked" funds
    for acct_ID,acct_tw,dur,amt,txn in zip(wflow['acct_IDs'][1:],\
                                           wflow['acct_tws'][1:],
                                           wflow['acct_durs'],\
                                           wflow['txn_amts'],\
                                           wflow['txn_txns']):
        # skip if this did not occur within the relevant timewindow
        if not acct_tw: continue
        # get the number of days this money stayed within this account
        days = int(dur//24)
        days = days if max_days is None else (days if days < max_days else max_days)
        # update the duration distribution
        savings_dist[acct_ID][days]['amt'] += amt
        savings_dist[acct_ID][days]['txn'] += txn
        savings_dist[acct_ID][days]['flw'] += 1
    # kick it back
    return savings_dist

###########################################################################################
# Define how the dictionary is written into table format
def cumulative_savings(savings_dist,max_days=None):
    max_key = max_days if max_days is not None else max([max(savings_dist[acct_ID].keys()) for acct_ID in savings_dist])
    for user in savings_dist:
        try:
            # fill in so eveyone has a total entry
            if not 0 in savings_dist[user]:
                savings_dist[user][0] = {'amt':0,'txn':0,'flw':0,'amt_c':0,'txn_c':0,'flw_c':0,'amt_cr':0,'txn_cr':0,'flw_cr':0}
            # could up the cumulative values
            prv_dict = {'amt':0,'txn':0,'flw':0,'amt_c':0,'txn_c':0,'flw_c':0,'amt_cr':0,'txn_cr':0,'flw_cr':0}
            for day in range(max_key,-1,-1):
                if day in savings_dist[user]:
                    savings_dist[user][day]['amt_c'] = prv_dict['amt_c']+savings_dist[user][day]['amt']
                    savings_dist[user][day]['txn_c'] = prv_dict['txn_c']+savings_dist[user][day]['txn']
                    savings_dist[user][day]['flw_c'] = prv_dict['flw_c']+savings_dist[user][day]['flw']
                    prv_dict = savings_dist[user][day]
            # and divide out by the total turnover
            for day in range(max_key,-1,-1):
                if day in savings_dist[user]:
                    savings_dist[user][day]['amt_cr'] = savings_dist[user][day]['amt_c']/savings_dist[user][0]['amt_c']
                    savings_dist[user][day]['txn_cr'] = savings_dist[user][day]['txn_c']/savings_dist[user][0]['txn_c']
                    savings_dist[user][day]['flw_cr'] = savings_dist[user][day]['flw_c']/savings_dist[user][0]['flw_c']
        except:
            print(str(savings_dist[user]))
    return savings_dist

#######################################################################################################
# Define the function that brings it all together
def users_savings(wflow_filename, savings_filename, max_days=None, timewindow_trj=(None,None), timewindow_accts=(None,None), timeformat="%Y-%m-%d %H:%M:%S"):
    ##########################################################################################
    # Define the user summary -- savings_dist[USER_ID][DAYS][TERM]
    savings_dist = defaultdict(lambda: defaultdict(lambda: {'amt':0,'txn':0,'flw':0,'amt_c':0,'txn_c':0,'flw_c':0,'amt_cr':0,'txn_cr':0,'flw_cr':0}))
    ##########################################################################################
    with open(wflow_filename,'r') as wflow_file:
        reader_wflows = csv.DictReader(wflow_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        # populate the users dictionary
        for wflow in timewindow_trajectories(reader_wflows,timewindow_trj,timeformat):
            # Update the dictionary
            try:
                wflow = parse(wflow,timeformat)
                wflow['acct_tws'] = timewindow_accounts(wflow,timewindow_accts,timeformat)
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
            for day in savings_dist[user]:
                record = savings_dist[user][day]
                record['user_ID'] = user
                record['days'] = day
                w.writerow(record)

if __name__ == '__main__':
    import argparse
    import sys
    import csv
    import os

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input weighted flow file (created by follow_the_money.py)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output filenames')
    parser.add_argument('--suffix', default="", help='Suffix appended to output filenames')
    parser.add_argument('--max_days', default=None, help='Aggregate by day up to this number of days, as an integer.')
    parser.add_argument('--timewindow', default='(,)', help='Include funds that entered accounts within this time window, as a tuple.')
    parser.add_argument('--timeformat', default="%Y-%m-%d %H:%M:%S", help='Format used for timestamps in trajectory file & timewindow(s), as a string.')
    # trajectory-based filtering
    parser.add_argument('--timewindow_trj', default='(,)', help='Include trajectories that begin within this time window, as a tuple.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    wflow_filename = args.input_file
    savings_filename = os.path.join(args.output_directory,args.prefix+"savings"+args.suffix+".csv")

    timewindow = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow.strip('()').strip('[]').split(',')])
    timewindow_trj = tuple([(datetime.strptime(timestamp,args.timeformat) if timestamp else None) for timestamp in args.timewindow_trj.strip('()').split(',')])
    max_days = int(args.max_days) if args.max_days else None

    ######### Creates weighted flow file #################
    users_savings(wflow_filename, savings_filename, max_days=max_days, timewindow_trj=timewindow_trj, timewindow_accts=timewindow, timeformat=args.timeformat)
    #################################################
