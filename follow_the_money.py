'''
Follow The Money - main
This is the script to run a basic "follow the money" data transformation within the specified system boundaries.
Author: Carolina Mattsson, Northeastern University, April 2018
'''

if __name__ == '__main__':
    from shutil import copyfile
    import argparse
    import json
    import sys
    import csv
    import os

    import initialize as init
    import follow as follow

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input transaction file (.csv)')
    parser.add_argument('config_file', help='The configuration file (.json)')
    parser.add_argument('output_directory', help='Path to the output directory')
    parser.add_argument('--prefix', default="", help='Prefix prepended to output files')
    parser.add_argument('--greedy', action="store_true", default=False, help='Track the using the "greedy" heuristic')
    parser.add_argument('--well_mixed', action="store_true", default=False, help='Track the using the "well-mixed" heuristic')
    parser.add_argument('--no_tracking', action="store_true", default=False, help='Track the using the baseline "no-tracking" heuristic')
    parser.add_argument('--cutoff', metavar='hours', nargs=1, type=int, default=None, help='Stop tracking funds after this number of hours')
    parser.add_argument('--smallest', metavar='value', nargs=1, type=int, default=0.01, help='Stop tracking funds with a value below this threshold')
    parser.add_argument('--infer', action="store_true", default=False, help='Record inferred deposits and withdrawals as transactions')
    parser.add_argument('--balance', action="store_true", default=False, help='Before running, infer the starting balance of all accounts')
    #parser.add_argument('--read_balance', nargs=3, type=str, default=None, help='Read balance information from these columns of the transaction file, either as the "starting" or "ending" balance')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isfile(args.config_file):
        raise OSError("Could not find the config file",args.config_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    ##################### INPUT ########################
    transaction_filename = args.input_file
    ########### Read the configuration file ############
    with open(args.config_file, 'r') as config_file:
        config_data = json.load(config_file)
    transaction_header = config_data["transaction_header"]
    timeformat = config_data["timeformat"]
    timewindow = (config_data["timewindow_beg"],config_data["timewindow_end"])
    boundary_type = config_data["boundary_type"] if config_data["boundary_type"] else None
    ############# Define what a *user* is ##############
    report_filename = os.path.join(args.output_directory,args.prefix+"system_report.txt")
    if boundary_type:
        if boundary_type == 'transactions':
            system = init.setup_system(transaction_header,timeformat,timewindow,boundary_type,transaction_categories=config_data["transaction_categories"])
        elif boundary_type == 'accounts':
            system = init.setup_system(transaction_header,timeformat,timewindow,boundary_type,account_categories=config_data["account_categories"],category_order=config_data["account_order"],category_follow=config_data["account_following"])
            system = init.get_account_categories(system,transaction_filename,report_filename)
        else:
            raise ValueError("Check config file -- boundary_type options are 'transactions' and 'accounts'",boundary_type)
    else:
        system = init.setup_system(transaction_header,timeformat,timewindow,boundary_type)
    ############## Infer starting balance ##############
    if args.balance:
        system = init.get_starting_balance(system,transaction_filename,report_filename)
    #################### OUTPUT ########################
    report_filename = os.path.join(args.output_directory,args.prefix+"wflows_report.txt")
    follow.start_report(report_filename,args)
    ############### Alright, let's go! #################
    if args.greedy:
        greedy_filename = os.path.join(args.output_directory,args.prefix+"wflows_greedy.csv")
        follow.run(system,transaction_filename,greedy_filename,report_filename,'greedy',args.cutoff,args.smallest,args.infer)
    if args.well_mixed:
        well_mixed_filename = os.path.join(args.output_directory,args.prefix+"wflows_well-mixed.csv")
        follow.run(system,transaction_filename,well_mixed_filename,report_filename,'well-mixed',args.cutoff,args.smallest,args.infer)
    if args.no_tracking:
        no_tracking_filename = os.path.join(args.output_directory,args.prefix+"wflows_no-tracking.csv")
        follow.run(system,transaction_filename,no_tracking_filename,report_filename,'no-tracking',args.cutoff,args.smallest,args.infer)
    ####################################################
