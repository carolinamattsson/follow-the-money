'''
Follow The Money - main
This is the script to run a basic "follow the money" data transformation within the specified system boundaries.
Author: Carolina Mattsson, Northeastern University, October 2018

How to execute this code from the linux command line:
python3 /path/to/input-file.csv /path/to/config-file.json /path/to/output-directory/ --greedy --infer --prefix "foo"

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
    parser.add_argument('--infer', action="store_true", default=False, help='Record inferred deposits and withdrawals as transactions')
    parser.add_argument('--cutoff', metavar='hours', type=int, default=None, help='Stop tracking funds after this number of hours')
    parser.add_argument('--smallest', metavar='value', type=int, default=0.01, help='Stop tracking funds with a value below this threshold')
    parser.add_argument('--no_balance', action="store_true", default=False, help='Avoid inferring starting balances before running well-mixed (no effect if balances are given)')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if not os.path.isfile(args.config_file):
        raise OSError("Could not find the config file",args.config_file)
    if not os.path.isdir(args.output_directory):
        raise OSError("Could not find the output directory",args.output_directory)

    report_filename = os.path.join(args.output_directory,args.prefix+"report.txt")
    init.start_report(report_filename,args)
    ##################### INPUT ########################
    transaction_filename = args.input_file
    ########### Read the configuration file ############
    with open(args.config_file, 'r') as config_file:
        config_data = json.load(config_file)
    ################ Initialize system #################
    system = init.setup_system(config_data)
    ########## Define accounting convention ############
    if "revenue/fee" in config_data:
        system = init.define_fee_accounting(system,config_data)
    ############# Define system boundary ###############
    if "boundary_type" in config_data:
        system = init.define_system_boundary(system,config_data)
    ########### Infer account categories ###############
    if config_data["boundary_type"] in ['inferred_accounts','inferred_accounts+otc']:
        system = init.infer_account_categories(system,transaction_filename,report_filename)
    ########## Read/infer starting balance #############
    if "balance_type" in config_data:
        system.define_needs_balances(config_data["balance_type"])
    elif not args.no_balance:
        init.infer_starting_balance(system,transaction_filename,report_filename)
    ####################################################

    #################### OUTPUT ########################
    follow.update_report(report_filename,args)
    ####################################################
    file_ending = ".csv"
    if args.infer:  file_ending = "_inf"+file_ending
    if args.no_balance: file_ending = "_nb"+file_ending
    if args.cutoff: file_ending = "_"+str(args.cutoff)+"hr"+file_ending
    ############### Alright, let's go! #################
    if args.greedy:
        filename = os.path.join(args.output_directory,args.prefix+"wflows_greedy"+file_ending)
        follow.run(system,transaction_filename,filename,report_filename,'greedy',args.cutoff,args.smallest,args.infer,args.no_balance)
    if args.well_mixed:
        filename = os.path.join(args.output_directory,args.prefix+"wflows_well-mixed"+file_ending)
        follow.run(system,transaction_filename,filename,report_filename,'well-mixed',args.cutoff,args.smallest,args.infer,args.no_balance)
    if args.no_tracking:
        filename = os.path.join(args.output_directory,args.prefix+"wflows_no-tracking"+file_ending)
        follow.run(system,transaction_filename,filename,report_filename,'no-tracking',args.cutoff,args.smallest,args.infer,args.no_balance)
    ####################################################
