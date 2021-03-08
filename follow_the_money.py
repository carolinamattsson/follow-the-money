'''
Follow The Money - main
This is the script to run a basic "follow the money" data transformation within the specified system boundaries.

How to execute this code from the linux command line:
python3 /path/to/input-file.csv /path/to/config-file.json /path/to/output-directory/ --lifo --infer --prefix "foo"

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
    parser.add_argument('--lifo', action="store_true", default=False, help='Track the using the "lifo" heuristic')
    parser.add_argument('--mixed', action="store_true", default=False, help='Track the using the "mixed" heuristic')
    parser.add_argument('--none', action="store_true", default=False, help='Track the using the baseline "no-tracking" heuristic')
    parser.add_argument('--no_balance', action="store_true", default=False, help='Avoid inferring account balances at start, when known')
    parser.add_argument('--no_infer', action="store_true", default=False, help='Avoid inferring unseen deposit and withdrawal transactions')
    parser.add_argument('--cutoff', metavar='hours', type=float, default=None, help='Stop tracking funds after this number of hours')
    parser.add_argument('--smallest', metavar='value', type=float, default=0.01, help='Stop tracking funds with a value below this threshold')

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
    ############## Begin the report file ###############
    report_filename = os.path.join(args.output_directory,args.prefix+"report.txt")
    init.start_report(report_filename,args,config_data)
    ################ Initialize system #################
    system = init.setup_system(config_data)
    ########## Define accounting convention ############
    if "fee/revenue" in config_data:
        system = init.define_fee_accounting(system,config_data)
    ############# Define system boundary ###############
    if "boundary_type" in config_data:
        system = init.define_system_boundary(system,config_data)
        ########### Infer account categories ###############
        if config_data["boundary_type"] in ['inferred_accounts','inferred_accounts+otc']:
            system = init.infer_account_categories(system,transaction_filename,report_filename)
    ########## Define how to read balances #############
    if "balance_type" in config_data:
        system.define_balance_functions(config_data["balance_type"])
    ######### Initialize balances ahead of time ########
    if not args.no_balance:
        init.infer_starting_balance(system,transaction_filename,report_filename)
    ####################################################

    #################### OUTPUT ########################
    follow.update_report(report_filename,args)
    ####################################################
    file_ending = ".csv"
    if args.no_balance: file_ending = "_nbal"+file_ending
    if args.no_infer:   file_ending = "_ninf"+file_ending
    if args.cutoff:     file_ending = "_"+str(args.cutoff)+"hr"+file_ending
    ############### Alright, let's go! #################
    if args.lifo:
        follow.update_report(report_filename,args,heuristic='lifo')
        output_filename = os.path.join(args.output_directory,args.prefix+"flows_lifo"+file_ending)
        follow.run(system,transaction_filename,output_filename,report_filename,'lifo',args.cutoff,args.smallest,args.no_infer)
    if args.mixed:
        follow.update_report(report_filename,args,heuristic='mixed')
        output_filename = os.path.join(args.output_directory,args.prefix+"flows_mixed"+file_ending)
        follow.run(system,transaction_filename,output_filename,report_filename,'mixed',args.cutoff,args.smallest,args.no_infer)
    if args.none:
        follow.update_report(report_filename,args,heuristic='none')
        output_filename = os.path.join(args.output_directory,args.prefix+"flows_none"+file_ending)
        follow.run(system,transaction_filename,output_filename,report_filename,'none',args.cutoff,args.smallest,args.no_infer)
    ####################################################
