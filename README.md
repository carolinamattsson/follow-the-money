# follow_the_money

This code turns a list of transactions from a financial ecosystem into trajectories of money through that system. These "money flows" include several possible weighting schemes and are built using explicit, modifiable, and accounting-consistent tracking heuristics.

ftm.py contains the classes and functions that 'follow' money through the system, including two promising heuristics

main.py illustrates the required inputs and parameters, highlighting three possible definitions of the system's boundaries

main.py runs the program in batch mode given the run parameters as arguments (main.py transaction_file output_prefix follow_heuristic time_cutoff infer) the file itself must still contain the inputs required to parse the input file, but these do not generally change between runs.

examples/ contains several example files:

    ex1_input.csv -- a simple self-contained series of transactions
    
    ex2_input.csv -- the same self-contained series of transactions with revenue/fees charged by the provider
    
    ex3_input.csv -- the same series of transactions, but no longer self-contained (missing deposits)
    
    ex4_input.csv -- the same series of transactions, but no longer self-contained (missing withdraws)
    
    ex_script.sh    -- runs main.py on each of the example files using several output options
