# follow_the_money
This code turns a list of transactions from a financial ecosystem into trajectories of money through that system. These "money flows" include several possible weighting schemes and are built using an explicit, modifiable, and accounting-consistent tracking heuristic.
ftm.py contains the classes and functions that 'follow' money through the system
main.py illustrates the required inputs when users are defined on a transaction-by-transaction basis
main2.py illustrates the required inputs when users are defined ongoingly as described in account_types.csv 
examples/ contains several example files:
    ex1 -- a simple self-contained series of transactions
    ex2 -- the same self-contained series of transactions with revenue/fees charged by the provider
    ex3 -- the same series of transactions, but no longer self-contained (requires inferred deposits)
    ex4 -- the same series of transactions, but no longer self-contained (requires inferred withdraws)
