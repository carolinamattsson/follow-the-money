#!/bin/bash

# Incorporating known account balances
python3 ../../follow_the_money.py account_txns_bal.csv account_config_bal.json output --prefix bal_ --lifo --mixed
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix miss_ --lifo --mixed

# Incorporating known account balances --no_balance
python3 ../../follow_the_money.py account_txns_bal.csv account_config_bal.json output --prefix bal_ --lifo --mixed --no_balance
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix miss_ --lifo --mixed --no_balance

# Incorporating known account balances --no_infer
python3 ../../follow_the_money.py account_txns_bal.csv account_config_bal.json output --prefix bal_ --lifo --mixed --no_infer
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix miss_ --lifo --mixed --no_infer

# Can't do both
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix miss_ --lifo --mixed --no_balance --no_infer
