#!/bin/bash

# Reset reports
rm ./output/account_balances_report.txt
rm ./output/account_missing_report.txt
# Incorporating known account balances
python3 ../../follow_the_money.py account_txns_bal.csv account_config_bal.json output --prefix account_balances_ --lifo --mixed
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix account_missing_ --lifo --mixed

# Reset reports
rm ./output/account_balances_nbal_report.txt
rm ./output/account_missing_nbal_report.txt
# Incorporating known account balances --no_balance
python3 ../../follow_the_money.py account_txns_bal.csv account_config_bal.json output --prefix account_balances_ --lifo --mixed --no_balance
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix account_missing_ --lifo --mixed --no_balance

# Reset reports
rm ./output/account_balances_ninf_report.txt
rm ./output/account_missing_ninf_report.txt
# Incorporating known account balances --no_infer
python3 ../../follow_the_money.py account_txns_bal.csv account_config_bal.json output --prefix account_balances_ --lifo --mixed --no_infer
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix account_missing_ --lifo --mixed --no_infer

# Can't do both
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix account_balances_ --lifo --mixed --no_balance --no_infer
