#!/bin/bash

# Reset reports
rm ./output/prev_report.txt
rm ./output/minus_report.txt
rm ./output/plus_report.txt
# Changing the time window
python3 ../../follow_the_money.py account_txns+.csv account_config.json output --prefix account_prev_ --lifo --mixed
python3 ../../follow_the_money.py account_txns+.csv account_config-.json output --prefix account_minus_ --lifo --mixed
python3 ../../follow_the_money.py account_txns+.csv account_config+.json output --prefix account_plus_ --lifo --mixed
