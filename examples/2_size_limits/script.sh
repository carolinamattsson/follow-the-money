#!/bin/bash

# Reset reports
rm ./output/account_sm500_report.txt
rm ./output/network_sm30000_report.txt
# Limiting the size of tracked flows (my convention is to always use two decimal points for noting the smallest limit)
python3 ../../follow_the_money.py account_txns.csv account_config.json output --prefix account_sm500_ --lifo --mixed --smallest 5
python3 ../../follow_the_money.py network_txns_sm.csv network_config_sm.json output --prefix network_sm30000_ --lifo --mixed --smallest 300