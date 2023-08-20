#!/bin/bash

# Reset reports
rm ./output/account_report.txt
rm ./output/network_report.txt
# Pairwise
python3 ../../follow_the_money.py account_txns.csv account_config.json output --prefix account_ --lifo --mixed --pairwise
python3 ../../follow_the_money.py network_txns.csv network_config.json output --prefix network_ --lifo --mixed --pairwise

# Reset reports
rm ./output/account_sm500_report.txt
rm ./output/network_sm30000_report.txt
# Pairwise with biting smallest limit
python3 ../../follow_the_money.py account_txns.csv account_config.json output --prefix account_sm500_ --mixed --pairwise --smallest 5
python3 ../../follow_the_money.py network_txns.csv network_config.json output --prefix network_sm30000_ --mixed --pairwise --smallest 300