#!/bin/bash

# Reset reports
rm ./output/account_report.txt
rm ./output/network_report.txt
# Base functionality
python3 ../../follow_the_money.py account_txns.csv account_config.json output --prefix account_ --lifo --mixed
python3 ../../follow_the_money.py network_txns.csv network_config.json output --prefix network_ --lifo --mixed