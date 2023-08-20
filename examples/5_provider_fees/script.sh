#!/bin/bash

# Reset reports
rm ./output/account_fees_report.txt
rm ./output/network_fees_report.txt
# Incorporating transaction fees
python3 ../../follow_the_money.py account_txns_fees.csv account_config_fees.json output --prefix account_fees_ --lifo --mixed
python3 ../../follow_the_money.py network_txns_fees.csv network_config_fees.json output --prefix network_fees_ --lifo
