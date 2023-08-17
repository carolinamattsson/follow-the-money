#!/bin/bash

# Reset reports
rm ./output/acct_fee_report.txt
rm ./output/ntwk_fee_report.txt
# Incorporating transaction fees
python3 ../../follow_the_money.py account_txns_fees.csv account_config_fees.json output --prefix account_fees_ --lifo --mixed
python3 ../../follow_the_money.py network_txns_fees.csv network_config_fees.json output --prefix network_fees_ --lifo
