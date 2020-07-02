#!/bin/bash

# Incorporating transaction fees
python3 ../../follow_the_money.py account_txns.csv      account_config.json output --prefix acct_prv_ --lifo --mixed
python3 ../../follow_the_money.py account_txns_fees.csv account_config.json output --prefix acct_fee_ --lifo --mixed
python3 ../../follow_the_money.py network_txns.csv      network_config.json output --prefix ntwk_prv_ --lifo
python3 ../../follow_the_money.py network_txns_fees.csv network_config.json output --prefix ntwk_fee_ --lifo
