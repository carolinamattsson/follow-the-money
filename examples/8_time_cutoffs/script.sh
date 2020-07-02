#!/bin/bash

# Incorporating transaction fees
python3 ../../follow_the_money.py account_txns.csv account_config.json output --prefix acct_ --lifo --mixed
python3 ../../follow_the_money.py account_txns.csv account_config.json output --prefix acct_ --lifo --mixed --cutoff 72
python3 ../../follow_the_money.py network_txns.csv network_config.json output --prefix ntwk_ --lifo
python3 ../../follow_the_money.py network_txns.csv network_config.json output --prefix ntwk_ --lifo --cutoff 48
