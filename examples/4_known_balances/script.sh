#!/bin/bash

# Limiting the size of tracked flows
python3 ../../follow_the_money.py account_txns_bal.csv account_config_bal.json output --prefix bal_ --lifo --mixed
python3 ../../follow_the_money.py account_txns_bal-.csv account_config_bal.json output --prefix miss_ --lifo --mixed
