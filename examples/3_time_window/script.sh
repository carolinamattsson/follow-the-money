#!/bin/bash

# Limiting the size of tracked flows
python3 ../../follow_the_money.py account_txns+.csv account_config.json output --prefix prev_ --lifo --mixed
python3 ../../follow_the_money.py account_txns+.csv account_config+.json output --prefix plus_ --lifo --mixed
python3 ../../follow_the_money.py account_txns+.csv account_config-.json output --prefix minus_ --lifo --mixed