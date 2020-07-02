#!/bin/bash

# Defining the boundary of the system
python3 ../../follow_the_money.py network_txns_t.csv network_config_t.json output --prefix txn_ --lifo
python3 ../../follow_the_money.py network_txns_a.csv network_config_a.json output --prefix act_ --lifo
python3 ../../follow_the_money.py network_txns_t.csv network_config_i.json output --prefix inf_ --lifo
