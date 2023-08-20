#!/bin/bash

# Reset reports
rm ./output/network_txn_report.txt
rm ./output/network_act_report.txt
rm ./output/network_inf_report.txt
# Defining the boundary of the system
python3 ../../follow_the_money.py network_txns_t.csv network_config_t.json output --prefix network_txn_ --lifo
python3 ../../follow_the_money.py network_txns_a.csv network_config_a.json output --prefix network_act_ --lifo
python3 ../../follow_the_money.py network_txns_t.csv network_config_i.json output --prefix network_inf_ --lifo
