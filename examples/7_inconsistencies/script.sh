#!/bin/bash

# Defining the boundary of an inconsistent system
python3 ../../follow_the_money.py network_txns_t.csv network_config_t.json output --prefix prev_ --lifo
python3 ../../follow_the_money.py network_txns_otc.csv network_config_t.json output --prefix otc_t_ --lifo
python3 ../../follow_the_money.py network_txns_a_otc.csv network_config_a.json output --prefix otc_a_ --lifo
python3 ../../follow_the_money.py network_txns_a_otc.csv network_config_a+.json output --prefix otc_a+_ --lifo
python3 ../../follow_the_money.py network_txns_otc.csv network_config_i.json output --prefix otc_i_ --lifo
python3 ../../follow_the_money.py network_txns_otc.csv network_config_i+.json output --prefix otc_i+_ --lifo
python3 ../../follow_the_money.py network_txns_inc.csv network_config_t.json output --prefix inc_t_ --lifo
python3 ../../follow_the_money.py network_txns_inc.csv network_config_i.json output --prefix inc_i_ --lifo
python3 ../../follow_the_money.py network_txns_inc.csv network_config_i+.json output --prefix inc_i+_ --lifo
