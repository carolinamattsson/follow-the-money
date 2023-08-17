#!/bin/bash

# Reset reports
rm ./output/size_report.txt
# Limiting the size of tracked flows (my convention is to always use two decimal points for noting the smallest limit)
python3 ../../follow_the_money.py account_txns.csv account_config.json output --prefix account_sm500_ --lifo --mixed --smallest 5
