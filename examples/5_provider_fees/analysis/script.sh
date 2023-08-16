#!/bin/bash

# "untracked" funds -- trj_summarize.py --motif
python3 ../../../analysis/trj_summarize.py ../output/ntwk_prv_flows_lifo.csv ./ --prefix ntwk_prv_ --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/ntwk_fee_flows_lifo.csv ./ --prefix ntwk_fee_ --split_by motif

# "untracked" funds -- acct_summarize.py
python3 ../../../analysis/acct_summarize.py ../output/ntwk_prv_flows_lifo.csv ./ --prefix ntwk_prv_ --split_by account
python3 ../../../analysis/acct_summarize.py ../output/ntwk_fee_flows_lifo.csv ./ --prefix ntwk_fee_ --split_by account
