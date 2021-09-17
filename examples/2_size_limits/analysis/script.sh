#!/bin/bash

# "untracked" funds -- trj_summarize.py --split_by motif
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by motif --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by motif --split_by categ

# "untracked" funds -- trj_summarize.py --split_by length
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by length --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by length --split_by categ

# "untracked" funds -- trj_summarize.py --split_by interval
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by interval --cutoffs "[0,1,24]" --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by interval --cutoffs "[0,1,24]" --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed_upper --split_by interval --cutoffs "[0,1,24]" --split_by categ --upper

# "untracked" funds -- trj_durations.py --duration
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --duration --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --duration --split_by categ

# "untracked" funds -- acct_summarize.py --split_by categ
python3 ../../../analysis/acct_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by subcateg
python3 ../../../analysis/acct_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by subcateg

# "untracked" funds -- acct_summarize.py --delta_t
python3 ../../../analysis/acct_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --delta_t --split_by subcateg
python3 ../../../analysis/acct_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --delta_t --split_by subcateg

# "untracked" funds -- motifs.py
python3 ../../../analysis/motifs.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed
python3 ../../../analysis/motifs.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed

# "untracked" funds -- users_savings.py
python3 ../../../analysis/users_savings.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed
python3 ../../../analysis/users_savings.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed
