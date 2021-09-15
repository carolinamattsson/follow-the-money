#!/bin/bash

# "untracked" funds -- trj_summarize.py --motif
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by motif

# "untracked" funds -- trj_summarize.py --length
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by length
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by length --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by length
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by length --split_by categ

# "untracked" funds -- trj_summarize.py --duration
python3 ../../../analysis/trj_summarize.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by duration --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by duration --split_by categ
python3 ../../../analysis/trj_summarize.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed_lower --split_by duration --split_by categ --lower

# "untracked" funds -- trj_durations.py --duration
python3 ../../../analysis/trj_durations.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed --split_by duration --split_by categ
python3 ../../../analysis/trj_durations.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed --split_by duration --split_by categ

# "untracked" funds -- motifs.py
python3 ../../../analysis/motifs.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed
python3 ../../../analysis/motifs.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed

# "untracked" funds -- users_savings.py
python3 ../../../analysis/users_savings.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed
python3 ../../../analysis/users_savings.py ../output/size_flows_mixed.csv ./ --prefix untracked_ --suffix _mixed
