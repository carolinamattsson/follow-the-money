#!/bin/bash

# "untracked" funds -- motifs.py
python3 ../../../analysis/motifs.py ../../1_base_functionality/output/account_flows_mixed.csv ./ --prefix account_ --suffix _mixed
python3 ../../../analysis/motifs.py ../output/size_flows_mixed.csv ./ --prefix size_ --suffix _mixed
