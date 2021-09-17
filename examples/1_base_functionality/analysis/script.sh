#!/bin/bash

# Base functionality -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by length
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _max --split_by length --max_transfers 0
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by motif --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]"
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by interval --cutoffs "[0,1,48]"
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --duration
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --duration --split_by motif --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]" --split_by categ --split_by length --max_transfers 0 --split_by interval --cutoffs "[0,1,48]"

# Base functionality -- acct_summarize.py
python3 ../../../analysis/acct_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/acct_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by account
python3 ../../../analysis/acct_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by submotif --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]" --split_by subcateg
python3 ../../../analysis/acct_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by interval --cutoffs "[0,1,48]"
python3 ../../../analysis/acct_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --delta_t
python3 ../../../analysis/acct_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --delta_t --split_by account --split_by submotif --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]" --split_by subcateg --split_by interval --cutoffs "[0,1,48]"

# Base functionality -- motifs.py
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _join --join "[check_deposit,direct_deposit,cash_deposit]" --name "deposit"

# Base functionality -- users_savings.py
python3 ../../../analysis/users_savings.py ../output/network_flows_lifo.csv ./ --prefix network_
