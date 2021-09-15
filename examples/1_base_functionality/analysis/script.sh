#!/bin/bash

# Base functionality -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --timewindow "(2017-03-01 00:00:00,2017-03-02 00:00:00)"
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by length
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _max --split_by length --max_transfers 0
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _consolidate --split_by motif --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]"
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _consolidate_max --split_by motif --max_transfers 0 --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]"
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by duration
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _cutoffs --split_by duration --cutoffs "[1,24,48]"

# Base functionality -- trj_durations.py
python3 ../../../analysis/trj_durations.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/trj_durations.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _splitby --split_by duration --cutoffs "[1,24,48]" --split_by motif --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]"

# Base functionality -- motifs.py
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --timewindow "(2017-03-01 00:00:00,2017-03-02 00:00:00)"
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _join --join "[check_deposit,direct_deposit,cash_deposit]" --name "deposit"

# Base functionality -- users_savings.py
python3 ../../../analysis/users_savings.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/users_savings.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --timewindow "(2017-03-01 00:00:00,2017-03-02 01:30:00)"
