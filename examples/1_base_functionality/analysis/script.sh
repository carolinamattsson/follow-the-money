#!/bin/bash

# Base functionality -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --split_by motif --timewindow "(2017-03-01 00:00:00,2017-03-02 00:00:00)"
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _consolidate --split_by motif --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]"

# Base functionality -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --split_by length
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --split_by length --timewindow "(2017-03-01 00:00:00,2017-03-02 00:00:00)"
python3 ../../../analysis/trj_summarize.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _consolidate --split_by length --consolidate "deposit:[check_deposit,direct_deposit,cash_deposit]"

# Base functionality -- motifs.py
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --timewindow "(2017-03-01 00:00:00,2017-03-02 00:00:00)"
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _join --join "[check_deposit,direct_deposit,cash_deposit]" --name "deposit"

# Base functionality -- users_savings.py
python3 ../../../analysis/users_savings.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/users_savings.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --timewindow "(2017-03-01 00:00:00,2017-03-02 01:30:00)"
