#!/bin/bash

# Base functionality -- motifs.py
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --timewindow "(2017-03-01 00:00:00,2017-03-02 00:00:00)"
python3 ../../../analysis/motifs.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _join --join "[check_deposit,direct_deposit,cash_deposit]" --name "deposit"

# Base functionality -- users_savings.py
python3 ../../../analysis/users_savings.py ../output/network_flows_lifo.csv ./ --prefix network_
python3 ../../../analysis/users_savings.py ../output/network_flows_lifo.csv ./ --prefix network_ --suffix _timewindow --timewindow "(2017-03-01 00:00:00,2017-03-02 01:30:00)"
