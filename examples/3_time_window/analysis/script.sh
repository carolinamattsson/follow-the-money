#!/bin/bash

# Base functionality
python3 ../../../analysis/motifs.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo
