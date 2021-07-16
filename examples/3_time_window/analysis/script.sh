#!/bin/bash

# inferred transactions -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo --split_by motif

# inferred transactions -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo --split_by length
python3 ../../../analysis/trj_summarize.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo --split_by length
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo --split_by length

# inferred transactions -- motifs.py
python3 ../../../analysis/motifs.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo
