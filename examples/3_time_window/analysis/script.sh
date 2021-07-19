#!/bin/bash

# inferred transactions -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo --split_by motif
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo --split_by motif

# inferred transactions -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo --split_by length
python3 ../../../analysis/trj_summarize.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo --split_by length
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo --split_by length

# inferred transactions -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo --split_by duration
python3 ../../../analysis/trj_summarize.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo --split_by duration
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo --split_by duration
# ^^ note that the output is not symmatric, by design. filtering inferred initial deposits with the --timewindow from the config file is most appropriate:
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo_timewindow --split_by duration --timewindow "(2019-08-18 00:00:01,)"

# inferred transactions -- motifs.py
python3 ../../../analysis/motifs.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo
