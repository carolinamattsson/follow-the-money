#!/bin/bash

# inferred transactions -- trj_summarize.py
python3 ../../../analysis/trj_summarize.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo --split_by categ --split_by motif --split_by length --split_by duration
python3 ../../../analysis/trj_summarize.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo --split_by categ --split_by motif --split_by length --split_by duration
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo --split_by categ --split_by motif --split_by length --split_by duration
# ^^ note that unknown duration is not symmetric, by design. filtering by initial deposits happening within the --timewindow from the config file is most appropriate:
python3 ../../../analysis/trj_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo_timewindow --split_by categ --split_by motif --split_by length --split_by duration --timewindow "(2019-08-18 00:00:01,)"

# inferred transactions -- acct_summarize.py
python3 ../../../analysis/acct_summarize.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo --split_by account --split_by subcateg --split_by submotif --split_by delta_t
python3 ../../../analysis/acct_summarize.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo --split_by account --split_by subcateg --split_by submotif --split_by delta_t
python3 ../../../analysis/acct_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo --split_by account --split_by subcateg --split_by submotif --split_by delta_t
# ^^ note that unknown duration is not symmetric, by design. filtering by in-transactions happening within the --timewindow from the config file is most appropriate:
python3 ../../../analysis/acct_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo_timewindow --split_by account --split_by subcateg --split_by submotif --split_by delta_t --timewindow "(2019-08-18 00:00:01,)"
# in this case, we get the same answer if we filter the trajectories. NOT SO if these trajectories include multiple steps.
python3 ../../../analysis/acct_summarize.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo_timewindow_trj --split_by account --split_by subcateg --split_by submotif --split_by delta_t --timewindow_trj "(2019-08-18 00:00:01,)"

# inferred transactions -- motifs.py
python3 ../../../analysis/motifs.py ../output/prev_flows_lifo.csv ./ --prefix prev_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/plus_flows_lifo.csv ./ --prefix plus_ --suffix _lifo
python3 ../../../analysis/motifs.py ../output/minus_flows_lifo.csv ./ --prefix minus_ --suffix _lifo
