#!/bin/bash

# transform the raw output into a format that can be imported into Pandas
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/account_pairs_lifo.csv ./ --prefix account_ --suffix _lifo --column all
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/account_pairs_mixed.csv ./ --prefix account_ --suffix _mixed --column all
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_pairs_lifo.csv ./ --prefix network_ --suffix _lifo --column all
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_pairs_mixed.csv ./ --prefix network_ --suffix _mixed --column all
# also for the network with a biting smallest limit and ending timewindow 
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_sm30000_pairs_mixed.csv ./ --prefix network_ --suffix _mixed_sm30000_min_unobs --column all --unobserveds
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_sm30000_pairs_mixed.csv ./ --prefix network_ --suffix _mixed_sm30000_max_unobs --column all --timewindow_beg "2017-03-01 00:00:00" --unobserveds
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_sm30000_pairs_mixed.csv ./ --prefix network_ --suffix _mixed_sm30000_end_unobs --column all --timewindow_beg "2017-03-01 00:00:00" --timewindow_end "2017-03-04 00:00:00" --unobserveds
# now with the default filtering
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_sm30000_pairs_mixed.csv ./ --prefix network_ --suffix _mixed_sm30000_min --column all
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_sm30000_pairs_mixed.csv ./ --prefix network_ --suffix _mixed_sm30000_max --column all --timewindow_beg "2017-03-01 00:00:00"
python3 ../../../analysis/trj_durations.py ../../9_pairwise/output/network_sm30000_pairs_mixed.csv ./ --prefix network_ --suffix _mixed_sm30000_end --column all --timewindow_beg "2017-03-01 00:00:00" --timewindow_end "2017-03-04 00:00:00"
