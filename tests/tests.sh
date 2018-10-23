#!/bin/bash

# Plain vanilla follow-the-money using accounts, inferred accounts, and transactions as defining the boundary
python3 ../follow_the_money.py 1a_regular.csv 1-4a_config.json ./output --prefix 1a_ --greedy --well_mixed
python3 ../follow_the_money.py 1t_regular.csv 1-4i_config.json ./output --prefix 1i_ --greedy --well_mixed
python3 ../follow_the_money.py 1t_regular.csv 1-4t_config.json ./output --prefix 1t_ --greedy --well_mixed

# Incorporating a time cutoff, after which money stops being tracked
python3 ../follow_the_money.py 1a_regular.csv 1-4a_config.json ./output --prefix 1a_ --greedy --well_mixed --cutoff 2
python3 ../follow_the_money.py 1t_regular.csv 1-4i_config.json ./output --prefix 1i_ --greedy --well_mixed --cutoff 2
python3 ../follow_the_money.py 1t_regular.csv 1-4t_config.json ./output --prefix 1t_ --greedy --well_mixed --cutoff 2

# Incorporating fees charged to the user (ie. revenue for the provider)
python3 ../follow_the_money.py 2a_revenue.csv 1-4a_config.json ./output --prefix 2a_ --greedy --well_mixed
python3 ../follow_the_money.py 2t_revenue.csv 1-4i_config.json ./output --prefix 2i_ --greedy --well_mixed
python3 ../follow_the_money.py 2t_revenue.csv 1-4t_config.json ./output --prefix 2t_ --greedy --well_mixed

# Incorporating the handling of finite data - existing balance can be inferred or not
python3 ../follow_the_money.py 3a_finite.csv 1-4a_config.json ./output --prefix 3a_ --greedy --well_mixed --no_balance
python3 ../follow_the_money.py 3t_finite.csv 1-4i_config.json ./output --prefix 3i_ --greedy --well_mixed --no_balance
python3 ../follow_the_money.py 3t_finite.csv 1-4t_config.json ./output --prefix 3t_ --greedy --well_mixed --no_balance
python3 ../follow_the_money.py 3a_finite.csv 1-4a_config.json ./output --prefix 3a_ --greedy --well_mixed
python3 ../follow_the_money.py 3t_finite.csv 1-4i_config.json ./output --prefix 3i_ --greedy --well_mixed
python3 ../follow_the_money.py 3t_finite.csv 1-4t_config.json ./output --prefix 3t_ --greedy --well_mixed

# Incorporating the handling of finite data - unseen transactions can also be explicitly inferred
python3 ../follow_the_money.py 3a_finite.csv 1-4a_config.json ./output --prefix 3a_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 3t_finite.csv 1-4i_config.json ./output --prefix 3i_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 3t_finite.csv 1-4t_config.json ./output --prefix 3t_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 3a_finite.csv 1-4a_config.json ./output --prefix 3a_ --greedy --well_mixed --cutoff 2 --infer
python3 ../follow_the_money.py 3t_finite.csv 1-4i_config.json ./output --prefix 3i_ --greedy --well_mixed --cutoff 2 --infer
python3 ../follow_the_money.py 3t_finite.csv 1-4t_config.json ./output --prefix 3t_ --greedy --well_mixed --cutoff 2 --infer

# Illustrate the handling of evident inconsistencies in the network boundary or transactions outside the boundary
python3 ../follow_the_money.py 4a_boundary.csv 1-4a_config.json ./output --prefix 4a_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary.csv 1-4i_config.json ./output --prefix 4i_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary.csv 1-4t_config.json ./output --prefix 4t_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary.csv 1-4i+_config.json ./output --prefix 4i+_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary2.csv 1-4i_config.json ./output --prefix 4i_2_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary2.csv 1-4t_config.json ./output --prefix 4t_2_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary2.csv 1-4i+_config.json ./output --prefix 4i+_2_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary3.csv 1-4i_config.json ./output --prefix 4i_3_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary3.csv 1-4t_config.json ./output --prefix 4t_3_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 4t_boundary3.csv 1-4i+_config.json ./output --prefix 4i+_3_ --greedy --well_mixed --infer

# Plain vanilla follow-the-money where the pre-transaction balance of accounts is provided
python3 ../follow_the_money.py 5a_balance.csv 5-6a_config.json ./output --prefix 5a_ --greedy --well_mixed
python3 ../follow_the_money.py 5t_balance.csv 5-6i_config.json ./output --prefix 5i_ --greedy --well_mixed
python3 ../follow_the_money.py 5t_balance.csv 5-6t_config.json ./output --prefix 5t_ --greedy --well_mixed

# Illustrate the handling of evident inconsistencies in the balances provided
python3 ../follow_the_money.py 6a_missing.csv 5-6a_config.json ./output --prefix 6a_ --greedy --well_mixed
python3 ../follow_the_money.py 6t_missing.csv 5-6i_config.json ./output --prefix 6i_ --greedy --well_mixed
python3 ../follow_the_money.py 6t_missing.csv 5-6t_config.json ./output --prefix 6t_ --greedy --well_mixed
python3 ../follow_the_money.py 6a_missing.csv 5-6a_config.json ./output --prefix 6a_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 6t_missing.csv 5-6i_config.json ./output --prefix 6i_ --greedy --well_mixed --infer
python3 ../follow_the_money.py 6t_missing.csv 5-6t_config.json ./output --prefix 6t_ --greedy --well_mixed --infer
