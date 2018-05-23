#!/bin/sh
python ../main.py ./examples/ex1_input.csv ./examples/ex1_ greedy none none
python ../main.py ./examples/ex1_input.csv ./examples/ex1_ well-mixed none none
python ../main.py ./examples/ex1_input.csv ./examples/ex1_ greedy 2 none
python ../main.py ./examples/ex1_input.csv ./examples/ex1_ well-mixed 2 none
python ../main.py ./examples/ex2_input.csv ./examples/ex2_ greedy none none
python ../main.py ./examples/ex3_input.csv ./examples/ex3_ greedy none none
python ../main.py ./examples/ex3_input.csv ./examples/ex3_ greedy none infer
python ../main.py ./examples/ex3_input.csv ./examples/ex3_ greedy 2 infer
python ../main.py ./examples/ex4_input.csv ./examples/ex4_ greedy none none
python ../main.py ./examples/ex4_input.csv ./examples/ex4_ greedy none infer
python ../main.py ./examples/ex4_input.csv ./examples/ex4_ greedy 2 infer

