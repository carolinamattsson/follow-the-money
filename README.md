# follow_the_money

This code turns a list of transactions from a financial ecosystem into trajectories of money through that system. These "money flows" include several possible weighting schemes and are built using explicit, modifiable, and accounting-consistent tracking heuristics.


### 1) Explore the data to create the configuration file
A `config.json` file contains everything the code needs to understand
how to read the transaction file. This includes straightforward inputs, like
the header that tells the code how to interpret the columns, and more complex
inputs like how to define the boundary of the system.
Please see the sample configuration files to clarify precise formatting.

The first thing `config.json` needs is the `transaction_header` with which to interpret
the columns of the file. The actual header in the file will be ignored in favor of this
one, which must contain specific columns that the code will know what to do with. The
incoming data needs to be ordered, and contain at least these columns:
 - txn_ID (unique ID)
 - timestamp (of some kind)
 - src/tgt_ID (sending/receiving account)
 - amt (transaction amount)

Additional columns that can be used with different variations:
 - type (transaction type)
 - src/tgt_fee (fee/revenue incurred)
 - src/tgt_categ (known account categories)
 - src/tgt_balance (known account balances)

`txn_ID`, `src_ID`, and `tgt_ID` must be hashable unique IDs. They are read as strings.
The `amt` column is converted to a float.

In order to read the `timestamp` column, the `config.json` file also needs to
contain the `timeformat`. Note that the file itself is read in the order given
(it should already be time-ordered) so the `timestamp` column is used primarily
to calculate time differences. As such, it can be inferred or coarse. Relatedly,
the `config.json` file needs to contain the `timewindow_beg` and `timewindow_end`
so that the program can account for the finite time window of the data.

If each transaction contains information on the fee or fees that users pay to use the
service (ie. the revenue the provider is generating from running the service), the
program requires a `fee/revenue` entry in the `config.json`. This entry can be set to
one of several possible accounting conventions. The `sender` convention tells the
code that `amt` `+` `src_fee` is to be taken from the sender and `amt` placed in the
recipient's account. The `recipient` convention means instead that `amt` is taken from
the sender and `amt` `-` `tgt_fee` is placed in the recipient's account. Some providers
may assess fees in both ways (the `split` convention) where `amt` `+` `src_fee` is taken
from the sender and `amt` `-` `tgt_fee` is placed in the recipient's account. Note that
these options all treat fees as tied to the transaction itself -- these funds never
reach the account of the recipient and are not "followed" separately. It is, of course,
entirely possible for providers to instead represent fees as separate transactions and
it is possible to pre-process data into such a form using additional assumptions. This
code uses this approach if the fees assessed on a recipient exceed the transaction amount,
which should be very rare if it happens at all. In such a case, this code withdraws a
separate fee from the recipient's account immediately prior to processing the transaction
in question.

If the transaction file contains information on the balance of the accounts at
the time of the transaction, you can tell the program to monitor these by putting
a `balance_type` entry in the `config.json`. This should be set to `pre` if the
balance column contains the balance of the accounts before the transaction is
processed, and `post` if after. Using this option will cause the balances in the
`src_balance` and `tgt_balance` columns to supersede the program's internal
accounting. If and when discrepancies occur, the program will infer the existence of
deposits and withdrawals enough to bring the balance back into line with what is given.
Note that accounting imperatives of the transaction itself override even a given balance.

Defining the `boundary_type` of the system is vital for interpreting the output of
`follow_the_money`. Payment systems are rarely fully contained. Most allow
users of the system to deposit and withdraw from their individual accounts, letting
the total balance of the system fluctuate with use. This means that most payment
systems have a user-facing side where the movement of money is user-driven, and
a provider-facing side that accommodates users' deposits and withdraws. By defining
a system boundary, you can tell the program to follow only user-driven activity.  

There are (at present) six options for defining the boundary of the system:
  - `none` (or left undefined)
  - `transactions`
  - `accounts`
  - `inferred_accounts`
  - `accounts+otc`
  - `inferred_accounts+otc`

Not defining a boundary, or setting `boundary_type` to `none`, will treat all
transactions as user-driven and the system as fully contained.  

In many datasets the `type` of transactions is known. This is enough to define a
network boundary if transaction types fall into specific categories: they are used
only amongst user-facing accounts (`transfer`), amongst provider-facing accounts
(`system`), or between user-facing and provider-facing accounts (`deposit` & `withdraw`).
Defining a `transactions` boundary requires a `type` column in the transaction
data, and a mapping (`transaction_categories`) from the transaction `type` to the
transaction category in the `config.json` file. Transaction types that are not included
in the mapping are assumed to be `system` transactions that you do not want to track.
Using this `boundary_type` with imperfect categories will report appropriate warnings when
the boundary appears inconsistent, such as when a `deposit` follows a `transfer`.  

In other datasets, we are provided with account categories (ex. `atm`, `user`, or
`bank`). This is enough to define a network boundary if we can cleanly say which
are user-facing and which are not. Defining an `accounts` boundary requires a
`src_categ` and a `tgt_categ` column in the transaction data, and a list of account
categories (`account_following`) that will be considered user-facing. Using this
`boundary_type` will track any transaction where one or both participants are
user-facing accounts. If a `type` column exists, however, this will still be used
in the output to describe the flows.

Some datasets conform more closely to the `accounts` logic, but we are only
given the transaction `type`. We may know that there are different account categories,
and we see them use the same transaction `type` for different purposes. It is still
possible to define a network boundary so long as there are some transaction types
that users are *not* allowed to make. For example, a user would never show up as
the source for a transaction `type` we know to be a cash `deposit` or the recipient
of a transaction `type` we know to be a purchase at a point of sale. Defining
an `inferred_accounts` boundary also requires a list of account categories
(`account_following`) that will be considered user-facing accounts. However, these
categories will be inferred using the mapping (`account_categories`). Some accounts
may have multiple possible categories, and will be given the first one that appears
in the ranked list that must be provided (`account_order`).

Sometimes, the dataset may contain *both* account and transaction information. The
`+otc` options allow for an amalgamation of the two boundaries, given that you also provide
a transaction mapping (`transaction_categories`). The results reflect that transactions
between two untracked accounts are now tracked as their category, except that their  
transaction type is given a prefix of `"OTC_"` in the output files. This is the acronym
for "over-the-counter", which is used to describe when non-users appear to be making user
transactions, possibly on a user's behalf. Transaction types that do not appear in the mapping,
or are not in one of the tracked categories (`deposit`,`transfer`,`withdraw`), remain untracked.

In all cases, the program will report untracked transactions so you can make sure
they are indeed uninteresting. But do note that it may be the case that no boundary
definition is perfect. The real world is messy, payment systems included.

### 2) Run 'follow the money' on transaction file producing a weighted flow file
```
follow_the_money.py input_file config_file output_directory --greedy --well_mixed --infer
```

This reads through the `input_file` (a `.csv`), using the interpretation detailed
in `config_file` (a `.json`), and produces three files:
 - `output_directory/wflows_greedy.csv`
 - `output_directory/wflows_well-mixed.csv`
 - `output_directory/report.csv`

The function calls the methods and functions in `initialize.py` and `follow.py` to
follow money through the user-facing system using two explicit heuristics: `--greedy`
and `--well-mixed`. Dropping either flag will avoid running with that heuristic.

If needed, the program first loops through the full data once to infer `account_categories`
and/or each account's `starting_balance`. Starting balances are the inferred minimum
balance that an account would have needed to have had at the beginning of the data
to cover the transactions that we see it make without running up a negative balance.
You can skip this step using the `--no_balance` flag, which assumes instead a
`starting_balance` of zero.

The `--infer` flag makes explicit in the output places where the program saw
changes to an account's balance with no accompanying transaction. This introduces an
inferred transaction at the beginning of the data that brings the account to it's
`starting_balance`, and one at the end that brings the account back to zero. If you
give the program balance information (a `balance_type` to interpret `src/tgt_balance`
columns), this flag will also make explicit cases where it inferred the existence of
deposits and withdrawals that it cannot see in order to bring the balance back into
line with what is given.

Additional options are available. You can use `--help` to get descriptions, and can
find a series of examples in `tests/`. These examples show how the output changes
under the available options for a simple transaction dataset reported in different ways.

### 3) Analyze the output
```
distributions.py wflows_greedy.csv output_directory
```
This script takes the output of follow-the-money, ie. of weighted flows, and reports
the distribution of their size, normalized size, and duration.
Additional options are available. You can use `--help` to get descriptions.

```
motifs.py wflows_greedy.csv output_directory --circulate 4
```
This script takes the output of follow-the-money, ie. of weighted flows, and reports
properties over observed transaction-type sequences, ie. motifs. The `--circulate`
flag consolidates motifs at and above the given length, retaining only the first
and last transaction type.
Additional options are available. You can use `--help` to get descriptions.

```
users.py wflows_greedy.csv output_directory
```
This script takes the output of follow-the-money, ie. of weighted flows, and reports
properties over observed `users`. These are accounts that have been observed within
trajectories at least once. This script reports the total amount processed by these
accounts, as well as the mean and median processing time. These measures are also
broken down by sub-motifs, meaning the in-out transaction type pattern that funds
passing through that account follow. Ex. money that enters an account as a transfer
follows a different sub-motif if it leaves as an ATM withdrawal or a payment.   
Additional options are available. You can use `--help` to get descriptions.

```
agents.py wflows_greedy.csv output_directory
```
This script takes the output of follow-the-money, ie. of weighted flows, and reports
properties over observed `agents`. These are accounts that have been observed to begin
or end trajectories at least once. This script reports the total amount for which
an account is a source or a sink, as well as the mean and median processing time.
These measures are also broken down by motif.   
Additional options are available. You can use `--help` to get descriptions.

```
length.py wflows_greedy.csv output_directory
```
This script takes the output of follow-the-money, ie. of weighted flows, and creates
a summary of the system that can be visualized as a bar-chart. Specifically, this
summary conveys how much money leaves the payment system at each step and the
transaction type through which it leaves.  
Additional options are available. You can use `--help` to get descriptions.

```
duration.py wflows_greedy.csv output_directory
```
This script takes the output of follow-the-money, ie. of weighted flows, and creates
a summary of the system that can be visualized as a bar-chart. Specifically, this
summary conveys how much money leaves the payment system each day and the
transaction type through which it leaves.   
Additional options are available. You can use `--help` to get descriptions.

### 4) Aggregate the output into entry-exit networks
```
(head -1 wflows_greedy.csv && tail -n +2 wflows_greedy.csv | sort -t, -k6 -s) > wflows_greedy_byagent.csv
```
First, sort the output of follow-the-money by the agent who stared the trajectory,
which is the entry point to the mobile money network.

```
entryexit.py wflows_greedy_byagent.csv output_directory --processes 32
```
This program aggregates trajectories into a network of entry to exit points (`network.csv`).
The weights for each network link is the sum of the amount, or deposit-normalized amount,
that moved from that entry point to that exit point via the payment system. The weight on
each link is also broken up into categories based on distance and time.

By distance:
- 0user   Funds passed directly from an entry point to an exit point (ex. over-the-counter bill payments)
- 1user   Funds passed through one user (ex. a deposit followed by a withdrawal, short-term money storage)
- 2user   Funds passed through two users (ex. a deposits, sent as a transfer, then used as a payment)
- 3+user  Funds passed through three or more users

By time:
- 0days   Funds moved instantaneously from entry point to an exit point
- 1days   Funds entered and exited the system on the same day
- 2days   Funds entered the system and then exited on the subsequent day
- 3+days  Funds remained in the system for longer

This script also creates a file of network descriptives for these accounts (`network_agents.csv`).

This aggregation is computationally intensive, and using multiple processes is suggested.

Additional options are available. You can use `--help` to get descriptions.

```
make_split_pajek.py  network.csv --split_term 0user --split_term 1user --split_term 2user,3+user
```
This python script reads the entry-exit network and splits it along the dimensions given,
creating separate networks in a condensed format, called pajek files (extension `.net`).
The example provided will create three networks, one with the instantaneous transaction
amount from one agent to another, a second with the money that is deposited at the entry
point and withdrawn at the exit, and a third with the money that experiences at least one
user-user transfer en route from entry to the exit.
By default, the edge-weight becomes the amount observed to move from entry to exit point
while the `--normalized` flag tells the code to use the deposit-normalized amount instead.
Using the `--split_type` tag, the script can also split the entry-exit network by the type
of edge, meaning the most common enter-exit transaction type combination observed between
those entry-exit points.
Without any `--split` flags, the code will create a network using the overall totals.
It is also possible to split each resulting network using a list of nodes, passed
to the `--subgraph` flag as a filename. This creates a `subgraph` network containing only the
links among these nodes and a `remgraph` network containing all remaining edges.
Additional options are available. You can use `--help` to get descriptions.

### 5) Mapping and visualization
```
nohup ./Infomap network_total_nrm.net OUTPUT_FOLDER/ -k -d -o -p 0.15 -N 4  --ftree -v >  OUTPUT_FOLDER/network_total_nrm.out
```
The pajek files (extension `.net`) can be used directly as the input to the
stand-alone C++ implementation of the Infomap algorithm, available here:
http://www.mapequation.org/code.html#Installation. Running this algorithm with
the above options simulates deposit transactions (`nrm.net`) or individual dollars
(`amt.net`) moving between agents randomly in proportion to the edge weights of
the system. With a 15% probability, at each step, we introduce some noise to the
system and the random movement begins again at an agent chosen randomly in
proportion to the actual deposits (amount deposited) they received. The result
is a 'map' of the entry-exit network with agents grouped together if deposits
(or dollars) get 'stuck' amongst them. This 'map' is fractal in nature if the
data supports it, and the `.ftree` file it produces can be interactively
viewed at: http://www.mapequation.org/apps/NetworkNavigator.html

```
make_core_gexf.py network_total_nrm.net --node_sort core_number --nodes 4000 --edge_sort noise_corrected_pct --edges 0.9
```
This python script reads a network in pajek format (extension `.net`). The script returns a `.gexf` file that can be immediately read by Gephi, free and open source network visualization software, available here: https://gephi.org/

The `--node_sort` flag must refer to a sortable property of the nodes in the
pajek file; by default this is the core_number, which is calculated within
`make_split_pajek.py`, but out_strength is also available out-of-the-box. The top
number of nodes given in `--nodes` are kept. The `--edge_sort` flag must refer
to a property returned by the `backboning.py` algorithm; by default this is
noise_corrected_pct, but a few others could be made available (see below). Agents
at or above the fraction given in `--edges` are kept.

```
backboning.py
```
This is a lightly modified version of Michele Coscia's network backboning code,
available here: http://www.michelecoscia.com/?page_id=287
The function that is called by `make_core_gexf.py` is called noise_corrected(),
offering the following options:
- weight                  The absolute link weight
- pct
- score
- score_pct
- noise_corrected
- noise_corrected_pct

It would be fairly simple to modify `make_core_gexf.py` to filter based off of
the other backboning options.
