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
 - fee (fee/revenue incurred)
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
code that `amt` `+` `fee` is to be taken from the sender and `amt` placed in the
recipient's account. The `recipient` convention means instead that `amt` is taken from
the sender and `amt` `-` `fee` is placed in the recipient's account. Some providers
may assess fees in both ways (the `split` convention) where `amt` `+` `src_fee` is taken
from the sender and `amt` `-` `tgt_fee` is placed in the recipient's account. Note that
these options all treat fees as tied to the transaction itself -- these funds never
reach the account of the recipient and are not "followed" separately. It is, of course,
entirely possible for providers to instead represent fees as separate transactions and
it is possible to pre-process data into such a form using additional assumptions. This
code does this only if the fees assessed on a recipient exceed the transaction amount,
in which case this code withdraws a separate fee from the recipient's account
immediately prior to processing the transaction in question.

If the transaction file contains information on the balance of the accounts at
the time of the transaction, you can tell the program to monitor these by putting
a `balance_type` entry in the `config.json`. This should be set to `pre` if the
balance column contains the balance of the accounts before the transaction is
processed, and `post` if after. Using this option will cause the balances in the
`src_balance` and `tgt_balance` columns to supersede the program's internal
accounting, and it will infer the existence of deposits and withdrawals that it
cannot see that would bring the balance back into line with what is given. Note that
accounting imperatives of the transaction itself would override even a given balance.

Defining the `boundary_type` of the system is vital for interpreting the output of
`follow_the_money`. Payment systems are rarely fully contained. Most allow
users of the system to deposit and withdraw from their individual accounts, letting
the total balance of the system fluctuate with use. This means that most payment
systems have a user-facing side where the movement of money is user-driven, and
a provider-facing side that accommodates users' deposits and withdraws. By defining
a system boundary, you tell the program to follow only user-driven activity.  

There are (at present) four options for defining the boundary of the system:
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
transaction category. Transaction types that are not included in the
mapping are assumed to be `system` transactions that you do not want to track.
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
the source for a transaction `type` we know to be a cash-in `deposit` or the recipient
of a transaction `type` we know to be a purchase and a point of sale. Defining
an `inferred_accounts` boundary also requires a list of account categories
(`account_following`) that will be considered user-facing accounts. However, these
categories will be inferred using the mapping (`account_categories`). Some accounts
may have multiple possible categories, and will be given the first one that appears
in the ranked list provided (`account_order`).

Sometimes, the dataset may contain *both* account and transaction information. The
`+otc` options allow for an amalgamation of the two boundaries, given that you also provide
a transaction mapping (`transaction_categories`). The results reflect that transactions
between two untracked accounts are now tracked as their category, except that their  
transaction type is given a prefix of `"OTC_"` in the output files. This is the acronym
for "over-the-counter", which is used to describe when non-users appear to be making user
transactions, possible on a user's behalf. Transaction types that do not appear in the mapping,
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

Additional options are available. You can use --help to get descriptions, and can
find a series of examples in `tests/`. These examples show how the output changes
under the available options for a simple transaction dataset reported in different ways.
