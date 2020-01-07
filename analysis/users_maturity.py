# this takes as input the depositfile.csv SORTED by the depositing user
# specifically:
# (head -1 DATE_deposits.csv && tail -n +2 DATE_deposits.csv | sort -t, -k7 -s) > DATE_deposits_byuser.csv

#######################################################################################################
def aggregate_by_user(deposit_file,user_file,issues_file):
    import traceback
    ##########################################################################################
    with open(deposit_file,'r') as deposit_file, open(user_file,'w') as user_file, open(issues_file,'w') as issues_file:
        reader_deposits    = csv.DictReader(deposit_file,delimiter=",",quotechar='"',escapechar="%")
        writer_users = csv.writer(user_file,delimiter=",",quotechar='"',escapechar="%")
        writer_issues   = csv.writer(issues_file,delimiter=",",quotechar='"',escapechar="%")
        #############################################################
        deposit_header = reader_deposits.fieldnames
        deposit_header_exits = [term for term in deposit_header if 'exit' in term]
        user_header = ['user_ID','user_type','user_deposits','user_amount','user_revenue']
        user_header = user_header + [measure+'_'+norm for measure in ['tue','prin01hr','prin03hr','prin24hr','prin72hr','p2pf'] for norm in ['bydeposit','byamount']]
        user_header = user_header + [norm+'_'+exit for exit in deposit_header_exits for norm in ['deposits','amount']]
        writer_users.writerow(user_header)
        #############################################################
        for deposits in gen_groups(reader_deposits):
            try:
                user = calculate_tux(deposits,user_header,deposit_header_exits)
                writer_users.writerow([user[term] for term in user_header])
            except:
                writer_issues.writerow([deposits[0][term] for term in deposits[0]]+[traceback.format_exc()])

def calculate_tux(deposits,user_header,deposit_header_exits):
    deposit = deposits[0]
    # parse the first deposit to get the user_ID
    user = {term:0 for term in user_header}
    user['user_ID']    = deposit['user_ID']
    user['user_type']  = {}
    for deposit in deposits:
        # convert the numerical columns to float, and get the fraction of this flow that exited as revenue
        for term in ['deposit_amt','tue','prin01hr','prin03hr','prin24hr','prin72hr','p2pf']:
            deposit[term] = float(deposit[term])
        # update the amount, revenue, deposits, and users
        user['user_deposits'] += 1
        user['user_amount'] += deposit['deposit_amt']
        user['user_revenue'] += deposit['deposit_rev']
        # keep track of the largest type
        user['user_type'].setdefault(deposit['deposit_type'],0)
        user['user_type'][deposit['deposit_type']] += 1
        # add up the TUE!
        user['tue_bydeposit'] += deposit['tue']
        user['tue_byamount']  += deposit['deposit_amt']*deposit['tue']
        # now PRIN-X!
        user['prin01hr_bydeposit'] += deposit['prin01hr']
        user['prin03hr_bydeposit'] += deposit['prin03hr']
        user['prin24hr_bydeposit'] += deposit['prin24hr']
        user['prin72hr_bydeposit'] += deposit['prin72hr']
        user['prin01hr_byamount']  += deposit['deposit_amt']*deposit['prin01hr']
        user['prin03hr_byamount']  += deposit['deposit_amt']*deposit['prin03hr']
        user['prin24hr_byamount']  += deposit['deposit_amt']*deposit['prin24hr']
        user['prin72hr_byamount']  += deposit['deposit_amt']*deposit['prin72hr']
        # now P2PF!
        user['p2pf_bydeposit'] += deposit['p2pf']
        user['p2pf_byamount']  += deposit['deposit_amt']*deposit['p2pf']
        # now exit type!
        for term in deposit_header_exits:
            if deposit[term] != '0':
                user['deposits_'+term] += float(deposit[term])/deposit['deposit_amt']
                user['amount_'+term]   += float(deposit[term])
    # get the most common deposit type
    user['user_type'] = max(user['user_type'], key=user['user_type'].get)
    # divide out by the totals to get the averages
    for term in ['tue_bydeposit','prin01hr_bydeposit','prin03hr_bydeposit','prin24hr_bydeposit','prin72hr_bydeposit','p2pf_bydeposit']:
        user[term] = user[term]/user['user_deposits']
    for term in ['tue_byamount','prin01hr_byamount','prin03hr_byamount','prin24hr_byamount','prin72hr_byamount','p2pf_byamount']:
        user[term] = user[term]/user['user_amount']
    return user

def gen_groups(deposits):
    user_deposits = []
    old_user = None
    for deposit in deposits:
        if deposit['deposit_type'] == 'inferred':
            continue
        user_ID = deposit['user_ID']
        if not old_user or old_user == user_ID:
            user_deposits.append(deposit)
            old_user = user_ID
        else:
            yield user_deposits
            del user_deposits[:]
            user_deposits = [deposit]
            old_user = user_ID
    yield user_deposits

if __name__ == '__main__':
    import os as os
    import csv

    ################## Defines the files to draw from ####################
    #path = '/Volumes/WorkWorkWork/Work/code/follow_the_money/tests/08072018/'
    #path = os.path.dirname(os.path.realpath(__file__))+'/'
    path = '/gss_gpfs_scratch/mattsson.c/Airtel/code/30072018_acttype/'
    deposit_file = path+'05092018_deposits_byuser.csv'
    user_file    = path+'05092018_users.csv'
    issues_file  = path+'05092018_users_issues.csv'
    ######################################################################

    ######### Creates weighted flow file #################
    aggregate_by_user(deposit_file,user_file,issues_file)
    #################################################
