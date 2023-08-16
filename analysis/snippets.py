###########################################################################################
def months_duration(month_list):
    '''
    This gives the total duration for each month (%Y-%m) in the given list.
    '''
    months_dur = []
    for yearmonth in month_list:
        year, month = yearmonth.split('-')
        if month in ['01','03','05','07','08','10','12']:
            months_dur.append(31*24)
        elif month in ['04','06','09','11']:
            months_dur.append(30*24)
        elif month=='02':
            if not int(year)%100:
                months_dur.append(29*24 if int(year)%400 else 28*24)
            else:
                months_dur.append(29*24 if int(year)%4 else 28*24)
    return months_dur
