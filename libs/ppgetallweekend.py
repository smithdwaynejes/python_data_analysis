from datetime import date, timedelta,datetime

# def allweekends(year):
# # January 1st of the given year
	# dt = date(year, 1, 1)
# # First Saturday of the given year	
	# dt += timedelta(days = 5 - dt.weekday())  

	# print(dt.year)
	# print(year)
	# while dt.year == year:

	   # yield dt					
	   # dt += timedelta(days = 1)	# Next Sunday
	   
	   # yield dt
	   # dt += timedelta(days = 6)
	   
	   


import pandas as pd

def get_week_ends(year,freq='W-SUN'):
    return pd.date_range(start=str(year), end=str(year+1), 
                         freq=freq).strftime('%m%d%Y').tolist() 

def get_all_week_ends(year):
	# dates = 
	dates_1 = [(datetime.strptime(d, '%m%d%Y').date()) for d in get_week_ends(year)+get_week_ends(year,'W-SAT')]
	dates_1.sort()
	return [i.strftime('%m%d%Y') for i in dates_1]
	# return dates


