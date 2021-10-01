#
# <data_compiler 1.0> 
# Author: Kai Bonsol
#
# This program collects poll data over time and graphs it
# with coronavirus and twitter data
#

import pandas as pd
import numpy as np
import pickle
import requests
from datetime import datetime
from urllib.error import HTTPError

def compileData(reload=False):

    if reload:
        savePollData()
        
    with open('pickles/poll_data.pickle', 'rb') as f:
        poll_df = pickle.load(f)

    #print(poll_df)
    poll_df = transformData(poll_df)

    date = datetime.now().strftime("%Y-%m-%d")
    poll_df.to_csv('poll_data/{}.csv'.format(date))
        
    

# <trasnformData>
# Average out poll ratings for one day and return a dataframe with reduced rows.
 
def transformData(poll_df):

    cur_date = poll_df.index[0]
    polls_for_day = []
    cur_sample_size = 0

    date = []
    approve = []
    disapprove = []
    sample_size = []

    for index, row in poll_df.iterrows():
        
        if index == cur_date:
            cur_sample_size += row['sample_size']
            polls_for_day.append((row['sample_size'],row['approve'], row['disapprove']))
        else:
            weighted_approve, weighted_disapprove = weightedAverage(polls_for_day, cur_sample_size)

            date.append(cur_date)
            approve.append(weighted_approve)
            disapprove.append(weighted_disapprove)
            sample_size.append(cur_sample_size)
            
            cur_date = index
            polls_for_day[:] = []
            cur_sample_size = 0
            
    confirmed_sum, death_sum, recovered_sum = aggregateCovidData(date)

    Data = {
        'date' : date,
        'approve' : approve,
        'disapprove' : disapprove,
        'sample_size' : sample_size,
        'case_count' : confirmed_sum,
        'death_count' : death_sum,
        'recovered_count' : recovered_sum
    }

    return pd.DataFrame(Data, columns=['date', 'approve', 'disapprove', 'sample_size', 'case_count', 'death_count', 'recovered_count'])  

# <weightedAverage>
# given a array of polls for a day with various sample sizes / approve / disapprove values,
# give a weighted approve & disapprove based on sample sizes
def weightedAverage(polls_for_day, sample_size):

    weighted_approve = 0.0
    weighted_disapprove = 0.0

    for poll in polls_for_day:

        cur_sample_size, approve, disapprove = poll
        weight_factor = cur_sample_size / sample_size
        weighted_approve += approve * weight_factor
        weighted_disapprove += disapprove * weight_factor

    return (weighted_approve, weighted_disapprove)
    
        
# <savePollData>
# Grab covid aproval poll data and pickle
def savePollData():

    url = 'https://raw.githubusercontent.com/fivethirtyeight/covid-19-polls/master/covid_approval_polls.csv'
    covid_approval_polls_df = pd.read_csv(url, index_col=1)
    covid_approval_polls_df.drop(columns=['start_date', 'pollster','sponsor','population', 'subject', 'tracking', 'url'], inplace=True)

    with open('pickles/poll_data.pickle', 'wb') as f:
        pickle.dump(covid_approval_polls_df, f)

def getCovidData(date):

    year, month, day = date.split('-')
    date = month + '-' + day + '-' + year

    # state dataframe
    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/' + date + '.csv'
    df = pd.read_csv(url, index_col=0)
    df.drop(columns=['Country_Region', 'Last_Update', 'FIPS', 'UID', 'ISO3'], inplace=True)
    return df

def aggregateCovidData(dates):

    confirmed_sum = []
    death_sum = []
    recovered_sum = []

    for date in dates:

        try:
            df = getCovidData(date)
            confirmed_sum.append(df['Confirmed'].sum())
            death_sum.append(df['Deaths'].sum())
            recovered_sum.append(df['Recovered'].sum())
        except HTTPError:
            confirmed_sum.append('NA')
            death_sum.append('NA')
            recovered_sum.append('NA')


    return (confirmed_sum, death_sum, recovered_sum)
    

if __name__ == '__main__':

    print('[data_compiler 1.0] starting...')
    compileData(True)
    print('[data_compiler 1.0] complete.')

        
