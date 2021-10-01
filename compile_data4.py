## <compile_data4.py>
## This program takes two dates and aggregates the data across different times
## Average for some values, sum for others
## Create many dataframes for each state, this should be done once a week?
## New aggregator can be made for weeks, or this can be recursively done?

import os
import os.path
import pandas as pd
pd.options.mode.chained_assignment = None
import pickle
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib import style

def getTimeIndices():
    with open('pickles/time_index_data.pickle', 'rb') as f:
        time_index_data = pickle.load(f)

    return time_index_data

def getAbbrevState():
    with open('pickles/abrv_state.pickle', 'rb') as f:
        abrv_state = pickle.load(f)

    return abrv_state
    

def aggregateDataByState(state, start, end):
    
    time_index_data = getTimeIndices()

    start_index = 0
    end_index = 0
    cur_index = 0
    
    for time_index in time_index_data:

        if time_index[0] == start:
            start_index = cur_index
        elif time_index[0] == end:
            end_index = cur_index

        cur_index += 1

    date = []
    num_tweets = []
    case_count = []
    cases_per_day = []
    deaths = []
    acutely_ill = []
    hospitalized = []
    recovered = []
    positive_tests = []
    negative_tests = []
    total_tests = []
    death_rate = []
    hospitalization_rate = []
    
    for time_index in time_index_data[start_index:end_index+1]:

        date.append(time_index[0])
        df_int = pd.read_csv('covid_data_by_day/covid_data_by_state_{}.csv'.format(time_index[0]))
        df_int.set_index('State', inplace=True)
        # access like - df_int['Num_Tweets']['AK']

        num_tweets.append(df_int['Num_Tweets'][state])
        case_count.append(df_int['Case_Count'][state])
        cases_per_day.append(df_int['Cases_Per_Day'][state])
        deaths.append(df_int['Deaths'][state])
        acutely_ill.append(df_int['Acutely_Ill'][state])
        hospitalized.append(df_int['Hospitalized'][state])
        recovered.append(df_int['Recovered'][state])
        positive_tests.append(df_int['Positive_Tests'][state])
        negative_tests.append(df_int['Negative_Tests'][state])
        total_tests.append(df_int['Total_Tests'][state])
        death_rate.append(df_int['Death_Rate'][state])
        hospitalization_rate.append(df_int['Hospitalization_Rate'][state])

    Data = {
        'Date' : date,
        'Case_Count' : case_count,
        'Num_Tweets' : num_tweets,
        'Cases_Per_Day' : cases_per_day,
        'Deaths' : deaths,
        'Acutely_Ill' : acutely_ill,
        'Hospitalized' : hospitalized,
        'Recovered' : recovered,
        'Positive_Tests' : positive_tests,
        'Negative_Tests' : negative_tests,
        'Total_Tests' : total_tests,
        'Death_Rate' : death_rate,
        'Hospitalization_Rate' : hospitalization_rate
    }

    df = pd.DataFrame(Data, columns=['Date', 'Case_Count', 'Num_Tweets', 'Cases_Per_Day', 'Deaths', 'Acutely_Ill', 'Hospitalized', 'Recovered', 'Positive_Tests', 'Negative_Tests', 'Total_Tests',
                                          'Death_Rate', 'Hospitalization_Rate'])

    if not os.path.exists('covid_data_by_day/aggregated_data/states/{}'.format(state)):
        os.makedirs('covid_data_by_day/aggregated_data/states/{}'.format(state))
    
    df.to_csv('covid_data_by_day/aggregated_data/states/{}/{}to{}.csv'.format(state, start, end))

def aggregateAllStateData(start, end):

    with open('pickles/abrv_state.pickle', 'rb') as f:
        abrv_state = pickle.load(f)

    for state in abrv_state:
        aggregateDataByState(state, start, end)

def aggregateAllDataUSA(start, end):

    time_index_data = getTimeIndices()

    start_index = 0
    end_index = 0
    cur_index = 0
    
    for time_index in time_index_data:

        if time_index[0] == start:
            start_index = cur_index
        elif time_index[0] == end:
            end_index = cur_index

        cur_index += 1

    date = []
    processed_tweets = []
    num_tweets = []
    case_count = []
    cases_per_day = []
    deaths = []
    acutely_ill = []
    hospitalized = []
    recovered = []
    positive_tests = []
    negative_tests = []
    total_tests = []
    death_rate = []
    hospitalization_rate = []

    abrv_state = getAbbrevState()
    
    for time_index in time_index_data[start_index:end_index+1]:

        date.append(time_index[0])
        processed_tweets.append(time_index[1])
        
        df_int = pd.read_csv('covid_data_by_day/covid_data_by_state_{}.csv'.format(time_index[0]))
        df_int.set_index('State', inplace=True)
        # access like - df_int['Num_Tweets']['AK']

        total = 1

        num_tweet_average = df_int['Num_Tweets']['AL']
        case_count_average = df_int['Case_Count']['AL']
        cases_per_day_average = df_int['Cases_Per_Day']['AL']
        death_count_average = df_int['Deaths']['AL']
        acutely_ill_average = df_int['Acutely_Ill']['AL']
        hospitalized_average = df_int['Hospitalized']['AL']
        recovered_average = df_int['Recovered']['AL']
        positive_tests_average = df_int['Positive_Tests']['AL']
        negative_tests_average = df_int['Negative_Tests']['AL']
        total_tests_average = df_int['Total_Tests']['AL']
        death_rate_average = df_int['Death_Rate']['AL']
        hospitalization_rate_average = df_int['Hospitalization_Rate']['AL']

        for state in abrv_state:
            
            if state == 'AL':
                continue

            total += 1
            
            num_tweet_average += (df_int['Num_Tweets'][state] - num_tweet_average) / total
            case_count_average += (df_int['Case_Count'][state] - case_count_average) / total
            cases_per_day_average += (df_int['Cases_Per_Day'][state] - cases_per_day_average) / total
            death_count_average += (df_int['Deaths'][state] - death_count_average) / total

            #if NA
            #print(df_int['Acutely_Ill'][state])
            if not pd.isna(df_int['Acutely_Ill'][state]):
                acutely_ill_average += (df_int['Acutely_Ill'][state] - acutely_ill_average) / total
            if not pd.isna(df_int['Hospitalized'][state]):
                hospitalized_average += (df_int['Hospitalized'][state] - hospitalized_average) / total
            if not pd.isna(df_int['Recovered'][state]):
                recovered_average += (df_int['Recovered']['AL'] - recovered_average) / total
            
            positive_tests_average += (df_int['Positive_Tests'][state] - positive_tests_average) / total
            negative_tests_average += (df_int['Negative_Tests'][state] - negative_tests_average) / total
            total_tests_average += (df_int['Total_Tests'][state] - total_tests_average) / total
            death_rate_average += (df_int['Death_Rate'][state] - death_rate_average) / total

            # if NA
            if not pd.isna(df_int['Hospitalization_Rate'][state]):
                hospitalization_rate_average += (df_int['Hospitalization_Rate'][state] - hospitalization_rate_average) / total
            
        num_tweets.append(num_tweet_average)
        case_count.append(case_count_average)
        cases_per_day.append(cases_per_day_average)
        deaths.append(death_count_average)
        acutely_ill.append(acutely_ill_average)
        hospitalized.append(hospitalized_average)
        recovered.append(recovered_average)
        positive_tests.append(positive_tests_average)
        negative_tests.append(negative_tests_average)
        total_tests.append(total_tests_average)
        death_rate.append(death_rate_average)
        hospitalization_rate.append(hospitalization_rate_average)

    Data = {
        'Date' : date,
        'Processed_Tweets' : processed_tweets,
        'Case_Count' : case_count,
        'Num_Tweets' : num_tweets,
        'Cases_Per_Day' : cases_per_day,
        'Deaths' : deaths,
        'Acutely_Ill' : acutely_ill,
        'Hospitalized' : hospitalized,
        'Recovered' : recovered,
        'Positive_Tests' : positive_tests,
        'Negative_Tests' : negative_tests,
        'Total_Tests' : total_tests,
        'Death_Rate' : death_rate,
        'Hospitalization_Rate' : hospitalization_rate
    }

    df = pd.DataFrame(Data, columns=['Date', 'Processed_Tweets', 'Case_Count', 'Num_Tweets', 'Cases_Per_Day', 'Deaths', 'Acutely_Ill', 'Hospitalized', 'Recovered', 'Positive_Tests', 'Negative_Tests', 'Total_Tests',
                                          'Death_Rate', 'Hospitalization_Rate'])

    df.to_csv('covid_data_by_day/aggregated_data/USA{}to{}.csv'.format(start, end))

# average columns for a STATE
# case_count average doesn't matter - % increase instead,
# over a time interval, but data not dependent on time

def aggregateAllDataAverage(start, end):

    time_index_data = getTimeIndices()

    start_index = 0
    end_index = 0
    cur_index = 0
    
    for time_index in time_index_data:

        if time_index[0] == start:
            start_index = cur_index
        elif time_index[0] == end:
            end_index = cur_index

        cur_index += 1

    states = []
    num_tweets = []
    case_count = []
    cases_per_day = []
    deaths = []
    acutely_ill = []
    hospitalized = []
    recovered = []
    positive_tests = []
    negative_tests = []
    total_tests = []
    death_rate = []
    hospitalization_rate = []

    abrv_state = getAbbrevState()

    for state in abrv_state:

        total = 1
        df_int = pd.read_csv('covid_data_by_day/covid_data_by_state_{}.csv'.format(start))
        df_int.set_index('State', inplace=True)

        states.append(state)
        num_tweet_average = df_int['Num_Tweets'][state]
        case_count_average = df_int['Case_Count'][state]
        cases_per_day_average = df_int['Cases_Per_Day'][state]
        death_count_average = df_int['Deaths'][state]
        acutely_ill_average = df_int['Acutely_Ill'][state]
        hospitalized_average = df_int['Hospitalized'][state]
        recovered_average = df_int['Recovered'][state]
        positive_tests_average = df_int['Positive_Tests'][state]
        negative_tests_average = df_int['Negative_Tests'][state]
        total_tests_average = df_int['Total_Tests'][state]
        death_rate_average = df_int['Death_Rate'][state]
        hospitalization_rate_average = df_int['Hospitalization_Rate'][state]        

        
        for time_index in time_index_data[start_index+1:end_index+1]:

            df_int = pd.read_csv('covid_data_by_day/covid_data_by_state_{}.csv'.format(time_index[0]))
            df_int.set_index('State', inplace=True)
            # access like - df_int['Num_Tweets']['AK']

            total += 1
            
            num_tweet_average += (df_int['Num_Tweets'][state] - num_tweet_average) / total
            case_count_average += (df_int['Case_Count'][state] - case_count_average) / total
            cases_per_day_average += (df_int['Cases_Per_Day'][state] - cases_per_day_average) / total
            death_count_average += (df_int['Deaths'][state] - death_count_average) / total

            #if NA
            #print(df_int['Acutely_Ill'][state])
            if not pd.isna(df_int['Acutely_Ill'][state]):
                acutely_ill_average += (df_int['Acutely_Ill'][state] - acutely_ill_average) / total
            if not pd.isna(df_int['Hospitalized'][state]):
                hospitalized_average += (df_int['Hospitalized'][state] - hospitalized_average) / total
            if not pd.isna(df_int['Recovered'][state]):
                recovered_average += (df_int['Recovered']['AL'] - recovered_average) / total
            
            positive_tests_average += (df_int['Positive_Tests'][state] - positive_tests_average) / total
            negative_tests_average += (df_int['Negative_Tests'][state] - negative_tests_average) / total
            total_tests_average += (df_int['Total_Tests'][state] - total_tests_average) / total
            death_rate_average += (df_int['Death_Rate'][state] - death_rate_average) / total

            # if NA
            if not pd.isna(df_int['Hospitalization_Rate'][state]):
                hospitalization_rate_average += (df_int['Hospitalization_Rate'][state] - hospitalization_rate_average) / total
            
        num_tweets.append(num_tweet_average)
        case_count.append(case_count_average)
        cases_per_day.append(cases_per_day_average)
        deaths.append(death_count_average)
        acutely_ill.append(acutely_ill_average)
        hospitalized.append(hospitalized_average)
        recovered.append(recovered_average)
        positive_tests.append(positive_tests_average)
        negative_tests.append(negative_tests_average)
        total_tests.append(total_tests_average)
        death_rate.append(death_rate_average)
        hospitalization_rate.append(hospitalization_rate_average)

    Data = {
        'State' : states,
        'Case_Count' : case_count,
        'Num_Tweets' : num_tweets,
        'Cases_Per_Day' : cases_per_day,
        'Deaths' : deaths,
        'Acutely_Ill' : acutely_ill,
        'Hospitalized' : hospitalized,
        'Recovered' : recovered,
        'Positive_Tests' : positive_tests,
        'Negative_Tests' : negative_tests,
        'Total_Tests' : total_tests,
        'Death_Rate' : death_rate,
        'Hospitalization_Rate' : hospitalization_rate
    }

    df = pd.DataFrame(Data, columns=['State', 'Case_Count', 'Num_Tweets', 'Cases_Per_Day', 'Deaths', 'Acutely_Ill', 'Hospitalized', 'Recovered', 'Positive_Tests', 'Negative_Tests', 'Total_Tests',
                                          'Death_Rate', 'Hospitalization_Rate'])

    df.to_csv('covid_data_by_day/aggregated_data/average{}to{}.csv'.format(start, end))


# this function aggregates the data, finding percent increases / decreases in all variables
def aggregateAllDataMarginal(start, end):

    time_index_data = getTimeIndices()

    start_index = 0
    end_index = 0
    cur_index = 0
    
    for time_index in time_index_data:

        if time_index[0] == start:
            start_index = cur_index
        elif time_index[0] == end:
            end_index = cur_index

        cur_index += 1

    states = []

    # all of these are now increase, a negative number means a decrease
    # also going to graph the marginal rates-- structure:

    # prev -> first row
    # cur -> second row
    # marginal var = curent - prev
    # %increase = (marginal var) / prev
    
    num_tweets = []
    case_count = []
    cases_per_day = []
    deaths = []
    acutely_ill = []
    hospitalized = []
    recovered = []
    positive_tests = []
    negative_tests = []
    total_tests = []
    death_rate = []
    hospitalization_rate = []

    abrv_state = getAbbrevState()

    for state in abrv_state:

        total = 1
        df_int = pd.read_csv('covid_data_by_day/covid_data_by_state_{}.csv'.format(start))
        df_int.set_index('State', inplace=True)

        states.append(state)
        num_tweet_average = df_int['Num_Tweets'][state]
        case_count_average = df_int['Case_Count'][state]
        cases_per_day_average = df_int['Cases_Per_Day'][state]
        death_count_average = df_int['Deaths'][state]
        acutely_ill_average = df_int['Acutely_Ill'][state]
        hospitalized_average = df_int['Hospitalized'][state]
        recovered_average = df_int['Recovered'][state]
        positive_tests_average = df_int['Positive_Tests'][state]
        negative_tests_average = df_int['Negative_Tests'][state]
        total_tests_average = df_int['Total_Tests'][state]
        death_rate_average = df_int['Death_Rate'][state]
        hospitalization_rate_average = df_int['Hospitalization_Rate'][state]        

        
        for time_index in time_index_data[start_index+1:end_index+1]:

            df_int = pd.read_csv('covid_data_by_day/covid_data_by_state_{}.csv'.format(time_index[0]))
            df_int.set_index('State', inplace=True)
            # access like - df_int['Num_Tweets']['AK']

            total += 1
            
            num_tweet_average += (df_int['Num_Tweets'][state] - num_tweet_average) / total
            case_count_average += (df_int['Case_Count'][state] - case_count_average) / total
            cases_per_day_average += (df_int['Cases_Per_Day'][state] - cases_per_day_average) / total
            death_count_average += (df_int['Deaths'][state] - death_count_average) / total

            #if NA
            #print(df_int['Acutely_Ill'][state])
            if not pd.isna(df_int['Acutely_Ill'][state]):
                acutely_ill_average += (df_int['Acutely_Ill'][state] - acutely_ill_average) / total
            if not pd.isna(df_int['Hospitalized'][state]):
                hospitalized_average += (df_int['Hospitalized'][state] - hospitalized_average) / total
            if not pd.isna(df_int['Recovered'][state]):
                recovered_average += (df_int['Recovered']['AL'] - recovered_average) / total
            
            positive_tests_average += (df_int['Positive_Tests'][state] - positive_tests_average) / total
            negative_tests_average += (df_int['Negative_Tests'][state] - negative_tests_average) / total
            total_tests_average += (df_int['Total_Tests'][state] - total_tests_average) / total
            death_rate_average += (df_int['Death_Rate'][state] - death_rate_average) / total

            # if NA
            if not pd.isna(df_int['Hospitalization_Rate'][state]):
                hospitalization_rate_average += (df_int['Hospitalization_Rate'][state] - hospitalization_rate_average) / total
            
        num_tweets.append(num_tweet_average)
        case_count.append(case_count_average)
        cases_per_day.append(cases_per_day_average)
        deaths.append(death_count_average)
        acutely_ill.append(acutely_ill_average)
        hospitalized.append(hospitalized_average)
        recovered.append(recovered_average)
        positive_tests.append(positive_tests_average)
        negative_tests.append(negative_tests_average)
        total_tests.append(total_tests_average)
        death_rate.append(death_rate_average)
        hospitalization_rate.append(hospitalization_rate_average)

    Data = {
        'State' : states,
        'Case_Count' : case_count,
        'Num_Tweets' : num_tweets,
        'Cases_Per_Day' : cases_per_day,
        'Deaths' : deaths,
        'Acutely_Ill' : acutely_ill,
        'Hospitalized' : hospitalized,
        'Recovered' : recovered,
        'Positive_Tests' : positive_tests,
        'Negative_Tests' : negative_tests,
        'Total_Tests' : total_tests,
        'Death_Rate' : death_rate,
        'Hospitalization_Rate' : hospitalization_rate
    }

    df = pd.DataFrame(Data, columns=['State', 'Case_Count', 'Num_Tweets', 'Cases_Per_Day', 'Deaths', 'Acutely_Ill', 'Hospitalized', 'Recovered', 'Positive_Tests', 'Negative_Tests', 'Total_Tests',
                                          'Death_Rate', 'Hospitalization_Rate'])

    df.to_csv('covid_data_by_day/aggregated_data/average{}to{}.csv'.format(start, end))

if __name__ == '__main__':

    start = '07_13_2020'
    end = '07_17_2020'
    #state = 'AL'

    #aggregateAllData(start, end)
