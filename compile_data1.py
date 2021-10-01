## <compile_data1.py>
## Author: Kai Bonsol
## This program will compile data into a more suitable form
## for representing with R
##
##

import os
import os.path
import pandas as pd
pd.options.mode.chained_assignment = None
import pickle
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib import style
import compile_data2

def pickleTimeIndexData():

    time_index_data = [('07_13_2020', 495), ('07_14_2020', 503), ('07_15_2020', 276), ('07_16_2020', 359)]
    with open('pickles/time_index_data.pickle', 'wb') as f:
        pickle.dump(time_index_data, f)

def getCountyData():

    with open("pickles/county_data.pickle", "rb") as f:
        county_data = pickle.load(f)

    return county_data

def visualizeData(file_name, date):

    style.use('ggplot')

    df = pd.read_csv('covid_data_by_day/{}.csv'.format(file_name))
    ax = df.plot(x = 'Case_Count', y = 'Num_Tweets', kind = 'scatter', title='COVID19 Case Count to COVID19 Tweet Count')
    a = pd.concat({'Case_Count' : df['Case_Count'], 'Num_Tweets' : df['Num_Tweets'], 'State' : df['State']}, axis=1)

    for i, point in a.iterrows():
        ax.text(point['Case_Count'], point['Num_Tweets'], str(point['State']))
    #df.plot(x = 'Case_Count', y = 'Num_Tweets', kind = 'line')

    fig1 = plt.gcf()
    plt.show()
    # only save data with states.
    #fig1.savefig('covid_data_by_day/{}.png'.format(date))


# <reformatCountyData>
## Data structure:
##
## county_data = {
##    'county1' : {
##        'lon' : lon,
##        'lat' : lat
##    },....
## }

def reformatCountyData():
    
    reformatted_county_data = {}
    
    county_data = getCountyData()

    for state in county_data:
        for county in county_data[state]:
            reformatted_county_data[state + county] = {
                'lon' : county_data[state][county]['lon'],
                'lat' : county_data[state][county]['lat']
            }

    return reformatted_county_data

def quickFix():
    with open('pickles/covid_data_by_state2.pickle', 'rb') as f:
        covid_data_by_state = pickle.load(f)
    for state in covid_data_by_state:
        try:
            covid_data_by_state[state]['acutely_ill']
        except KeyError:
            covid_data_by_state[state]['acutely_ill'] = 'NA'

        try:
            covid_data_by_state[state]['recovered']
        except KeyError:
            covid_data_by_state[state]['recovered'] = 'NA'
            
    with open('pickles/covid_data_by_state2.pickle', 'wb') as f:
        pickle.dump(covid_data_by_state,f)

def updateData(date):

    compile_data2.updateCovidData()
    quickFix()
    
    with open('pickles/covid_data_by_state2.pickle', 'rb') as f:
        covid_data_by_state = pickle.load(f)
        
    with open('pickles/pop_data_by_state.pickle', 'rb') as f:
        pop_data_by_state = pickle.load(f)
    with open('pickles/pop_data_by_county.pickle', 'rb') as f:
        pop_data_by_county = pickle.load(f)
    
    # get access to states' latitude and longitude
    df1 = pd.read_csv('states.csv')
    df1.set_index('state', inplace=True)
    # accesss like - df1['latitude']['AK']
    #              - df1['longitude']['AK']

    # get access to counties' latitude and longitude
    county_data = reformatCountyData()
    # access like - county_data['NJEssex']['lat']
    #             - county_data['NJEssex']['lon']

    # get access to mined data for today
    df2 = pd.read_csv('covid_data_by_day/covid_data_{}.csv'.format(date))
    df2.set_index('State', inplace=True)
    # access like - df2['Num_Tweets']['AK']
    #             - df2['Case_Count']['AK']

    # go through df2 state by state and add working lat, lon list.

    try:
        df1.drop(['PR', 'DC'], inplace=True)
        df2.drop(['AS', 'GU', 'MP', 'PR', 'DC', 'VI'], inplace=True)
    except KeyError:
        # this happens if one of those values is not there, which they shouldn't be
        print('[] no invalid rows')

    # df2['Case_Count'] == 0 ? Double check
    # load covid_data_by_state.pickle
    with open('pickles/covid_data_by_state2.pickle', 'rb') as f:
        covid_data_by_state = pickle.load(f)
    with open('pickles/covid_data_by_county2.pickle', 'rb') as f:
        covid_data_by_county = pickle.load(f)

    lon = []
    lat = []
    cases_per_day = []
    deaths = []
    acutely_ill = []
    hospitalized = []
    recovered = []
    positive_tests = []
    negative_tests = []
    population = []
    total_tests = []
    death_rate = []
    hospitalization_rate = []

    #print(covid_data_by_county)

    num = 0
    #print(pop_data_by_county)
    for state in df2.index:
        
        if num <= 49:

            if int(df2.at[state, 'Case_Count']) == 0:
                df2.at[state, 'Case_Count'] = covid_data_by_state[state]['date10'].replace(",","")
            
            lat.append(df1['latitude'][state])
            lon.append(df1['longitude'][state])
            cases_per_day.append(covid_data_by_state[state]['cases_per_day'])
            deaths.append(covid_data_by_state[state]['deaths'])
            acutely_ill.append(covid_data_by_state[state]['acutely_ill'])
            hospitalized.append(covid_data_by_state[state]['hospitalized'])
            recovered.append(covid_data_by_state[state]['recovered'])
            positive_tests.append(covid_data_by_state[state]['positive_tests'])
            negative_tests.append(covid_data_by_state[state]['negative_tests'])
            population.append(pop_data_by_state[state])
            
            num_tests = int(covid_data_by_state[state]['positive_tests']) + int(covid_data_by_state[state]['negative_tests'])
            total_tests.append(num_tests)
            if int(df2.at[state, 'Case_Count']) != 0:
                rate_num = float(covid_data_by_state[state]['deaths']) / float(df2.at[state, 'Case_Count'])
                death_rate.append(rate_num)
            else:
                death_rate.append('NA')
                
            if covid_data_by_state[state]['hospitalized'] != 'NA' and int(df2.at[state, 'Case_Count']) != 0:
                hosp_rate = float(covid_data_by_state[state]['hospitalized']) / float(df2.at[state, 'Case_Count'])
                hospitalization_rate.append(hosp_rate)
            else:
                hospitalization_rate.append('NA')
            
            
        else:
            lat.append(county_data[state]['lat'])
            lon.append(county_data[state]['lon'])

            if state == 'NYNew York (Manhattan)':
                state = 'NYNew York'

            try:
                cases_per_day.append(covid_data_by_county[state]['cases_per_day'])
            except KeyError:
                cases_per_day.append('NA')
                
            try:
                deaths.append(covid_data_by_county[state]['deaths'])
                rate_num = float(covid_data_by_county[state]['deaths']) / float(df2.at[state, 'Case_Count'])
                death_rate.append(rate_num)
            except KeyError:
                deaths.append('NA')
                death_rate.append('NA')


            
            ##
            hospitalization_rate.append('NA')
            acutely_ill.append('NA')
            hospitalized.append('NA')
            recovered.append('NA')
            positive_tests.append('NA')
            negative_tests.append('NA')
            total_tests.append('NA')
            
            state.replace("'","")

            if state == 'NYNew York':
                population.append(pop_data_by_county[state + ' (Manhattan)'])
            elif state == 'NYKings':
                population.append(pop_data_by_county[state + ' (Brooklyn)'])
            elif state == 'VACity of Alexandria':
                state = 'VAAlexandria'
                population.append(pop_data_by_county[state])
            elif state == 'HIOahu in Honolulu':
                state = 'HIHonolulu'
                population.append(pop_data_by_county[state])
            elif state == 'VACity of Norfolk':
                state = 'VANorfolk'
                population.append(pop_data_by_county[state])
            elif state == 'HILanai in Maui':
                state = 'HIMaui'
                population.append(pop_data_by_county[state])
            elif state == 'MDPrince Georges':
                state = "MDPrince George's"
                population.append(pop_data_by_county[state])
            elif state == 'VACity of Fredericksburg':
                state = 'VAFredericksburg'
                population.append(pop_data_by_county[state])
            else:
                try:
                    population.append(pop_data_by_county[state])
                except KeyError:
                    print('[] pop append error, ' + state)
                    population.append('X')

        num += 1

    # get into list format: 

    Data = {
        'Case_Count' : df2['Case_Count'].tolist(),
        'Num_Tweets' : df2['Num_Tweets'].tolist(),
        'State' : df2.index.tolist(),
        'Lat' : lat,
        'Lon' : lon,
        'Cases_Per_Day' : cases_per_day,
        'Deaths' : deaths,
        'Acutely_Ill' : acutely_ill,
        'Hospitalized' : hospitalized,
        'Recovered' : recovered,
        'Positive_Tests' : positive_tests,
        'Negative_Tests' : negative_tests,
        'Total_Tests' : total_tests,
        'Population' : population,
        'Death_Rate' : death_rate,
        'Hospitalization_Rate' : hospitalization_rate
    }

    df_int = pd.DataFrame(Data, columns=['Case_Count', 'Num_Tweets', 'State', 'Lat', 'Lon', 'Cases_Per_Day', 'Deaths', 'Acutely_Ill', 'Hospitalized', 'Recovered', 'Positive_Tests', 'Negative_Tests', 'Total_Tests',
                                         'Population', 'Death_Rate', 'Hospitalization_Rate'])
    
    #df_int.to_csv('covid_data_by_day/covid_data_07_13_2020_updated.csv')
    
    df_states = df_int[0:50]
    df_counties = df_int[50:]
    df_states.to_csv('covid_data_by_day/covid_data_by_state_{}.csv'.format(date))

    df_counties.drop(columns=['Acutely_Ill', 'Hospitalized', 'Recovered', 'Positive_Tests', 'Negative_Tests', 'Total_Tests', 'Hospitalization_Rate'], inplace=True)
    
    #print(df_counties)
    df_counties.to_csv('covid_data_by_day/covid_data_by_county_{}.csv'.format(date))

if __name__ == '__main__':
    updateData('07_13_2020')
    updateData('07_14_2020')
    updateData('07_15_2020')
    updateData('07_16_2020')
    updateData('07_17_2020')

##quickFix()
##updateData('07_13_2020')
##updateData('07_15_2020')
##updateData('07_13_2020')
##visualizeData('covid_data_by_state_07_13_2020', '07_13_2020')
##visualizeData('covid_data_by_state_07_14_2020', '07_14_2020')
##visualizeData('covid_data_by_state_07_15_2020', '07_15_2020')

