## <data_miner1.py>
## Author : Kai Bonsol
## Project Description:
##
##    This program uses Twitter Dev API with tweepy in order to analyze social media data and its correlation with Covid data provided by Jon Hopkins.
##
##    The program uses several queries and then searches through a large quantity of tweets for location data. The data will be presented in a variety of ways--
##
##    1. Number of cases per state versus social media hits in that state
##    2. #1 but with detail on counties
##    
##
import tweepy
import webbrowser
import os
import os.path
from os import path
import time
import matplotlib.pyplot as plt
from matplotlib import style
import bs4 as bs
import pandas as pd
import numpy as np
import pandas_datareader.data as web
import requests
import pickle
import dbfread
from dbfread import DBF
import geopy.distance
from twilio.rest import Client
from datetime import datetime
import compile_data1

def update(text):
    print(text)
    account_sid = "AC1360cea428da1fcb809f629507bc5774"
    auth_token = "5c0c45fb68782364b67b086701ec17ee"
    
    client = Client(account_sid, auth_token)

    message = client.messages.create(
        to="+18155662099",
        from_="+12408396909",
        body=text)

    print(message.sid)


# <loadIntoDataFrame>
# use data structure made by <accessTweetsByQuery> and create
# DataFrame object

def loadIntoDataFrame(api, query, dummy, max_tweets, date_since, date_until):

    style.use('ggplot')

    d = accessTweetsByQuery(api, query, dummy, max_tweets, date_since, date_until)

    if not dummy:
        update("Your data miner has completed its task.")

    
    num_tweets = []
    case_counts = []
    states = []
    counties = []

    num = 0
    total_tweets = 0

    for state in d:
        
        num_tweets.append(d[state]['num_tweets'])
        case_counts.append(d[state]['total']) 
        states.append(state)
            #counties.append(state)
        if num <= 49:
            total_tweets += int(d[state]['num_tweets'])
            
        num += 1

    Data = {
        'Case_Count' : case_counts,
        'Num_Tweets' : num_tweets,
        'State' : states,
    }
    
    df = pd.DataFrame(Data, columns=['Case_Count', 'Num_Tweets', 'State'])
    #df = df.transpose()

    date = datetime.now().strftime("%m_%d_%Y")
    file_name = 'covid_data_' + date

    if not dummy:
        if not os.path.exists('covid_data_by_day/{}.csv'.format(file_name)):
            df.to_csv('covid_data_by_day/{}.csv'.format(file_name))
        else:
            print('Already have {}'.format(file_name))
    else:
        df.to_csv('dummy.csv')
    
    ax = df.plot(x = 'Case_Count', y = 'Num_Tweets', kind = 'scatter')
    a = pd.concat({'Case_Count' : df['Case_Count'], 'Num_Tweets' : df['Num_Tweets'], 'State' : df['State']}, axis=1)
    for i, point in a.iterrows():
        ax.text(point['Case_Count'], point['Num_Tweets'], str(point['State']))

    ## stuff to update date&count pickle list (will be useful for aggregating data)

    if not dummy:
        with open('pickles/time_index_data.pickle', 'rb') as f:
            time_index_data = pickle.load(f)

        time_index_data.append((date, total_tweets))

        with open('pickles/time_index_data.pickle', 'wb') as f:
            pickle.dump(time_index_data, f)

    # use compile_data1.py functions to automate data
    if not dummy:
        compile_data1.updateData(date)
    #compile_data1.visualizeData('covid_data_by_state_' + date, date)

    
    
# <accessTweetsByQuery>

##Construct a data structure for creating a DataFrame object
##that can graph x: number of cases, y: number of tweets by state

##data = {
##    'state1' : {
##        'num_tweets' : num_tweets,
##        'total' : total,
##        'county1' : (num_tweets1, total1),
##        ....
##        'countyX' : totalX
##    },...
##}

def searchForTweets(query, max_tweets):
    
    searched_tweets = []
##    last_id = -1
##
##    while len(searched_tweets) < max_tweets:
##        count = max_tweets - len(searched_tweets)
##        try:
##            new_tweets = api.search(q=query, count=count, max_id=str(last_id-1))
##            if not new_tweets:
##                break
##            searched_tweets.extend(new_tweets)
##            last_id = new_tweets[-1].id
##        except tweepy.TweepError as e:
##            print("[1] Caught TweepError-- waiting 5 minutes")
##            print(e)
##            time.sleep(5*60)
##            print("Continuing...")
##            continue

    # use date_since and date_until to find all tweets
    # use geocode for united states to limit
    # lang='en'

    return searched_tweets

def accessTweetsByQuery(api, query, dummy, max_tweets, date_since, date_until):

    with open("pickles/state_abrv.pickle", "rb") as f:
        states = pickle.load(f)

    data = {}

    for state, abrv in states.items():
        data[abrv] = {
            'total' : readCovidDataByState(abrv),
            'num_tweets' : 0
        }

    if dummy:
        max_tweets /= 500

    if not dummy:
        update("Data miner is starting...")

    # algorithm for finding relevant hashtags ? - use trending and cross reference similar covid19twitter data project.

    searched_tweets = []

    for hashtag in query:
        searched_tweets.extend(tweepy.Cursor(api.search, q=hashtag, geocode='39.8,-95.583068847656,2500km',
                                    since=date_since, until=date_until).items(max_tweets))
    place_objects = []
    coordinate_objects = []
    processed_tweets = 0

    tweets = []

    for status in searched_tweets:

        if status.id not in tweets:
            tweets.append(status.id)
        else:
            if dummy:
                print('duplicate')
            continue

##        if (datetime.now() - status.created_at).days >= 1:
##            continue

        place = status.place
        if place is not None:
            ccode = place.country_code
            full_name = place.full_name
        coordinates = status.coordinates

            
        if place is not None and ccode == "US":
            # coordinates come in (longitude, latitude)
            # print("[" + status.place.country_code + "] " + status.place.full_name)
            try:
                
                city, state = full_name.split(', ')

            except ValueError:

                print("(ignored value)")
                continue
                
            # exception: Nevada, USA does not give city or state but instead state and country
            print("location enabled tweet about coronavirus found!")
            
            if state != "USA" and state != "DC" and coordinates is not None:

                try:
                    county = findCountyByCoordinates(state, coordinates['coordinates'][0], coordinates['coordinates'][1])

                    #print(county)
                    case_count = readCovidDataByCounty(state + county)
                except KeyError:
                    continue
                
##                try:
##                    data[state]['total']
##                except KeyError:
##                    data[state]['total'] = readCovidDataByState(state)

                try:
                    data[state + county]['num_tweets'] += 1
                except KeyError:
                    data[state + county] = {
                        'total' : case_count,
                        'num_tweets' : 1
                    }

                data[state]['num_tweets'] += 1
                processed_tweets += 1
                
            else:

                if state == "USA":
                    with open("pickles/state_abrv.pickle", "rb") as f:
                        state_abbrev = pickle.load(f)
                    try:
##                        case_count = readCovidDataByState(state_abbrev[city])
##                        data[state_abbrev[city]]['total'] = case_count
                        data[state_abbrev[city]]['num_tweets'] += 1
                        processed_tweets += 1
                    except KeyError:
                        print("(ignored value)")

                elif state == "DC":
                    continue
                else:
                    case_count = readCovidDataByState(state)
                    try:
##                        data[state]['total'] = case_count
                        data[state]['num_tweets'] += 1
                        processed_tweets += 1
                    except KeyError:
                        print("(ignored value)")

    date = datetime.now().strftime('%m_%d_%Y')
    if not dummy:
        with open('pickles/tweets/tweet_{}.pickle'.format(date), 'wb') as f:
            pickle.dump(tweets, f)

    print("Processed " + str(processed_tweets) + " tweets")
    if not dummy:
        update("Processed " + str(processed_tweets) + " tweets")
        
    return data
                

# <readCovidDataByCounty>
# reads covid data given a county name --
# returns the past few dates

# ISSUE: duplicate county names :(
# SOLUTION: ILCook vs. MICook 

def readCovidDataByCounty(county):

    with open("pickles/covid_data_by_county.pickle", "rb") as f:
        covid_data = pickle.load(f)

    #print(covid_data)
    #exit()

    date_name = "date10"
    try:
        covid_data[county][date_name]
    except KeyError:
        return 0
        
   #print(covid_data)
   #print("Cases in county " + county + ": " + str(covid_data[county][date_name]))
    return int(covid_data[county][date_name].replace(',',''))
        
def readCovidDataByState(state):

    with open("pickles/covid_data_by_state.pickle", "rb") as f:
        covid_data = pickle.load(f)

    date_name = "date10"
    #print(covid_data)
    #print("Cases in " + state + ": " + str(covid_data[state][date_name]))
    try:
        covid_data[state][date_name]
    except KeyError:
        return 0
    
    return int(covid_data[state][date_name].replace(',',''))
# <reloadData>
# calls functions to reload data collection

def reloadData():
    saveCovidData()
    # saveStateAbbrev()
    
# <saveCovidData>
# Constructs covid data structure from webscraping
# https://www.citypopulation.de/en/usa/covid/

# makes nested dictionary like so
##covid_data = {
##    "county/state 1" : {
##        "num_cases_date_1" : num1
##    },...
##}

# creates data structure by county, then data structure by state

def saveCovidData():
    resp = requests.get('https://www.citypopulation.de/en/usa/covid/')
    soup = bs.BeautifulSoup(resp.text, "lxml")
    table = soup.find('table', {'class':'data'})
    data_objs_by_county = {}
    data_objs_by_state = {}

    curState = ""

    for row in table.findAll('tr')[1:]:

        td = row.findAll('td')
        county = td[0].text.replace("\n","")
        isState = td[1].text.replace("\n","") == "State"
        
        if isState:
            with open("pickles/state_abrv.pickle", "rb") as f:
                state_abbrev = pickle.load(f)
                
            state = state_abbrev[county]
            
            data_objs_by_state[state] = {}
            curState = state
        else:
            county = curState + county
            data_objs_by_county[county] = {}

        date_name = "date10"
        cases_at_date = td[11].text.replace("\n","")
        
        if isState:
            data_objs_by_state[state][date_name] = cases_at_date
        else:
            data_objs_by_county[county][date_name] = cases_at_date
        

    with open("pickles/covid_data_by_county.pickle", "wb") as f:
        pickle.dump(data_objs_by_county, f)

    with open("pickles/covid_data_by_state.pickle", "wb") as f:
        pickle.dump(data_objs_by_state, f)


# <constructCountyDS>
# Reads a .dbf file using dbfread in order to construct a county data structure
# using nested dictionaries so we can access data by state abbreviations,
# which are given in tweepy place objects.

# makes a nested dictionary like so
##states = {
##    "illinois" : {
##        "county1" : {
##            "lon" : lon
##            "lat" : lat
##        },
##        "county2" : {
##            "lon" : lon
##            "lat" : lat
##        },...
##    },...
##}

def getStates():
    
    states_string = []
    f = open("states.txt", "r+")
    f1 = f.readlines()
    for line in f1:
        line = line.strip('\n')
        states_string.append(line)

    f.close()

    return states_string

def saveCountyData():
    # get list of the states
    states = {}
    states_string = getStates()

    for state in states_string:
        states[state] = {}
        for record in DBF('c_03mr20.dbf'):
            if(record['STATE'] == state):
                countyname = record['COUNTYNAME']
                lon = record['LON']
                lat = record['LAT']
                states[state][countyname] = {
                    "lon" : lon,
                    "lat" : lat
                }

    with open("pickles/county_data.pickle", "wb") as f:
        pickle.dump(states, f)

        
# <findCountByCoordinates>
# If a user has percise location enabled, this function will search through the counties
# and find the particular county to associate that tweet with. 

def findCountyByCoordinates(state, lon, lat):
    # get states data structure
    with open("pickles/county_data.pickle", "rb") as f:
        states = pickle.load(f)

    minDistance = 4000
    state_obj = states[state]
    
    # to get first value (or county) of the state for default value.
    values_view = state_obj.values()
    value_iterator = iter(values_view)
    county_obj = next(value_iterator)

    coords_1 = (lat, lon)

    for county, coordinates in state_obj.items():
        
        coords_2 = (coordinates['lat'], coordinates['lon'])
        # print(coords_1)
        # print(coords_2)
        curDistance = geopy.distance.distance(coords_1, coords_2).km
        if(curDistance < minDistance):
            minDistance = curDistance
            county_obj = county

    return county_obj

def helperMain(api, query, dummy, max_tweets, date_since, date_until):
    try:
        print('Attempting to run data mine')
        loadIntoDataFrame(api, query, dummy, max_tweets, date_since, date_until)
    except tweepy.TweepError as e:
        print(e)
        print('Caught TweepError-- waiting 5 minutes.')
        time.sleep(5*60)
        helperMain(api, query, dummy, max_tweets, date_since, date_until)

## Begin connection ##

reload = input("reload? ")

if reload != "":
    reloadData()
    print('Reloaded data')
else:
    print('Not reloading data')

dummy = False
dummy_input = input("dummy? ")

if dummy_input != "":
    dummy = True
    print('Running on DUMMY mode')
else:
    print('Not running DUMMY mode')
    

auth = tweepy.OAuthHandler("TEu3797AuZUmmAtlNc2MSML4H","zOu2FpB9yIHDDKGuJvY4OwEQR1OiDHjQ0j06t4xUGJi5rmJkGS")

try:
    redirect_url = auth.get_authorization_url()
except tweepy.TweepError:
    print('Error! Failed to get request token.')

webbrowser.open(redirect_url, new=2)

# user goes to website to get verifier
verifier = input('Verifier: ')

auth.get_access_token(verifier)
api = tweepy.API(auth, wait_on_rate_limit_notify=True)

## Now connected ##

search_words = ['#covid', '#coronavirus', '#coronaviruscases', '#pandemic', '#pandemia', '#nuevoscasos',
                    '#wearamask', '#stayhome', '#mask', '#socialdistancing', '#coronaviruspandemic', '#COVID19', '#COVID__19']

helperMain(api, search_words, dummy, 50000, '2020-07-17', '2020-07-18')

