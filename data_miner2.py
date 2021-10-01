#
# < data_miner 2.0 >
# Author: Kai Bonsol
#
# this data miner avoids webscraping by downloading Jon Hopkins data directly
# from the github site.
#

import tweepy
import webbrowser
import os
import pickle
import pandas as pd
import requests
import geopy.distance
from twilio.rest import Client


# <update>
# sends text message on status of data miner

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

# <compileData>
# cleans DataFrame and uploads .csv files

def compileData(api, query, dummy, max_tweets, date_since, date_until):

    # load pickled DataFrame objects
    with open('pickles/covid_data_by_state.pickle', 'rb') as f:
        covid_state_df = pickle.load(f)
    with open('pickles/covid_data_by_county.pickle', 'rb') as f:
        covid_county_df = pickle.load(f)

    processed_tweets, total_tweets, hashtag_data, tweet_data = processTweets(api, query, dummy, max_tweets, date_since, date_until)

    covid_state_df['Num_Tweets'] = covid_state_df.index.map(tweet_data)
    covid_county_df['Num_Tweets'] = covid_county_df.index.map(tweet_data).fillna('0')

    # to_csv these dataframes
    covid_state_df.to_csv('covid_data_by_day/{}bystate.csv'.format(date_since))
    covid_county_df.to_csv('covid_data_by_day/{}bycounty.csv'.format(date_since))

    # handle updating time data frame

    time_df = pd.read_csv('covid_data_by_day/timedata.csv', index_col=None)

    time_df = time_df.loc[:, ~time_df.columns.str.contains('^Unnamed')]

    timedata = []
    timedata.append(date_since)
    timedata.append(processed_tweets)
    timedata.append(total_tweets)

    nalist = []
    cur_index = 0
    for col in time_df.columns:
        if not col in hashtag_data and col != 'date' and col != 'processed_tweets' and col != 'total_tweets':
            print('[data_miner 2.0] new hashtag, ' + col)
            nalist.append(cur_index)
        cur_index += 1

    cur_index = 0
    for hashtag, count in hashtag_data.items():
        while cur_index in nalist:
            timedata.append('')
            cur_index += 1

        if not hashtag in time_df.columns:
            time_df[hashtag] = ''
        
        timedata.append(count)
        cur_index += 1


    # code to insert new row and send csv file

    try:
        time_df.loc[time_df.shape[0]] = timedata
        time_df.to_csv('covid_data_by_day/timedata.csv')
    except ValueError:
        print('[data_miner 2.0] timedata error, manual logging info:')
        print('<timedata>')
        print(timedata)
        print('<time_df>')
        print(time_df.columns)
    

# <processTweets>
# produce a DataFrame from mined tweets and Jon Hopkins

def processTweets(api, query, dummy, max_tweets, date_since, date_until):

    with open('pickles/state_abrv.pickle', 'rb') as f:
        states = pickle.load(f)

    # dictionary for hashtag frequencies
    hashtags = query.split(' OR ')

    hashtag_data = {}
    for hashtag in hashtags:
        hashtag_data[hashtag] = 0

    # nested dictionary for tweet data
    data = {}

    for state in states:
        data[state] = 0
        
    if dummy:
        max_tweets /= 150
    else:
        update("[data_miner 2.0] starting...")

    print('[data_miner 2.0] max_tweets = ' + str(int(max_tweets)))

##    for hashtag in query:

    cur_tweet = 0
    processed_tweets = 0
    tweets = []

    for status in tweepy.Cursor(api.search, q=hashtag, geocode='39.8,-95.583068847656,2500km',
                                    since=date_since, until=date_until, lang='en').items(int(max_tweets)):

        # process hashtags

        processHashtags(hashtag_data, status)

        # process tweets
        
        if status.id not in tweets:
            tweets.append(status.id)
        else:
            if dummy:
                print('duplicate')
            continue

        if status.place is not None and status.place.country_code == "US":
            
            try:
                city, state = status.place.full_name.split(', ')
            except ValueError:
                if dummy:
                    print("(ignored value)")
                continue

            print('[' + city + ', ' + state + ']' + ' location enabled tweet about coronavirus found!')

            with open('pickles/abrv_state.pickle', 'rb') as f:
                abrv_state = pickle.load(f)
            
            if state != "USA" and state != "DC" and status.coordinates is not None:

                county = findCountyByCoordinates(state, status.coordinates['coordinates'][0], status.coordinates['coordinates'][1])
                try:
                    data[county + ', ' + abrv_state[state] + ', US'] += 1
                except KeyError:
                    data[county + ', ' + abrv_state[state] + ', US'] = 1

                processed_tweets += 1

            else:
                
                if state == "USA":
                    try:
                        data[city] += 1
                        processed_tweets += 1
                    except KeyError:
                        if dummy:
                            print('(ignored value)')
                elif state == "DC":
                    if dummy:
                        print('(ignored value)')
                    continue
                else:
                    try:
                        data[abrv_state[state]] += 1
                        processed_tweets += 1
                    except KeyError:
                        print('(ignored value)')

        cur_tweet += 1
##        if cur_tweet % 500000 == 0:
##            update('[data_miner 2.0] checkpoint, looked at ' + str(cur_tweet) + ' tweets')

    if not dummy:
        with open('pickles/tweets/tweet_{}.pickle'.format(date_since), 'wb') as f:
            pickle.dump(tweets, f)

    print("Processed " + str(processed_tweets) + " out of " + str(cur_tweet) + " tweets")
    if not dummy:
        update("Processed " + str(processed_tweets) + " out of " + str(cur_tweet) + " tweets")

    return (processed_tweets, cur_tweet, hashtag_data, data)

# <processHashtags>
# takes hashtag data dictionary and tweet and updates counts

def processHashtags(hashtag_data, tweet):

    for hashtag in hashtag_data:

        if hashtag in tweet.text or hashtag.upper() in tweet.text:

            hashtag_data[hashtag] += 1
    

# <reloadData>
# runs relevant save functions

def reloadData(date):
    
    saveCovidData(date)
    print('reloaded data for ' + date)

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


# <saveCovidData>
# grabs covid data from Jon Hopkins
# pickles state and county dataframes

def saveCovidData(date):

    year, month, day = date.split('-')
    date = month + '-' + day + '-' + year

    # state dataframe
    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/' + date + '.csv'
    state_df = pd.read_csv(url, index_col=0)

    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + date + '.csv'
    county_df = pd.read_csv(url, index_col=0)

    # reformat DataFrames
    
    # county, state name
    county_df.set_index('Combined_Key', inplace=True)
    county_df = county_df[0:3144]
    county_df.drop(columns=['Admin2', 'Province_State', 'Country_Region', 'Last_Update'], inplace=True)
    # access like = covid_data_by_county['county name']['Confirmed']

    state_df.drop(['Diamond Princess', 'Virgin Islands', 'Puerto Rico', 'Northern Mariana Islands', 'Guam', 'District of Columbia', 'Grand Princess', 'American Samoa'], inplace=True)
    state_df.drop(columns=['Country_Region', 'Last_Update', 'FIPS', 'UID', 'ISO3'], inplace=True)
    # access like = covid_data_by_state['state name']['Confirmed']
    
    with open('pickles/covid_data_by_state.pickle', 'wb') as f:
        pickle.dump(state_df, f)
    
    with open('pickles/covid_data_by_county.pickle', 'wb') as f:
        pickle.dump(county_df, f)

# <twitterConnect>
# connects to twitter API

def twitterConnect():
    
    ## Begin connection ##

    date_since = '2020-07-17'
    date_until = '2020-07-18'

    reload = input("Enter anything to reload >")

    if reload != "":
        reloadData(date_since)
        print('Reloaded COVID data')
    else:
        print('Not reloading COVID data')

    dummy = False
    dummy_input = input("Enter anything to run DUMMY mode >")

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
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    return (date_since, date_until, api, dummy)

if __name__ == '__main__':


    print('[data_miner 2.0] starting')
    print('**Remember to manually update date_since and date_until')
    
    date_since, date_until, api, dummy = twitterConnect()

    #print('running date_miner 2.0 for date ' + date_since)

    ## Now connected ##

    search_words = ('#covid OR #coronavirus OR #CoronavirusVaccine OR #pandemic OR #wearamask OR #CoronaVirusUpdate OR #mask OR #socialdistancing OR #CoronavirusPandemic OR #covid19 OR #covid__19 OR coronavirus OR corona OR covid OR pandemic OR lockdown OR quarantine OR covidiots OR covid-19 OR social distancing OR flattening the curve OR flatten the curve OR coronavirus vaccine')

    max_tweets = 500000
    reloadData('2020-07-23')
    compileData(api, search_words, dummy, max_tweets, '2020-07-23', '2020-07-24')
    reloadData('2020-07-24')
    compileData(api, search_words, dummy, max_tweets, '2020-07-24', '2020-07-25')
    reloadData('2020-07-25')
    compileData(api, search_words, dummy, max_tweets, '2020-07-25', '2020-07-26')
