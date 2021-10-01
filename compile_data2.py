## <compile_data2.py>
## Author: Kai Bonsol
## This program will access new cases per day, test count,
## acutely ill, hospitalized, recovered, deceased
##
##

import pickle
import bs4 as bs
import requests
from twilio.rest import Client

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


# <saveAdditionalCovidData>
##data structure:
##
##data = {
##    'county/state 1' : {
##        'date10' : num_cases,
##        'population' : population,
##        'cases_per_day' : cases_per_day,
##        'deaths' : deaths,
##        'cases' : cases,
##        'tests' : tests
##    }
##}

def saveAdditionalCovidDataState(site, covid_data, state):
    
    resp = requests.get('https://www.citypopulation.de/en/usa/covid/' + site)
    soup = bs.BeautifulSoup(resp.text, "lxml")
    tables = soup.findAll('table', {'class':'data'})

    cases_per_day_table = tables[1]
    death_table = tables[2]
    cases_table = tables[3]
    test_table = tables[4]

    # ISSUE: individual counties only have new cases per day & deaths


    covid_data[state]['cases_per_day'] = cases_per_day_table.findAll('tr')[9].findAll('td')[1].text.replace(',','')
    covid_data[state]['deaths'] = death_table.findAll('tr')[9].findAll('td')[1].text.replace(',','')
    
    for row in cases_table.findAll('tr')[1:]:
        
        if row.findAll('td')[0].text == 'Cases (total)' or row.findAll('td')[0].text == 'Deceased':
            continue
        elif row.findAll('td')[0].text == 'Acutely ill':
            covid_data[state]['acutely_ill'] = row.findAll('td')[1].text.replace(',','')
        elif row.findAll('td')[0].text == 'Hospitalized':
            covid_data[state]['hospitalized'] = row.findAll('td')[1].text.replace(',','')
        elif row.findAll('td')[0].text == 'Recovered':
            covid_data[state]['recovered'] = row.findAll('td')[1].text.replace(',','')
            
    covid_data[state]['positive_tests'] = test_table.findAll('tr')[1].findAll('td')[1].text.replace(',','')
    covid_data[state]['negative_tests'] = test_table.findAll('tr')[2].findAll('td')[1].text.replace(',','')
    
    try:
        covid_data[state]['hospitalized']
    except KeyError:
        covid_data[state]['hospitalized'] = 'NA'
        
    try:
        covid_data[state]['acutely_ill']
    except KeyError:
        covid_data[state]['acutely_ill'] = 'NA'

##}

def saveAdditionalCovidDataCounty(site, covid_data, county):
    
    resp = requests.get('https://www.citypopulation.de' + site)
    soup = bs.BeautifulSoup(resp.text, "lxml")
    tables = soup.findAll('table', {'class':'data'})

    try:
        tables[1]
    except IndexError:
        return
    
    cases_per_day_table = tables[1]
    covid_data[county]['cases_per_day'] = cases_per_day_table.findAll('tr')[9].findAll('td')[1].text.replace(',','')

    try:
        tables[2]
    except IndexError:
        return
    
    death_table = tables[2]
    covid_data[county]['deaths'] = death_table.findAll('tr')[9].findAll('td')[1].text.replace(',','')


def updateCovidData():
    
    with open('pickles/covid_data_by_state.pickle', 'rb') as f:
        covid_data_by_state = pickle.load(f)
    with open('pickles/covid_data_by_county.pickle', 'rb') as f:
        covid_data_by_county = pickle.load(f)
    with open('pickles/abrv_state.pickle', 'rb') as f:
        abrv_state = pickle.load(f)

##    # states first
##    for state in covid_data_by_state:
##        site = state + '__' + abrv_state[state].replace(' ', '_')
##        saveAdditionalCovidDataState(site, covid_data_by_state, state)
##
##    with open('pickles/covid_data_by_state2.pickle', 'wb') as f:
##        pickle.dump(covid_data_by_state,f)
    
    # now counties
    update('starting data compiler...')

    county_sites = getCountySites()
    #print(county_sites[0])
    index = 0
    for county in covid_data_by_county:
        try:
            county_sites[index]
        except IndexError:
            index += 1
            continue
        
        site = county_sites[index]
        if site == '/en/usa/covid/district_of_columbia/11001__district_of_columbia/':
            index += 1
            continue

        print('visiting ' + site + ': for county ' + county)
        saveAdditionalCovidDataCounty(site, covid_data_by_county, county)
        index += 1

    print(covid_data_by_county)
    with open('pickles/covid_data_by_county2.pickle', 'wb') as f:
        pickle.dump(covid_data_by_county, f)

    update('Data compiler 2 is finished')
##    num = 0
##    num1 = 1
##    num2 = 1
##    curState = 'AL'
##    for county in covid_data_by_county:
##        site = abrv_state[county[0:2]] + '/'
##        if county[0:2] != curState:
##            curState = county[0:2]
##            num2 = 1
##            num1 += 1
##        if num1 < 10:
##            site += '0'
##        site += str(num1)
##        if num2 < 10:
##            site += '00'
##        elif num2 < 100:
##            site += '0'
##        site += str(num2)
##        num2 += 2
##        site += '__' + county[2:].replace(' ','_')
##        print(site)
##        num += 1
##        if num > 100:
##            break

def getCountySites():

    county_sites = []
    resp = requests.get('https://www.citypopulation.de/en/usa/covid/')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class':'data'})
    for row in table.findAll('tr')[1:]:
        if row.findAll('td')[1].text == 'State':
            continue
        if row.find('td', {'class':'sc'}).find('a') is not None:
            #print(row.find('td', {'class':'sc'}).find('a')['href'])
            county_sites.append(row.find('td', {'class':'sc'}).find('a')['href'])

    #print(county_sites)
    return county_sites

def show():
    with open('pickles/covid_data_by_state2.pickle', 'rb') as f:
        data = pickle.load(f)
    print(data)

