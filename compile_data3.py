## <compile_data3.py>
## Author: Kai Bonsol
## This program will access population per county, 
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

##data = {
##    'state1' : pop,
##    'state2' : pop, ...
##}

def getPopulation():

    pop_data_by_state = {}
    pop_data_by_county = {}

    resp = requests.get('https://www.citypopulation.de/en/usa/admin')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class':'data'})

    with open('pickles/state_abrv.pickle', 'rb') as f:
        state_abrv = pickle.load(f)

    curState = 'AL'

    for row in table.findAll('tr'):

        td = row.findAll('td')
        try:
            td[1]
        except IndexError:
            continue
        
        if td[1].text == 'State':
            curState = td[0].text
            pop_data_by_state[state_abrv[td[0].text]] = td[5].text.replace(',','')
        elif td[0].text == 'USA [United States of America]':
            break
        else:
            pop_data_by_county[state_abrv[curState] + td[0].text] = td[5].text.replace(',','')

    with open('pickles/pop_data_by_state.pickle', 'wb') as f:
        pickle.dump(pop_data_by_state,f)
    with open('pickles/pop_data_by_county.pickle', 'wb') as f:
        pickle.dump(pop_data_by_county,f)
        
