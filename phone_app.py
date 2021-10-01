#
# <xKai AI>
#
# this app will send me covid data real time (perhaps graphs? idk)


from flask import Flask, request
from twilio.rest import Client
import datetime
import data_compiler1
import pandas as pd
import os

app = Flask(__name__)

# <update>
# sends text message on status of data miner

def sendMessage(text):

    print(text)
    account_sid = "AC1360cea428da1fcb809f629507bc5774"
    auth_token = "5c0c45fb68782364b67b086701ec17ee"
    
    client = Client(account_sid, auth_token)

    message = client.messages.create(
        to="+18155662099",
        from_="+12408396909",
        body=text)

    print(message.sid)

@app.route('/sms', methods=['POST'])
def sms():
    number = request.form['From']
    message_body = request.form['Body']
    handleMessage(message_body)

def handleMessage(message):

    today = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    covid_df_today = data_compiler1.getCovidData(today)
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    covid_df_yesterday = data_compiler1.getCovidData(yesterday)

    confirmed_sum_today = covid_df_today['Confirmed'].sum()
    death_sum_today = covid_df_today['Deaths'].sum()
    recovered_sum_today = covid_df_today['Recovered'].sum()

    confirmed_sum_yesterday = covid_df_yesterday['Confirmed'].sum()
    death_sum_yesterday = covid_df_yesterday['Deaths'].sum()
    recovered_sum_yesterday = covid_df_yesterday['Recovered'].sum()

    marginal_confirmed = confirmed_sum_today - confirmed_sum_yesterday
    marginal_recovered = recovered_sum_today - recovered_sum_yesterday
    marginal_deaths = death_sum_today - death_sum_yesterday
    
    # return covid numbers for the U.S.
    if message == 'get covid data':
        sendMessage('Covid data for ' + today + '\n' + 'Confirmed sum: ' + str(confirmed_sum_today) + ', ' + str(marginal_confirmed) + ' since yesterday'
               + '\n' + 'Recovered sum: ' + str(recovered_sum_today) + ', ' + str(marginal_recovered) + ' since yesterday' +
                  '\n' + 'Death sum: ' + str(death_sum_today) + ', ' + str(marginal_deaths) + ' since yesterday')
        

if __name__ == '__main__':
    app.run()
