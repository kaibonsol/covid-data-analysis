#
# <phone app>
#

from flask import Flask, request
from twilio import twiml
from twilio.rest import Client
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

def runMiner():
    os.system("python C:\\Users\\kaibo\\OneDrive\\Desktop\\python-twitter\\data_miner2.py")

@app.route('/sms', methods=['POST'])
def sms():
    number = request.form['From']
    message_body = request.form['Body']

    sendMessage('Will do, Kai!')
    runMiner()
   

    return str(resp)

if __name__ == '__main__':
    app.run()
