import tweepy
import webbrowser
import os.path
from os import path
import time

## Functions ##

# Print and Count Twitter Followers #
def printAndCount(api):
    count = 0
    for follower in tweepy.Cursor(api.followers).items():
        print(follower.name)
        count += 1
    return count

# Look at followers- reference whether consistent with previous list of followers #
# Return [*change in followers*, *change in following*]
    
def trackFollowers(api):
    # if file does not exist, let's write all the current data and return out of this function.
    if not path.exists("followers.txt"):
        f = open("followers.txt", "w+")
        for follower in tweepy.Cursor(api.followers).items():
            f.write(str(follower.id) + "\n")
        f.close()
        return 0
    else:
        # read in data and compare with current following
        f = open("followers.txt", "r+")
        curFollowers = []
        oldFollowers = []

        # check if oldFollower is not in current followers
        for curFollower in tweepy.Cursor(api.followers).items():
            curFollowers.append(curFollower.id)

        print(curFollowers)
            
        f1 = f.readlines()
        unfollowedCount = 0
        for oldFollower in f1:
            oldFollower = oldFollower.strip('\n')
            oldFollowers.append(oldFollower)

        for oldFollower in oldFollowers:
            if oldFollower not in curFollowers:
                print(str(oldFollower) + " unfollowed you")

                
        f.truncate(0)
        f.close()

        # clear & update the .txt file
        f = open("followers.txt", "w+")
        for follower in curFollowers:
            f.write(str(follower) + "\n")

        f.close()
        return unfollowedCount

def lookAtMessages(api):

    user_screen_name = input("Enter screen name > ")
    user = api.get_user(user_screen_name)
    user_id = user.id
    print("user_id: " + str(user_id))
    print(type(user_id))
    user_direct_messages = api.list_direct_messages(8000)
    
    for direct_message in user_direct_messages:

        rid = direct_message.message_create['target']['recipient_id']
        sid = direct_message.message_create['sender_id']

        recipient = api.get_user(rid)
        sender = api.get_user(sid)

        if user_id == recipient.id or user_id == sender.id:
            print("[" + sender.screen_name + "] " + direct_message.message_create['message_data']['text'])

def sendMessage(api):

    recipient_screen_name = input("Enter screen name > ")
    recipient = api.get_user(recipient_screen_name)
    recipient_id = recipient.id

    text = input("Message " + recipient_screen_name + " > ")
    for i in range(10):
        time.sleep(10)
        api.send_direct_message(recipient_id, text)

    print("Message sent!")

def automaticMessageWithRicardo(api):

    recipient = api.get_user("RikardoArroyo7")
    text = "bro... I have something super insane to tell you.. message me soon..."
    api.send_direct_message(recipient.id, text)
    time.sleep(60)
    text = "bro I'm going insane. There's something happening outside of my house"
    api.send_direct_message(recipient.id, text)
    time.sleep(30)
    text = "Yeah. What are you doing today?"
    api.send_direct_message(recipient.id, text)
    time.sleep(40)
    text = "Bro. You know what's crazy?"
    api.send_direct_message(recipient.id, text)
    time.sleep(40)
    text = "You've been talking to a MACHINE this entire time"
    api.send_direct_message(recipient.id, text)
    
def tweet(api):
    message = input("tweet: ")
    api.update_status(message)
    time.sleep(1000)

## Begin connection ##

auth = tweepy.OAuthHandler("TEu3797AuZUmmAtlNc2MSML4H","zOu2FpB9yIHDDKGuJvY4OwEQR1OiDHjQ0j06t4xUGJi5rmJkGS")

try:
    redirect_url = auth.get_authorization_url()
except tweepy.TweepError:
    print('Error! Failed to get request token.')

webbrowser.open(redirect_url, new=2)

# user goes to website to get verifier
verifier = input('Verifier: ')

auth.get_access_token(verifier)
api = tweepy.API(auth)

## Now connected ## 

# Input loop #

command = input("Enter a command or # to exit > ")

while command != "#":
    if command == "printandcount":
        count = printAndCount(api)
        print("Follower count: ", count)
    if command == "trackfollowers":
        count = trackFollowers(api)
        print("Unfollowed count: ", count)
    if command == "lookatmessages":
        lookAtMessages(api)
    if command == "sendmessage":
        sendMessage(api)
    if command == "messagericardo":
        automaticMessageWithRicardo(api)
    if command == "tweet":
        tweet(api)

    command = input("Enter a command or # to exit > ")

print("Thank you for using the Python-Twitter app!")
        

