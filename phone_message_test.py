from twilio.rest import Client

account_sid = "AC1360cea428da1fcb809f629507bc5774"
auth_token = "5c0c45fb68782364b67b086701ec17ee"

client = Client(account_sid, auth_token)

message = client.messages.create(
    to="+18155662099",
    from_="+12408396909",
    body="Your data miner has completed its task.")

print(message.sid)
