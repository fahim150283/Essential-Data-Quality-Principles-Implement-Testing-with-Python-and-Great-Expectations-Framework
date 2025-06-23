# this file will read json file and print the needed data
import json

with open('Email Credentials.json') as f:
    data = json.load(f)

mailserveraddr = data['email']['mailServer']
fromaddr = data['email']['from']
password = data['email']['password']
to = data['email']['to']

print (mailserveraddr)
print (fromaddr)
print (password)
print (to)