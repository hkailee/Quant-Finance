#!/usr/bin/env python3
__author__ = 'Hong Kai LEE'
version = '1.0'

# Import packages used commonly
import os, requests, time, urllib
from splinter import Browser

# defining account access
client_id = {key}
user_name = {user_name}
pass_word = {pass_word}

# define the location of Chrome driver
executable_path = {"executable_path": r'/Applications/chromedriver'}
browser = Browser('chrome', **executable_path, headless = False)

# define the components of the url
method = 'GET'
url = 'https://auth.tdameritrade.com/auth?'
client_code = client_id + '@AMER.OAUTHAP'
payload = {'response_type': 'code',
            'redirect_uri': 'https://localhost/test',
            'client_id': client_code}

# build URL
built_url = requests.Request(method, url, params=payload).prepare()
built_url = built_url.url

# Go to our URL
browser.visit(built_url)

# define elements to pass through the form
payload = {'username': user_name, 'password': pass_word}

# fill up each element in the form
browser.find_by_id("username").first.fill(payload['username'])
browser.find_by_id("password").first.fill(payload['password'])
browser.find_by_id("accept").first.click()

# accepting terms and conditions
browser.find_by_id("accept").first.click()

# give it a second to load
time.sleep(1)
new_url = browser.url

# grab the url and parse it
parse_url = urllib.parse.unquote(new_url.split('code=')[1])

# quit browser
browser.quit()

print(parse_url)

# define the endpoint
url = r'https://api.tdameritrade.com/v1/oauth2/token'

# define the header
headers = {'Content-Type': "application/x-www-form-urlencoded"}

# define the payload
payload = {'grant_type': 'authorization_code',
            'access_type': 'offline',
            'code': parse_url,
            'client_id': client_id,
            'redirect_uri': 'https://localhost/test'}

authReply = requests.post(url, headers=headers, data=payload)

# convert json string to dict
decoded_content = authReply.json()
print(decoded_content)

# grab the access token
access_token = decoded_content['access_token']
headers = {'Authorization': 'Bearer {}'.format(access_token)}