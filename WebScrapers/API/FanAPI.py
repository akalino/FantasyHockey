"""
Project Name: WebScrapers 
@author: alkal 
Created on: 6/3/16 at  9:21 PM 
"""

import requests
from requests_oauthlib import OAuth1Session
from requests_oauthlib import OAuth1
import webbrowser
import csv

GET_TOKEN_URL = 'https://api.login.yahoo.com/oauth/v2/get_token'
AUTHORIZATION_URL = 'https://api.login.yahoo.com/oauth/v2/request_auth'
REQUEST_TOKEN_URL = 'https://api.login.yahoo.com/oauth/v2/get_request_token'
CALLBACK_URL = 'oob'


class CreateConn(object):

    def __init__(self, auth_file):
        self.auth_file = auth_file
        self.auth_dict = self.get_authvals_csv(self.auth_file)

    def get_authvals_csv(self, auth_file):
        vals = {}
        with open(auth_file, 'rb') as f:
            f_iter = csv.DictReader(f)
            vals = f_iter.next()
        return vals

    def write_authvals_csv(self, auth_dict, auth_file):
        f = open(auth_file, 'wb')
        fieldnames = tuple(auth_dict.iterkeys())
        headers = dict((n, n) for n in fieldnames)
        f_iter = csv.DictWriter(f, fieldnames=fieldnames)
        f_iter.writerow(headers)
        f_iter.writerow(auth_dict)
        f.close

    def register_user(self):
        # Request token
        oauth = OAuth1Session(self.auth_dict['consumer_key'],
                              self.auth_dict['consumer_secret'],
                              callback_uri=CALLBACK_URL)

        grab_response = oauth.fetch_request_token(REQUEST_TOKEN_URL)
        self.auth_dict['oauth_token'] = grab_response.get('oauth_token')
        self.auth_dict['oauth_token_secret'] = grab_response.get('oauth_token_secret')

        # User verification
        print '*** Directing to website for login ***'
        webbrowser.open('{aurl}?oauth_token={tok}'.format(aurl=AUTHORIZATION_URL,
                                                          tok=self.auth_dict['oauth_token']))
        self.auth_dict['oauth_verifier'] = raw_input('Please enter your PIN:')
        self.get_login_token()

    def get_login_token(self):
        # Somehow need to trick it into taking this verifier
        oauth2 = OAuth1Session(self.auth_dict['consumer_key'],
                              self.auth_dict['consumer_secret'],
                              self.auth_dict['oauth_token'],
                              self.auth_dict['oauth_token_secret'],
                              verifier=self.auth_dict['oauth_verifier'])
        oauth_tokens = oauth2.fetch_access_token(GET_TOKEN_URL)
        self.auth_dict.update({'oauth_token': oauth_tokens.get('oauth_token')})
        self.auth_dict.update({'oauth_token_secret': oauth_tokens.get('oauth_token_secret')})
        self.write_authvals_csv(self.auth_dict, self.auth_file)
        return oauth_tokens

    def refresh_token(self):
        oauth = OAuth1Session(self.auth_dict['consumer_key'],
                              self.auth_dict['consumer_secret'],
                              self.auth_dict['oauth_token'],
                              self.auth_dict['oauth_token_secret'],
                              verifier=self.auth_dict['oauth_verifier'])
        oauth_tokens = oauth.fetch_access_token(GET_TOKEN_URL)
        self.auth_dict.update({'oauth_token': oauth_tokens.get('oauth_token')})
        self.auth_dict.update({'oauth_token_secret': oauth_tokens.get('oauth_token_secret')})
        self.write_authvals_csv(self.auth_dict, self.auth_file)

    def execute_query(self, query_string):
        header_oauth = OAuth1(self.auth_dict['consumer_key'],
                              self.auth_dict['consumer_secret'],
                              self.auth_dict['oauth_token'],
                              self.auth_dict['oauth_token_secret'],
                              signature_type='auth_header')

        base_url = 'http://fantasysports.yahooapis.com/fantasy/v2/'
        url = base_url + query_string + '?format=json'
        results = requests.get(url, auth=header_oauth)
        if results.status_code != 200:
            self.refresh_token()
            results = requests.get(url, auth=header_oauth)

        return results.text


    def api_request(self, query_string):
        base_url = 'http://fantasysports.yahooapis.com/fantasy/v2/'
        url = base_url + query_string + '?format=json'
        if ('oauth_token' not in self.auth_dict) or ('oauth_token_secret' not in self.auth_dict) or (not (self.auth_dict['oauth_token'] and self.auth_dict['oauth_token_secret'])):
            self.register_user()

        query = self.call_api(url, 'auth_header')
        if query.status_code != 200:
            self.refresh_token()
            query = self.call_api(url, 'auth_header')

        return query

    

