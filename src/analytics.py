import requests
import json
import boto3
import botocore
import pandas as pd
import os

from time import sleep
from database.dynamodb import create_event, create_profile
from urllib import urlencode
from datetime import date, timedelta
from os import environ
from requests.auth import HTTPBasicAuth

EXPORT_URL = 'https://data.mixpanel.com/api/2.0/export'
ENGAGE_URL = 'https://mixpanel.com/api/2.0/engage'
TIMEFRAME_DAYS = 90

with open('data/event_names.json') as json_file:
    data = json.load(json_file)
    EVENT_NAMES = map(lambda obj: obj['Name'], data)

class Analytics(object):
    """Data from Mixpanel"""
    def __init__(self, token):
        self.token = token

    def events(self, days=TIMEFRAME_DAYS, increment=1, offset=1):
        for num_days in xrange(offset, (days + offset)):
            end_date = date.today() - timedelta(days=num_days)
            start_date = end_date - timedelta(days=increment)
            url = self._generate_export_url(start_date, end_date)

            print "Fetching for {}".format(str(start_date))
            response = self._fetch_url(url, stream=True)
            count = 0
            try:
                for line in response.iter_lines():
                    count += 1
                    if count % 1000 == 0:
                        print "{} entries exported".format(count)

                    if line:
                        event_data = line.decode('utf-8')
                        yield event_data
            except requests.exceptions.ChunkedEncodingError as e:
                print "IncompleteRead"
                print e
                print '='*20

            # Be a nice API citizen, sleep for 1 minute
            print "{} finished, sleeping for 1 minute".format(str(start_date))
            sleep(60)

    def profiles(self):
        page=0
        auth = HTTPBasicAuth(self.token, '')
        session_id = ''
        initial_url = self._generate_engage_url()
        session_id = self._fetch_url(initial_url).json()['session_id']

        while True:
            print 'Getting page: {}'.format(page)

            url = self._generate_engage_url(page, session_id)
            response = self._fetch_url(url)
            data = response.json()
            if 'error' in data:
                raise ValueError(data['error'])

            if 'session_id' in data:
                session_id = data['session_id']
            for profile in data['results']:
                yield profile

            if len(data['results']) == 1000:
                page += 1
            else:
                break

    def _fetch_url(self, url, stream=False):
        auth = HTTPBasicAuth(self.token, '')
        return requests.get(url, auth=auth, stream=stream)

    def _generate_export_url(self, from_date, to_date):
        params = urlencode({
            "from_date": str(from_date),
            "to_date": str(to_date),
            "event": json.dumps(EVENT_NAMES)
        })

        return '?'.join([EXPORT_URL, params])

    def _generate_engage_url(self, page=0, session_id=''):
        behaviors = [{
            'event_selectors': [{'event': 'Sign Up'}],
            'name': 'signup',
            'window': '{}d'.format(TIMEFRAME_DAYS)
        }]

        selector = '(behaviors["signup"] > 0)'

        params = {
            'page': page,
            'session_id': session_id,
            'behaviors': json.dumps(behaviors),
            'selector': selector
        }

        if len(session_id) > 0:
            del params['behaviors']
            del params['selector']
        else:
            del params['session_id']
            del params['page']

        print "Requesting Profiles with params: \t {}".format(json.dumps(params))

        return '?'.join([ENGAGE_URL, urlencode(params)])

if __name__ == '__main__':
    analytics = Analytics(token=environ['HD_MIXPANEL_TOKEN'])
    analytics.fetch_profiles()
