import requests
import json
import boto3
import botocore
import pandas as pd
from db import create_event, create_profile
from urllib import urlencode
from datetime import date, timedelta
from os import environ
from requests.auth import HTTPBasicAuth

EXPORT_URL = 'https://data.mixpanel.com/api/2.0/export'
ENGAGE_URL = 'https://mixpanel.com/api/2.0/engage'

with open('data/event_names.json') as json_file:
    data = json.load(json_file)
    EVENT_NAMES = map(lambda obj: obj['Name'], data)

class Analytics(object):
    """Data from Mixpanel"""
    def __init__(self, token):
        self.token = token

    def fetch_events(self, days=30, increment=1):
        for num_days in xrange(1, days + 1):
            end_date = date.today() - timedelta(days=num_days)
            start_date = end_date - timedelta(days=increment)
            url = self._generate_export_url(start_date, end_date)
            self._fetch_url(url)

    def fetch_people(self):
        page=0
        auth = HTTPBasicAuth(self.token, '')
        session_id = ''
        while True:
            print 'Getting page: {}'.format(page)

            url = self._generate_engage_url(page, session_id)
            response = requests.get(url, auth=auth)
            data = response.json()
            if data['session_id']:
                session_id = data['session_id']

            [create_profile(profile) for profile in data['results']]

            page += 1

        return True

    def _fetch_url(self, url):
        print "Fetching URL: {}".format(url)
        auth = HTTPBasicAuth(self.token, '')
        response = requests.get(url, auth=auth, stream=True)

        try:
            for line in response.iter_lines():
                if line:
                    event_data = line.decode('utf-8')
                    create_event(event_data)
        except requests.exceptions.ChunkedEncodingError as e:
            print "IncompleteRead"
            print e
            print '='*20

    def _generate_export_url(self, from_date, to_date):
        params = urlencode({
            "from_date": str(from_date),
            "to_date": str(to_date),
            "event": json.dumps(EVENT_NAMES)
        })

        return '?'.join([EXPORT_URL, params])

    def _generate_engage_url(self, page=0, session_id=''):
        params = urlencode({
            'page': page,
            'session_id': session_id
        })
        return '?'.join([ENGAGE_URL, params])

if __name__ == '__main__':
    a = Analytics()
    timeframe = timedelta(days=7)

    end_date = date.today()
    start_date = end_date - timeframe
    a.fetch(start_date, end_date)
