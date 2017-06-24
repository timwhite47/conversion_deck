import requests
import json
import boto3
import botocore
from db import create_event
from urllib import urlencode
from datetime import date, timedelta
from os import environ
from requests.auth import HTTPBasicAuth

BASE_URL = 'https://data.mixpanel.com/api/2.0/export'

class Analytics(object):
    """Data from Mixpanel"""
    def __init__(self, token):
        self.token = token

    def fetch_email(self, email, start_date=None, end_date=None):
        if not end_date:
            end_date = date.today() - timedelta(days=1)

        if not start_date:
            start_date = date(2016,1,1)

        url = self._generate_url(email, start_date, end_date)

    def _fetch_url(self, url):
        print "Fetching URL: {}".format(url)
        auth = HTTPBasicAuth(self.token, '')
        response = requests.get(url, auth=auth, stream=True)

        for line in response.iter_lines():
            if line:
                event_data = line.decode('utf-8')
                create_event(event_data)

    def _generate_url(self, email, from_date, to_date):
        params = urlencode({
            "from_date": str(from_date),
            "to_date": str(to_date),
            "where": 'properties["$email"] == "{}"'.format(email)
        })

        return '?'.join([BASE_URL, params])

if __name__ == '__main__':
    a = Analytics()
    timeframe = timedelta(days=7)

    end_date = date.today()
    start_date = end_date - timeframe
    a.fetch(start_date, end_date)
