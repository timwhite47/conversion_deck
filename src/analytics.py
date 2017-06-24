import requests
import json
import boto3
import botocore
from db import create_event
from multiprocessing import cpu_count, Pool
from urllib import urlencode
from datetime import date, timedelta
from os import environ
from requests.auth import HTTPBasicAuth

BASE_URL = 'https://data.mixpanel.com/api/2.0/export'

def _fetch_url(url):
    print "Fetching URL: {}".format(url)
    auth = HTTPBasicAuth(environ['HD_MIXPANEL_TOKEN'], '')
    response = requests.get(url, auth=auth, stream=True)

    for line in response.iter_lines():
        if line:
            event_data = line.decode('utf-8')
            create_event(event_data)

class Analytics(object):
    """Data from Mixpanel"""
    def __init__(self, token):
        self.token = token

        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 4

        self.cpus = cpus

    def fetch(self, start_date, end_date):
        increment = timedelta(days=1)
        from_date = start_date
        to_date = start_date + increment
        pool = Pool(processes=self.cpus)
        urls = list()
        while from_date <= end_date:
            url = self._generate_url(from_date, to_date)
            urls.append(url)

            from_date = to_date
            to_date = from_date + increment

        pool.map(_fetch_url, urls)

    def _generate_url(self, from_date, to_date):
        params = urlencode({
            "from_date": str(from_date),
            "to_date": str(to_date),
        })

        return '?'.join([BASE_URL, params])

if __name__ == '__main__':
    a = Analytics()
    timeframe = timedelta(days=7)

    end_date = date.today()
    start_date = end_date - timeframe
    a.fetch(start_date, end_date)
