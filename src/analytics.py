import requests
import json
from urllib import urlencode
from datetime import date, timedelta
from os import environ
from requests.auth import HTTPBasicAuth

BASE_URL = 'https://data.mixpanel.com/api/2.0/export'

class Analytics(object):
    """Data from Mixpanel"""
    def __init__(self):
        self.token = environ['HD_MIXPANEL_TOKEN']

    def fetch(self, start_date, end_date):
        increment = timedelta(days=1)
        from_date = start_date
        to_date = start_date + increment
        auth = HTTPBasicAuth(self.token, '')

        while from_date <= end_date:
            url = self._generate_url(from_date, to_date)
            print "Fetching URL: {}".format(url)
            response = requests.get(url, auth=auth)
            data = self._parse_response(response)

            import ipdb; ipdb.set_trace()

            from_date = to_date
            to_date = from_date + increment

    def _parse_response(self, response):
        return response.text.split('\n')

    def _generate_url(self, from_date, to_date):
        params = urlencode({
            "from_date": str(from_date),
            "to_date": str(to_date),
        })

        return '?'.join([BASE_URL, params])

if __name__ == '__main__':
    a = Analytics()
    start_date = date(2017, 6, 1)
    end_date = date.today()
    a.fetch(start_date, end_date)
