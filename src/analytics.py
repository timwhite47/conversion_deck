import requests
import json
import boto3
from urllib import urlencode
from datetime import date, timedelta
from os import environ
from requests.auth import HTTPBasicAuth

TABLE_NAME = 'conversion_deck.events'
BASE_URL = 'https://data.mixpanel.com/api/2.0/export'

class Analytics(object):
    """Data from Mixpanel"""
    def __init__(self):
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

        self.token = environ['HD_MIXPANEL_TOKEN']
        self.table = dynamodb.Table(TABLE_NAME)

    def fetch(self, start_date, end_date):
        increment = timedelta(days=1)
        from_date = start_date
        to_date = start_date + increment
        auth = HTTPBasicAuth(self.token, '')

        while from_date <= end_date:
            url = self._generate_url(from_date, to_date)

            print "Fetching URL: {}".format(url)
            response = requests.get(url, auth=auth)

            event_data = self._parse_response(response)
            [self._store_event(event) for event in event_data]
            print "{} events stored".format(len(event_data))

            from_date = to_date
            to_date = from_date + increment

    def _store_event(self, event):
        return self.table.put_item(
           Item=event
        )

    def _parse_entry(self, entry):
        try:
            event_data = json.loads(entry)
            event_data['event_id'] = event_data['properties']['distinct_id']
        except ValueError as e:
            pass
        except KeyError as e:
            print "Could not parse entry"
            print event_data
            pass

    def _parse_response(self, response):
        # import ipdb; ipdb.set_trace()
        entries = response.text.split('\n')
        return map(self._parse_entry, entries)

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
