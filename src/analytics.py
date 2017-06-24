import requests
import json
import boto3
import botocore
from multiprocessing import cpu_count, Pool
from urllib import urlencode
from datetime import date, timedelta
from os import environ
from requests.auth import HTTPBasicAuth

TABLE_NAME = 'conversion_deck.events'
BASE_URL = 'https://data.mixpanel.com/api/2.0/export'
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
TABLE = dynamodb.Table(TABLE_NAME)


class Analytics(object):
    """Data from Mixpanel"""
    def __init__(self):
        self.token = environ['HD_MIXPANEL_TOKEN']

        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 4

        self.cpus = cpus

    def fetch(self, start_date, end_date):
        increment = timedelta(days=1)
        from_date = start_date
        to_date = start_date + increment
        auth = HTTPBasicAuth(self.token, '')
        # pool = Pool(processes=self.cpus)
        
        while from_date <= end_date:
            url = self._generate_url(from_date, to_date)

            print "Fetching URL: {}".format(url)
            response = requests.get(url, auth=auth, stream=True)

            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    event_data = self._parse_entry(decoded_line)
                    self._store_event(event_data)

            from_date = to_date
            to_date = from_date + increment

    def _store_event(self, event):
        try:
            item = TABLE.put_item(Item=event)
            print "Event Stored {}".format(event['event_id'])

            return item
        except botocore.exceptions.ClientError as e:
            print "Could not store event"
            print event
            print e
            print '='*20
        except Exception as e:
            print "Could not store event"
            print event
            print e
            print '='*20

    def _parse_entry(self, entry):
        try:
            event_data = json.loads(entry)
            event_data['event_id'] = event_data['properties']['distinct_id']
            return event_data
        except ValueError as e:
            pass
        except KeyError as e:
            print "Could not parse entry"
            print event_data
            pass

    def _parse_response(self, response):
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
