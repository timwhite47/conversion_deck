import json
import boto3
from os import environ
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
TABLE_NAME = 'conversion_deck.users'
USER_TABLE = dynamodb.Table(TABLE_NAME)

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.analytics = Analytics()
        self.payment_processor = Payment(token=environ['HD_STRIPE_TOKEN'])

    def run(self):
        self.load_users()
        self.load_events()

    def load_users(self):
        ''' Load users in to MongoDB from Stripe'''
        payments = self.payment_processor

        for customer in payments.users(limit=100):
            print 'Adding user: {}'.format(customer.email)
            user = json.loads(str(customer))
            item = USER_TABLE.put_item(Item=event)

    def load_events(self):
        timeframe = timedelta(days=7)

        end_date = date.today()
        start_date = end_date - timeframe
        self.analytics.fetch(start_date, end_date)

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
