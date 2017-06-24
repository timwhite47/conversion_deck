import json
import boto3
import decimal
from os import environ
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
TABLE_NAME = 'conversion_deck.users'
USER_TABLE = dynamodb.Table(TABLE_NAME)

def replace_decimals(obj):
    if isinstance(obj, list):
        for i in xrange(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.iterkeys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

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
            user = replace_decimals(user)
            try:
                USER_TABLE.put_item(Item=user)
            except Exception as e:
                import ipdb; ipdb.set_trace()
                raise e

    def load_events(self):
        timeframe = timedelta(days=7)

        end_date = date.today()
        start_date = end_date - timeframe
        self.analytics.fetch(start_date, end_date)

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
