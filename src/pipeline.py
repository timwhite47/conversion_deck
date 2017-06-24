from os import environ
from db import fetch_user_emails
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import cpu_count, Pool

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.analytics = Analytics(token=environ['HD_MIXPANEL_TOKEN'])
        self.payment_processor = Payment(token=environ['HD_STRIPE_TOKEN'])

        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 4

        self.cpus = cpus

    def run(self):
        # self.load_users()
        self.load_events()

    def load_users(self):
        ''' Load users in to DynamoDB from Stripe'''
        payments = self.payment_processor
        payments.import_customers()

    def load_events(self):
        self.analytics.fetch()

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
