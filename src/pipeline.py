from os import environ
from db import fetch_user_emails
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.analytics = Analytics(token=environ['HD_MIXPANEL_TOKEN'])
        self.payment_processor = Payment(token=environ['HD_STRIPE_TOKEN'])

    def run(self):
        self.load_users()
        self.load_events()

    def load_users(self):
        ''' Load users in to DynamoDB from Stripe'''
        payments = self.payment_processor
        payments.import_customers()

    def load_events(self):
        timeframe = timedelta(days=28)

        end_date = date.today()
        start_date = end_date - timeframe
        for email in fetch_user_emails():
            self.analytics.fetch_email(email, start_date, end_date)

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
