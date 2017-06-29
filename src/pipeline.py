import psycopg2

from os import environ
from db import fetch_user_emails, import_sql_profiles, import_sql_events
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import cpu_count, Pool

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.connection = psycopg2.connect(dbname='conversion_deck', host='localhost')
        self.analytics = Analytics(token=environ['HD_MIXPANEL_TOKEN'])
        self.payment_processor = Payment(token=environ['HD_STRIPE_TOKEN'])

        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 4

        self.cpus = cpus

    def run(self):
        # self.load_users()
        self.load_profiles()
        self.load_events()


    def load_profiles(self):
        cursor = self.connection.cursor()

        self.analytics.fetch_profiles()
        import_sql_profiles(cursor)
        self.connection.commit()

    def load_users(self):
        ''' Load users in to DynamoDB from Stripe'''
        payments = self.payment_processor
        payments.import_customers()

    def load_events(self):
        cursor = self.connection.cursor()

        self.analytics.fetch_events()
        import_sql_events(cursor)
        self.connection.commit()

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
