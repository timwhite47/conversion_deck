import psycopg2

from os import environ
from db import import_sql_profiles, import_sql_events, psql_connection, import_sql_customers
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import cpu_count, Pool

STRIPE_TOKEN = environ['HD_STRIPE_TOKEN']
MIXPANEL_TOKEN = environ['HD_MIXPANEL_TOKEN']

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.connection = psql_connection()
        self.analytics = Analytics(token=MIXPANEL_TOKEN)
        self.payment_processor = Payment(token=STRIPE_TOKEN)

        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 4

        self.cpus = cpus

    def run(self):
        self.load_customers()
        self.load_profiles()
        self.load_events()


    def load_profiles(self):
        print "Loading Profiles"
        cursor = self.connection.cursor()

        self.analytics.fetch_profiles()
        import_sql_profiles(cursor)
        self.connection.commit()

    def load_customers(self):
        ''' Load users in to DynamoDB from Stripe'''
        print "Loading Users"
        cursor = self.connection.cursor()
        payments = self.payment_processor
        payments.import_customers()

        import_sql_customers(cursor)
        self.connection.commit()

    def load_events(self):
        cursor = self.connection.cursor()

        self.analytics.fetch_events()
        import_sql_events(cursor)
        self.connection.commit()

if __name__ == '__main__':
    print 'Strarting Pipeline'
    pipeline = Pipeline()
    pipeline.run()
