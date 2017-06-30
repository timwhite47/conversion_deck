import psycopg2

from os import environ
from db import import_sql_profiles, import_sql_events
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import cpu_count, Pool

STRIPE_TOKEN = environ['HD_STRIPE_TOKEN']
MIXPANEL_TOKEN = environ['HD_MIXPANEL_TOKEN']
PSQL_HOST = environ['CD_PSQL_HOST']
PSQL_PW = environ['CD_PSQL_PASSWORD']
PSQL_USER = environ['CD_PSQL_USERNAME']
PSQL_DB = environ['CD_PSQL_DB']

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.connection = psycopg2.connect(dbname=PSQL_DB, host=PSQL_HOST, user=PSQL_USER, password=PSQL_PW)
        self.analytics = Analytics(token=MIXPANEL_TOKEN)
        self.payment_processor = Payment(token=STRIPE_TOKEN)

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
        print "Loading Profiles"
        cursor = self.connection.cursor()

        self.analytics.fetch_profiles()
        import_sql_profiles(cursor)
        self.connection.commit()

    def load_users(self):
        ''' Load users in to DynamoDB from Stripe'''
        print "Loading Users"
        payments = self.payment_processor
        payments.import_customers()

    def load_events(self):
        cursor = self.connection.cursor()

        self.analytics.fetch_events()
        import_sql_events(cursor)
        self.connection.commit()

if __name__ == '__main__':
    print 'Strarting Pipeline'
    pipeline = Pipeline()
    pipeline.run()
