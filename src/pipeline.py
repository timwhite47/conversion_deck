import psycopg2

from os import environ
from database.dynamodb import *
from database.sql import *
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import cpu_count, Pool

STRIPE_TOKEN = environ['HD_STRIPE_TOKEN']
MIXPANEL_TOKEN = environ['HD_MIXPANEL_TOKEN']

TIMEFRAME_DAYS = 180
TIMEFRAME_OFFSET = 90

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
        self.import_datasources()
        self.import_sql()

    def import_datasources(self):
        payments = self.payment_processor
        analytics = self.analytics

        # Import Payment Events into DynamoDB
        for payment_event in payments.events(timeframe_days=TIMEFRAME_DAYS, offset=TIMEFRAME_OFFSET):
            create_payment_event(payment_event)

        # Import Customers into DynamoDB
        for customer in payments.customers():
            create_customer(customer)

        # Import Mixpanel Events into DynamoDB
        for event in analytics.events(days=TIMEFRAME_DAYS, offset=TIMEFRAME_OFFSET):
            create_event(event)

        # Import Mixpanel Profiles into DynamoDB
        for profile in analytics.profiles():
            create_profile(profile)

    def import_sql(self):
        # Load data from DynamoDB into PostgresSQL
        import_sql_payment_events(self.connection)
        import_sql_customers(self.connection)
        import_sql_profiles(self.connection)
        import_sql_events(self.connection)


if __name__ == '__main__':
    print 'Strarting Pipeline'
    pipeline = Pipeline()
    pipeline.run()
