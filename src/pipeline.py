import psycopg2
import time

from os import environ
from database.dynamodb import *
from database.sql import *
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import Process, Queue, cpu_count

STRIPE_TOKEN = environ['HD_STRIPE_TOKEN']
MIXPANEL_TOKEN = environ['HD_MIXPANEL_TOKEN']

TIMEFRAME_DAYS = 180
TIMEFRAME_OFFSET = 90

def work_queue(queue):
    print "Process started"
    
    while True:
        try:
            job_type, data = queue.get()

            print "{}: \t Processing Job ({})".format(time.time(), job_type)
            if job_type == 'payment_event':
                create_payment_event(data)
            elif job_type == 'customer':
                create_customer(data)
            elif job_type == 'event':
                create_event(data)
            elif job_type == 'profile':
                create_profile(profile)
        except KeyboardInterrupt as e:
            break
        except Exception as e:
            print e
            pass
class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.connection = psql_connection()
        self.analytics = Analytics(token=MIXPANEL_TOKEN)
        self.payment_processor = Payment(token=STRIPE_TOKEN)

        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 2

        self.queue = Queue()
        self.processes = list()

        for i in xrange(cpus):
            print "Worker Process #{} Initialized".format(i)

            p = Process(target=work_queue, args=(self.queue,))
            p.start()
            self.processes.append(p)

    def run(self):
        self.import_datasources()
        self.import_sql()

    def import_datasources(self):
        payments = self.payment_processor
        analytics = self.analytics

        # Import Payment Events into DynamoDB
        for payment_event in payments.events(timeframe_days=TIMEFRAME_DAYS, offset=TIMEFRAME_OFFSET):
            self.queue.put(('payment_event', payment_event))

        # Import Customers into DynamoDB
        for customer in payments.customers():
            self.queue.put(('customer', customer))

        # Import Mixpanel Events into DynamoDB
        for event in analytics.events(days=TIMEFRAME_DAYS, offset=TIMEFRAME_OFFSET):
            self.queue.put(('event', event))

        # Import Mixpanel Profiles into DynamoDB
        for profile in analytics.profiles():
            self.queue.put(('profile', profile))

        while not self.queue.empty():
            sleep(1)

    def import_sql(self):
        # Load data from DynamoDB into PostgresSQL
        print "{} \t Importing Payment Events to SQL".format(time.time())
        import_sql_payment_events(self.connection)

        print "{} \t Importing Customers to SQL".format(time.time())
        import_sql_customers(self.connection)

        print "{} \t Importing Profiles to SQL".format(time.time())
        import_sql_profiles(self.connection)

        print "{} \t Importing Events to SQL".format(time.time())
        import_sql_events(self.connection)


if __name__ == '__main__':
    print 'Strarting Pipeline'
    pipeline = Pipeline()
    pipeline.run()
