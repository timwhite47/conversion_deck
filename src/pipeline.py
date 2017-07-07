import psycopg2
import time

from os import environ
from database.dynamodb import *
from database.sql import *
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import Process, Queue, cpu_count
from modeling.conversion import main as build_conversion_model

STRIPE_TOKEN = environ['HD_STRIPE_TOKEN']
MIXPANEL_TOKEN = environ['HD_MIXPANEL_TOKEN']

TIMEFRAME_DAYS = 7
TIMEFRAME_OFFSET = 0

def work_queue(queue):
    print "Process started"

    while True:
        job = queue.get()

        if not job:
            print "No job in queue"
            time.sleep(1)
            pass

        job_type, job_data = job
        data = json.loads(job_data)

        print "{}: \t Processing Job ({})".format(time.time(), job_type)
        if job_type == 'payment_event':
            create_payment_event(data)
        elif job_type == 'customer':
            create_customer(data)
        elif job_type == 'event':
            create_event(data)
        elif job_type == 'profile':
            create_profile(data)

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
        try:
            # Import Data
            self.import_datasources()
            self.import_sql()

            # Build Models
            build_conversion_model()
            self._terminate_pool()
        except KeyboardInterrupt as e:
            self._terminate_pool()



    def import_datasources(self):
        payments = self.payment_processor
        analytics = self.analytics

        # Import Payment Events into DynamoDB
        for payment_event in payments.events(timeframe_days=TIMEFRAME_DAYS, offset=TIMEFRAME_OFFSET):
            self._add_to_queue('payment_event', json.loads(str(payment_event)))

        # Import Customers into DynamoDB
        for customer in payments.customers():
            self._add_to_queue('customer', json.loads(str(customer)))

        # Import Mixpanel Events into DynamoDB
        for event in analytics.events(days=TIMEFRAME_DAYS, offset=TIMEFRAME_OFFSET):
            self._add_to_queue('event', event)

        # Import Mixpanel Profiles into DynamoDB
        for profile in analytics.profiles():
            self._add_to_queue('profile', profile)

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

    def _terminate_pool(self):
        for p in self.processes:
            print "Killing Process {}".format(p.pid)
            p.terminate()

    def _add_to_queue(self, job_type, data):
        self.queue.put((job_type, json.dumps(data)))

if __name__ == '__main__':
    print 'Strarting Pipeline'
    pipeline = Pipeline()
    pipeline.run()
