from os import environ
from db import fetch_user_emails
from payments import Payment
from analytics import Analytics
from datetime import date, timedelta
from multiprocessing import cpu_count, Pool, Queue
from time import sleep
MIXPANEL_TOKEN = environ['HD_MIXPANEL_TOKEN']
STRIPE_TOKEN = environ['HD_STRIPE_TOKEN']

def _load_events_for_email(queue):
     a = Analytics(token=MIXPANEL_TOKEN)
     email = queue.get()
     print 'Fetching events for email: '.format(email)
     a.fetch_email(email)

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.analytics = Analytics(token=MIXPANEL_TOKEN)
        self.payment_processor = Payment(token=STRIPE_TOKEN)

        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 4

        self.cpus = cpus

    def run(self):
        self.load_users()
        self.load_events()

    def load_users(self):
        ''' Load users in to DynamoDB from Stripe'''
        payments = self.payment_processor
        payments.import_customers()

    def load_events(self):
        queue = Queue()
        processes = self.cpus*2
        pool = Pool(processes, _load_events_for_email, (queue,))

        for email in fetch_user_emails():
            queue.put(email)
            self.analytics.fetch_email(email)

        while len(queue) > 1:
            sleep(1)

        pool.close()
        return True
if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
