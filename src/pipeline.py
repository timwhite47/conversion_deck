import json
from pymongo import MongoClient
from os import environ
from payments import Payment

DB_NAME = 'conversion_deck'
USER_COL_NAME = 'users'

class Pipeline(object):
    """Pull data from data sources into MongoDB"""

    def __init__(self):
        self.mongodb = MongoClient()[DB_NAME]
        self.payment_processor = Payment(token=environ['HD_STRIPE_TOKEN'])

    def run(self):
        self.load_users()

    def load_users(self):
        ''' Load users in to MongoDB from Stripe'''
        payments = self.payment_processor
        users = self.mongodb[USER_COL_NAME]

        for customer in payments.users(limit=100):
            print 'Adding user: {}'.format(customer.email)
            user = json.loads(str(customer))
            users.insert_one(user)

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
