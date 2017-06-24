import stripe
from os import environ
from db import create_user

class Payment(object):
    """docstring for Payment."""
    def __init__(self, token):
        self.token = token

    def customers(self, limit=None):
        customers = stripe.Customer.list(api_key=self.token, limit=limit)

        for customer in customers.auto_paging_iter():
            yield customer

    def import_customers(self):
        for customer in self.customers():
            create_user(customer)


if __name__ == '__main__':
    token = environ['HD_STRIPE_TOKEN']
    p = Payment(token)
    for user in p.users(limit=10):
        print user
