import stripe
from datetime import timedelta, datetime
from os import environ

class Payment(object):
    """docstring for Payment."""
    def __init__(self, token):
        self.token = token

    def customers(self, limit=None):
        customers = stripe.Customer.list(api_key=self.token, limit=limit)

        for customer in customers.auto_paging_iter():
            yield customer

    def events(self, timeframe_days=90, offset=0):
        start_date = datetime.today() - timedelta(days=offset)
        end_date = start_date - timedelta(days=timeframe_days)
        end_date = int(end_date.strftime("%s"))

        events = stripe.Event.list(api_key=self.token, created={
            "gt": end_date,
            "lt": start_date
        })

        for event in events.auto_paging_iter():
            yield event
