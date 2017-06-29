from db import USER_TABLE, PROFILES_TABLE, EVENTS_TABLE
from boto3.dynamodb.conditions import Key, Attr

class User(object):
    """
        Representation of HaikuDeck User

        Takes in a `distinct_id` (mixpanel id)
        Loads data from both stripe and mixpanel
        Can load events for given user using `distinct_id`
    """
    def __init__(self, distinct_id):
        self.distinct_id = distinct_id

    def load_data(self):
        self._load_profile_data()
        self._load_customer_data()
        self._load_events()

    def _load_profile_data(self):
        expression = Key('$distinct_id').eq(self.distinct_id)
        response = PROFILES_TABLE.query(KeyConditionExpression=expression)
        self.profile = response['Items'][0]['$properties']

    def _load_customer_data(self):
        email = self.profile['$email']
        expression = Key('email').eq(email)
        response = USER_TABLE.query(KeyConditionExpression=expression)
        self.customer = response['Items'][0]

    def _load_events(self):
        self.events = []

        fe = Key('profile_id').eq(self.distinct_id)
        response = EVENTS_TABLE.scan(FilterExpression=fe)

        while 'LastEvaluatedKey' in response:
            print response['LastEvaluatedKey']
            response = EVENTS_TABLE.scan(
                FilterExpression=fe,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )

            for i in response['Items']:
                self.events.append(i)

        return True
