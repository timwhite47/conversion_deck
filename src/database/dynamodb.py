import boto3
import botocore
import json
from os import environ
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
from time import sleep
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

CUSTOMERS_TABLE_NAME = 'conversion_deck.users'
EVENT_TABLE_NAME = 'conversion_deck.events'
PROFILE_TABLE_NAME = 'conversion_deck.profiles'
PAYMENT_EVENT_TABLE_NAME = 'conversion_deck.payment_event'

CUSTOMERS_TABLE = dynamodb.Table(CUSTOMERS_TABLE_NAME)
EVENTS_TABLE = dynamodb.Table(EVENT_TABLE_NAME)
PROFILES_TABLE = dynamodb.Table(PROFILE_TABLE_NAME)
PAYMENT_EVENT_TABLE = dynamodb.Table(PAYMENT_EVENT_TABLE_NAME)

def fetch_customers():
    return _fetch_from_table(CUSTOMERS_TABLE)

def fetch_events():
    return _fetch_from_table(EVENTS_TABLE)

def fetch_profiles():
    return _fetch_from_table(PROFILES_TABLE)

def fetch_payment_events():
    return _fetch_from_table(PAYMENT_EVENT_TABLE)

def create_payment_event(payment_event):
    payment_event['event_id'] = payment_event['id']
    payment_event = _sanitize_dynamodb(payment_event)

    return _create_object(PAYMENT_EVENT_TABLE, payment_event)

def create_event(event):
    event = _parse_event(event)
    event = _sanitize_dynamodb(event)

    return _create_object(EVENTS_TABLE, event)

def create_profile(profile):
    profile = _sanitize_dynamodb(profile)

    return _create_object(PROFILES_TABLE, profile)

def create_customer(customer):
    customer = _sanitize_dynamodb(customer)

    return _create_object(CUSTOMERS_TABLE, customer)

def _fetch_from_table(table):
    response = table.scan()

    for item in response['Items']:
        yield item

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

        for item in response['Items']:
            yield item

        sleep(0.1)

def _create_object(table, item):
    try:
        return table.put_item(Item=item)
    except KeyboardInterrupt as e:
        raise e
    except botocore.exceptions.ParamValidationError as e:
        print "Could not store data"
        print data
        print e
        print '='*20

def _parse_event_id(properties):
    return ':'.join([
        properties['distinct_id'],
        str(properties['time'])
    ])

def _parse_event(entry):
    try:
        event_data = json.loads(entry)
        properties = event_data['properties']
        event_data['profile_id'] = properties['distinct_id']
        event_data['event_id'] = _parse_event_id(properties)

        return event_data
    except ValueError as e:
        pass
    except KeyError as e:
        pass

def _sanitize_dynamodb(data):
    """ Sanitizes an object so it can be updated to dynamodb (recursive) """
    if not data and isinstance(data, (basestring, set)):
        new_data = None  # empty strings/sets are forbidden by dynamodb
    elif isinstance(data, (basestring, bool)):
        new_data = data  # important to handle these one before sequence and int!
    elif isinstance(data, dict):
        new_data = {key: _sanitize_dynamodb(data[key]) for key in data}
    elif isinstance(data, list):
        new_data = [_sanitize_dynamodb(item) for item in data]
    elif isinstance(data, set):
        new_data = {_sanitize_dynamodb(item) for item in data}
    elif isinstance(data, (float, int, long, complex)):
        new_data = Decimal(str(data))
    else:
        new_data = data
    return new_data
