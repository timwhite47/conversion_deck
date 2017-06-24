import json
import boto3
import decimal
from os import environ
from datetime import date, timedelta
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

USER_TABLE_NAME = 'conversion_deck.users'
EVENT_TABLE_NAME = 'conversion_deck.events'

USER_TABLE = dynamodb.Table(USER_TABLE_NAME)
EVENTS_TABLE = dynamodb.Table(EVENT_TABLE_NAME)

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
        new_data = Decimal(data)
    else:
        new_data = data
    return new_data

def fetch_user_emails():
    response = USER_TABLE.scan(Select='SPECIFIC_ATTRIBUTES',AttributesToGet=['email'])
    return [obj['email'] for obj in response['Items']]

def create_user(stripe_customer):
    user = json.loads(str(stripe_customer))
    user = _sanitize_dynamodb(user)
    try:
        print "Adding User: {}".format(user['email'])
        USER_TABLE.put_item(Item=user)
    except KeyboardInterrupt as e:
        raise e
    except Exception as e:
        _print_error(e, user, USER_TABLE)
        pass

def create_event(event):
    event = _parse_event(event)
    event = _sanitize_dynamodb(event)
    # if not event:
    #     return
    try:
        # print "Adding Event: {}".format(event['event_id'])
        return EVENTS_TABLE.put_item(Item=event)
    except KeyboardInterrupt as e:
        raise e
    # except Exception as e:
    #     _print_error(e, event, EVENTS_TABLE)

def _print_error(e, data, table_name):
    print "Could not store data in {}".format(table_name)
    print data
    print e
    print '='*20

def _parse_event(entry):
    try:
        event_data = json.loads(entry)
        event_data['event_id'] = event_data['properties']['distinct_id']
        return event_data
    except ValueError as e:
        pass
    except KeyError as e:
        pass
