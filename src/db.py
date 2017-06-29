import json
import boto3
import decimal
import botocore

from collections import defaultdict
from boto3.dynamodb.conditions import Key, Attr
from os import environ
from datetime import date, timedelta, datetime
from decimal import Decimal
from time import sleep
from sqlalchemy.exc import IntegrityError
from psycopg2.extensions import AsIs
from dateutil import parser

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

USER_TABLE_NAME = 'conversion_deck.users'
EVENT_TABLE_NAME = 'conversion_deck.events'
PROFILE_TABLE_NAME = 'conversion_deck.profiles'

USER_TABLE = dynamodb.Table(USER_TABLE_NAME)
EVENTS_TABLE = dynamodb.Table(EVENT_TABLE_NAME)
PROFILES_TABLE = dynamodb.Table(PROFILE_TABLE_NAME)

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

def import_sql_profiles(cursor):
    for profile in fetch_profiles():
        try:
            insert_sql_profile(cursor, profile)
        except IntegrityError:
            # Duplicate key error
            pass

def import_sql_events(cursor):
    for event in fetch_events():
        try:
            insert_sql_event(event)
        except IntegrityError as e:
            # Duplicate key error
            pass

def fetch_events():
    response = EVENTS_TABLE.scan()
    for event in response['Items']:
        yield event

    while 'LastEvaluatedKey' in response:
        response = EVENTS_TABLE.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

        for event in response['Items']:
            yield event

        sleep(0.1)
def fetch_profiles():
    response = PROFILES_TABLE.scan()

    for profile in response['Items']:
        yield profile

    while 'LastEvaluatedKey' in response:
        response = PROFILES_TABLE.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

        for profile in response['Items']:
            yield profile

        sleep(0.1)

def format_sql_event(event):
    ts = None

    if 'time' in event['properties']:
        ts = datetime.fromtimestamp(event['properties']['time'])

    return {
        'type': event['event'],
        'event_id': event['event_id'],
        'distinct_id': event['profile_id'],
        'time': ts
    }

def insert_sql_event(cur, event):
    data = format_sql_event(event)
    columns = data.keys()
    values = [data[column] for column in columns]
    insert_statement = 'insert into events (%s) values %s'

    return cur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))

def format_sql_profile(profile):
    data = profile['Item']
    properties = data['$properties']

    return {
        "distinct_id": data['$distinct_id'],
        "camp_count": len(properties['$campaigns']),
        "camp_deliveries": len(properties['$deliveries']),
        "email": properties['$email'],
        "first_name": properties['$first_name'],
        "last_name": properties['$last_name'],
        "is_paying": properties['isPaying'],
        'is_registered': properties['isRegistered'],
        'signup_at': parser.parse(properties['signupDate']),
        'vertical': properties['vertical'],
        'subscription_type': properties['subscriptionType']
    }

def insert_sql_profile(cursor, profile):
    data = format_sql_profile(profile)
    columns = data.keys()
    values = [data[column] for column in columns]
    insert_statement = 'insert into users (%s) values %s'

    cursor.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))

def create_sql_tables(cursor):
    query = '''
        CREATE TABLE users (
            distinct_id varchar(75),
            camp_count int,
            camp_deliveries int,
            email varchar(255),
            first_name varchar(255),
            last_name varchar(255),
            is_paying boolean,
            is_registered boolean,
            signup_at date,
            vertical varchar(255),
            country_code varchar(10),
            subscription_type varchar(255)
        );

        CREATE UNIQUE INDEX distinct_idx ON users (distinct_id);
        CREATE TABLE events (
            type varchar(255),
            time timestamp,
            distinct_id varchar(75),
            event_id varchar(255)
        );

        CREATE UNIQUE INDEX event_id_idx ON events (event_id);
    '''

    return cursor.execute(query)

def format_sql_profile(profile):
    properties = profile['$properties']
    properties = defaultdict(str, properties)

    signup_at = None
    vertical = None
    subscription_type = None
    camp_count = 0
    camp_deliveries = 0
    is_paying = False
    is_registered = False

    if 'signupDate' in properties:
        signup_at = parser.parse(properties['signupDate'])

    if 'vertical' in properties:
        vertical = properties['vertical']

    if 'subscriptionType' in properties:
        subscription_type = properties['subscriptionType']

    if '$campaigns' in properties:
        camp_count = len(properties['$campaigns'])

    if '$deliveries' in properties:
        camp_deliveries = len(properties['$deliveries'])

    if 'isPaying' in properties:
        is_paying = properties['isPaying']

    if 'isRegistered' in properties:
        is_registered = properties['isRegistered']

    return {
        "distinct_id": profile['$distinct_id'],
        "camp_count": camp_count,
        "camp_deliveries": camp_deliveries,
        "email": properties['$email'],
        "first_name": properties['$first_name'],
        "last_name": properties['$last_name'],
        "is_paying": is_paying,
        'is_registered': is_registered,
        'signup_at': signup_at,
        'vertical': vertical,
        'subscription_type': subscription_type,
    }

def format_sql_event(arg):
    pass

def events_for_profile_ids(profile_ids):
    expression = Attr('profile_id').is_in(profile_ids)
    response = EVENTS_TABLE.scan(FilterExpression=expression)
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = EVENTS_TABLE.scan(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            FilterExpression=expression
        )
        data.extend(response['Items'])
        sleep(0.1)

    return data

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
    try:
        return EVENTS_TABLE.put_item(Item=event)
    except KeyboardInterrupt as e:
        raise e
    except botocore.exceptions.ParamValidationError as e:
        _print_error(e, event, EVENTS_TABLE)

def create_profile(profile):
    profile = _sanitize_dynamodb(profile)

    try:
        # print "Adding Profile: {}".format(profile['$distinct_id'])
        return PROFILES_TABLE.put_item(Item=profile)
    except KeyboardInterrupt as e:
        raise e
    except botocore.exceptions.ParamValidationError as e:
        _print_error(e, profile, PROFILES_TABLE)

def _print_error(e, data, table_name):
    print "Could not store data in {}".format(table_name)
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
