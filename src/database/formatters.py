from datetime import datetime

def format_sql_customer(obj):
    customer = {
        "identifier": obj['id'],
        "email": obj['email'],
        "delinquent": obj['delinquent'],
        "created_at": datetime.fromtimestamp(obj['created'])
    }

    subscription = None
    plan = None


    # Setup Subscription used by user
    if len(obj['subscriptions']['data']):
        s = obj['subscriptions']['data'][0]
        subscription = {
            "identifier": s['id'],
            "plan_id": s['plan']['id'],
            "customer_id": s['customer']
        }

    # Setup plan user is on
    if subscription:
        p = s['plan']
        plan = {
            "identifier": p['id'],
            "amount": p['amount'],
            "interval": p['interval']
        }

    if len(obj['sources']['data']):
        c = obj['sources']['data'][0]
        card = {
            "identifier":c['id'],
            "customer_id":c['customer']
        }

    return customer, subscription, plan
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
def format_sql_payment_event(payment_event):
    customer_id = None

    if 'customer' in payment_event['data']['object']:
        customer_id = payment_event['data']['object']['customer']

    return {
        "identifier": payment_event['id'],
        "customer_id": customer_id,
        "type": payment_event['type'],
        "time": datetime.fromtimestamp(payment_event['created'])
    }

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
