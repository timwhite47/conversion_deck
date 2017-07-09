import psycopg2
from os import environ
from psycopg2.extensions import AsIs
from dynamodb import fetch_customers, fetch_profiles, fetch_events, fetch_payment_events
from formatters import format_sql_customer, format_sql_event, format_sql_profile, format_sql_payment_event
from sqlalchemy import create_engine

PSQL_HOST = environ['CD_PSQL_HOST']
PSQL_PW = environ['CD_PSQL_PASSWORD']
PSQL_USER = environ['CD_PSQL_USERNAME']
PSQL_DB = environ['CD_PSQL_DB']

def psql_connection():
    return psycopg2.connect(
        dbname=PSQL_DB,
        host=PSQL_HOST,
        user=PSQL_USER,
        password=PSQL_PW
    )
def pandas_engine():
    url = "postgresql://{}:{}@{}/{}".format(PSQL_USER, PSQL_PW, PSQL_HOST, PSQL_DB)
    return create_engine(url)
def import_sql_payment_events(connection):
    _import_sql(connection, fetch_payment_events(), insert_sql_payment_event)

def import_sql_customers(connection):
    for customer in fetch_customers():
        insert_sql_customer(connection, customer)

def import_sql_profiles(connection):
    _import_sql(connection, fetch_profiles(), insert_sql_profile)

def import_sql_events(connection):
    _import_sql(connection, fetch_events(), insert_sql_event)

def _import_sql(connection, fetcher, insert):
    for item in fetcher:
        try:
            cursor = connection.cursor()
            insert(cursor, item)
            connection.commit()
        except psycopg2.IntegrityError as e:
            connection.rollback()


def insert_sql_payment_event(cursor, payment_event):
    data = format_sql_payment_event(payment_event)
    return _insert_sql(cursor, 'payment_events', data)

def insert_sql_customer(connection, customer):
    tables = ['customers', 'subscriptions', 'plans', 'card']
    data = format_sql_customer(customer)
    objs = zip(tables, data)

    for table, data in objs:
        cur = connection.cursor()

        if data:
            columns = data.keys()
            values = [data[column] for column in columns]
            insert_statement = 'insert into {} (%s) values %s'.format(table)

            try:
                cur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))
                connection.commit()
            except psycopg2.IntegrityError as e:
                connection.rollback()



def insert_sql_event(cursor, event):
    data = format_sql_event(event)
    return _insert_sql(cursor, 'events', data)

def insert_sql_profile(cursor, profile):
    data = format_sql_profile(profile)
    return _insert_sql(cursor, 'users', data)

def _insert_sql(cursor, table, data):
    columns = data.keys()
    values = [data[column] for column in columns]
    insert_statement = 'insert into {} (%s) values %s'.format(table)

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

        CREATE TABLE customers (
            identifier varchar(255),
            email varchar(255),
            delinquent boolean,
            identifier varchar(255),
            created_at timestamp
        );

        CREATE UNIQUE INDEX customer_email_idx ON customers (email);

        CREATE TABLE subscriptions (
            plan_id varchar(255),
            customer_id varchar(255),
            identifier varchar(255)
        )

        CREATE UNIQUE INDEX subscription_identifier_idx ON subscriptions (identifier);

        CREATE TABLE cards (
            identifier varchar(255),
            customer_id varchar(255)
        )

        CREATE UNIQUE INDEX card_identifier_idx ON cards (identifier);

        CREATE TABLE plans (
            identifier varchar(255),
            amount int,
            interval varchar(255)
        )

        CREATE UNIQUE INDEX plan_identifier_idx ON plans (identifier);

        CREATE TABLE payment_events (
            identifier varchar(255),
            customer_id varchar(255),
            type varchar(255),
            time timestamp
        )

        CREATE UNIQUE INDEX payment_events_identifier_idx ON payment_events (identifier);

    '''

    return cursor.execute(query)
