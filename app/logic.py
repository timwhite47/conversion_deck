import pandas as pd
from app import db
from app.models import Conversion, Churn
from queries import CHURN_STATS_QUERY, CONVERSION_STATS_QUERY

PAGE_SIZE = 250

def dashboard_stats(arg):
    conversion_stats = pd.read_sql_query(CONVERSION_STATS_QUERY, db.engine)
    churn_stats = pd.read_sql_query(CHURN_STATS_QUERY, db.engine)

    return pd.concat([conversion_stats, churn_stats], axis=1)

def conversions(args):
    page = _get_page_arg(args)

    records = (
        Conversion.query
        .order_by(
            Conversion.conversion_proba.desc()
        )
        .limit(PAGE_SIZE)
        .offset(page * PAGE_SIZE)
        .all()
    )


    current_page = page
    count = Conversion.query.count()
    pages = count / PAGE_SIZE
    records = _format_response(records)

    return (records, count, pages, current_page)

def churns(args):
    page = _get_page_arg(args)
    records = (
        Churn.query
        .order_by(
            Churn.churn_proba.desc()
        )
        .limit(PAGE_SIZE)
        .offset(page * PAGE_SIZE)
        .all()
    )

    current_page = page
    count = Churn.query.count()
    pages = count / PAGE_SIZE
    records = _format_response(records)

    return (records, count, pages, current_page)

def _get_page_arg(args):
    if not args:
        return 0
    if args.get('page'):
        return int(args.get('page'))
    else:
        return 0

def _format_response(records):
    return map(lambda x: x.__attributes__(), records)
