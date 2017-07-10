from app import db
from app.models import Conversion, Churn

PAGE_SIZE = 100

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

    return _format_response(records)

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
    return _format_response(records)

def _get_page_arg(args):
    if not args:
        return 0
    if args.get('page'):
        return int(args.get('page'))
    else:
        return 0

def _format_response(records):
    return map(lambda x: x.__attributes__(), records)
