import os
import sys
import numpy as np
import pandas as pd

module_path = os.path.abspath(os.path.join('../conversion_deck'))

if module_path not in sys.path:
    sys.path.append(module_path)


from queries import CONVERTED_AGE_QUERY, CONVERTED_EVENTS_QUERY
from src.database.sql import psql_connection
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier

FEATURE_COLUMNS = [
    'App Became Active',
    # 'Click Button',
    # 'Click Link',
    # 'Client error',
    # 'Countdown Pro Button',
    'Deck Created',
    'Display Limit Modal',
    'Display Limit Notification',
    # 'Display Video Editor Modal',
    'Display Welcome Countdown',
    # 'Display Zuru Upgrade Modal',
    # 'Download PPTX',
    # 'Downloaded Video',
    'Editor Opened',
    'Ended Onboarding',
    # 'Error on payment',
    # 'Error on payment (stripe)',
    # 'Error on payment (stripe, reporting to user)',
    'Export',
    'Export PPT',
    'Export PPTX',
    # 'Land on Checkout Page',
    'Land on Classroom Page',
    'Land on Education Page',
    'Land on Homepage',
    'Land on Pricing Page',
    'Land on Zuru Page',
    'Limit Button',
    'Limit Modal Edu Button',
    'Limit Modal Pro Button',
    'New Deck',
    'Privacy',
    'Recorded Audio',
    'Set Privacy Public',
    'Set Privacy Restricted',
    'Share',
    'Sign In',
    'Sign Up',
    'Slide start',
    'Start',
    'Started Onboarding',
    # 'Successfully completed classroom upgrade',
    # 'Successfully completed edu signup',
    # 'Successfully completed edu upgrade',
    # 'Successfully completed pro signup',
    # 'Successfully completed pro upgrade',
    # 'Validation failed',
    'View player page',
    # 'Zuru Upgrade Edu Button',
    # 'cancel',
    'signin',
    'vertical',
    'camp_deliveries'
    # 'account_age'
]

LABEL_COLUMN = 'converted'

class ConversionClassifier(object):
    """docstring for ConversionClassifier."""
    def __init__(self, connection=None):
        super(ConversionClassifier, self).__init__()

        if not connection:
            connection = psql_connection()

        self.connection = connection
        self._clf = GradientBoostingClassifier(
            # learning_rate=0.001,
            # n_estimators=5000,
            verbose=100,
            # max_depth=5
        )

    def load_dataset(self):
        # Load Raw dataframe from sql
        raw_converted_events_df = pd.read_sql_query(CONVERTED_EVENTS_QUERY, self.connection)
        raw_converted_age_df = pd.read_sql_query(CONVERTED_AGE_QUERY, self.connection, index_col='distinct_id')
        converted_events_df = raw_converted_events_df.pivot(index='distinct_id', columns='type', values='count')
        self.raw_df = converted_events_df.join(raw_converted_age_df).fillna(0)
        self.df = self.raw_df

        # Set Vertical Dummies
        vertical_dummies = pd.get_dummies(self.df['vertical'], prefix='vertical')
        self.df = self.df.join(vertical_dummies)
        self.df.drop('vertical', axis=1, inplace=True)

        # Set Train/Test Data
        self.X = self.df[FEATURE_COLUMNS].values
        self.y = self.df[LABEL_COLUMN].values
        self._X_train, self._X_test, self._y_train, self._y_test = train_test_split(self.X, self.y)

    def fit(self):
        return self._clf.fit(self._X_train, self._y_train)

    def predict(X):
        return self._clf.predict(X)

    def score(self):
        return self._clf.score(self._X_test, self._y_test)

if __name__ == '__main__':
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    print "Starting Converstion Classifier"
    clf = ConversionClassifier()

    print "Loading dataset"
    clf.load_dataset()

    print "Fitting model with {} rows".format(len(clf._X_train))
    clf.fit()
    # import ipdb; ipdb.set_trace()
    feature_importances = zip(FEATURE_COLUMNS, clf._clf.feature_importances_)
    feature_importances = sorted(feature_importances, key=lambda tup: tup[1])

    for feature, imporance in feature_importances:
        print "{} \t {}".format(feature, imporance)

    print "Model Score: {}".format(clf.score())
