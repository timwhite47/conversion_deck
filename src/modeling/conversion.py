import os
import sys
import numpy as np
import pandas as pd
import cPickle as pickle
module_path = os.path.abspath(os.path.join('../conversion_deck'))

if module_path not in sys.path:
    sys.path.append(module_path)

import boto3
from queries import CONVERTED_AGE_QUERY, CONVERTED_EVENTS_QUERY
from src.database.sql import psql_connection
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.utils import shuffle

MODEL_FILEPATH = 'data/conversion_model.pkl'
FEATURE_COLUMNS = [
    'App Became Active',
    'Countdown Pro Button',
    'Deck Created',
    'Display Limit Modal',
    'Display Limit Notification',
    'Display Video Editor Modal',
    'Display Zuru Upgrade Modal',
    'Download PPTX',
    'Downloaded Video',
    'Editor Opened',
    'Ended Onboarding',
    'Export',
    'Export PPT',
    'Export PPTX',
    'Land on Classroom Page',
    'Land on Education Page',
    'Land on Homepage',
    'Land on Pricing Page',
    'Land on Realtor Page',
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
    'Slide start',
    'Start',
    'Started Onboarding',
    # 'Validation failed',
    'View player page',
    'Zuru Upgrade Edu Button',
    'Zuru Upgrade Pro Button',
    'signin',
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
            learning_rate=0.001,
            n_estimators=250,
            verbose=100,
            max_depth=7,
        )

    def load_dataset(self):
        # Load Raw dataframe from sql
        raw_converted_events_df = pd.read_sql_query(CONVERTED_EVENTS_QUERY, self.connection)
        raw_converted_age_df = pd.read_sql_query(CONVERTED_AGE_QUERY, self.connection, index_col='distinct_id')
        converted_events_df = raw_converted_events_df.pivot(index='distinct_id', columns='type', values='count')
        raw_df = converted_events_df.join(raw_converted_age_df).fillna(0)

        # Handle balanacing the classes
        converted_df = raw_df[raw_df['converted'] == True]
        not_converted = raw_df[raw_df['converted'] == False].sample(len(converted_df))
        self.df = pd.concat([converted_df,not_converted])
        self.df = shuffle(self.df)

        # Set Train/Test Data
        self.X = self.df[FEATURE_COLUMNS]
        self.y = self.df[LABEL_COLUMN]

        (
            self._X_train,
            self._X_test,
            self._y_train,
            self._y_test
        ) = train_test_split(self.X.values, self.y.values)

    def fit(self):
        return self._clf.fit(self._X_train, self._y_train)

    def predict(X):
        return self._clf.predict(X)

    def score(self):
        return self._clf.score(self._X_test, self._y_test)

    def __getstate__(self):
        return { 'df': self.df, '_clf': self._clf }

def main():
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    s3 = boto3.resource('s3')

    print "Starting Converstion Classifier"
    clf = ConversionClassifier()

    print "Loading dataset"
    clf.load_dataset()

    print "Fitting model with {} rows".format(len(clf._X_train))
    clf.fit()

    with open(MODEL_FILEPATH, 'w') as pkl:
        pickle.dump(clf, pkl)

    with open(MODEL_FILEPATH, 'r') as pkl:
        bucket = s3.Bucket('conversion-deck')
        bucket.put_object(Key='models/conversion.pkl', Body=pkl)

    feature_importances = zip(FEATURE_COLUMNS, clf._clf.feature_importances_)
    feature_importances = sorted(feature_importances, key=lambda tup: tup[1], reverse=True)

    for feature, imporance in feature_importances:
        print "{} \t {}".format(imporance, feature)

    print "Model Score: {}".format(clf.score())


if __name__ == '__main__':
    main()
