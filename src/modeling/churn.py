import os
import sys
import boto3
import numpy as np
import pandas as pd
import cPickle as pickle
import pandas as pd

from queries import CHURNED_EVENT_QUERY, CHURNED_AGE_QUERY, CHURN_PREDICTION_QUERY
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.utils import shuffle
from helpers import serialize_to_s3, determine_top_features
module_path = os.path.abspath(os.path.join('../conversion_deck'))

if module_path not in sys.path:
    sys.path.append(module_path)

from src.database.sql import psql_connection, pandas_engine
from features import CHURN as FEATURE_COLUMNS
MODEL_FILEPATH = 'data/churn_model.pkl'
LABEL_COLUMN = 'churned'

class ChurnClassifier(object):
    """docstring for ChurnClassifier."""
    def __init__(self, connection=None):
        super(ChurnClassifier, self).__init__()

        if not connection:
            connection = psql_connection()

        self.connection = connection
        self._clf = GradientBoostingClassifier(
            learning_rate=0.001,
            n_estimators=2500,
            verbose=100,
            max_depth=12,
            max_features='sqrt',
        )

    def load_dataset(self):
        events_df = self._load_events_dataframe()
        age_df = self._load_age_dataframe()
        self.df = events_df.join(age_df, how='inner')
        self._load_training_dataset()

    def fit(self):
        return self._clf.fit(self._X_train, self._y_train)

    def predict(self, X):
        return self._clf.predict_proba(X)

    def score(self):
        return self._clf.score(self._X_test, self._y_test)

    def subscriber_predictions(self):
        query_df = pd.read_sql_query(CHURN_PREDICTION_QUERY, self.connection)
        ages = query_df[['distinct_id', 'account_age', 'camp_deliveries']]
        vertical_dummies = pd.get_dummies(query_df['vertical'], prefix='vertical')
        ages = pd.concat([ages, vertical_dummies], axis=1).set_index('distinct_id')

        events = query_df.pivot(index='distinct_id', columns='type', values='count')
        prediction_df = events.join(ages).fillna(0)

        prediction_df['churn_proba'] = map(lambda prediction: prediction[1], self.predict(prediction_df[FEATURE_COLUMNS].values))
        prediction_df = prediction_df[FEATURE_COLUMNS+['churn_proba']].drop_duplicates()
        prediction_df.to_sql('churns', pandas_engine(), if_exists='replace')

    def _load_training_dataset(self):
        train_df = self.df.copy()

        self.y = train_df[LABEL_COLUMN].values
        self.features = train_df[FEATURE_COLUMNS].columns
        self.X = train_df[FEATURE_COLUMNS].values

        (
            self._X_train,
            self._X_test,
            self._y_train,
            self._y_test
        ) =  train_test_split(self.X, self.y)

    def _load_events_dataframe(self):
        events_df = pd.read_sql_query(CHURNED_EVENT_QUERY, self.connection)
        events_df = events_df.pivot(index='distinct_id', columns='type', values='count')

        return events_df.fillna(0)

    def _load_age_dataframe(self):
        age_df = pd.read_sql_query(CHURNED_AGE_QUERY, self.connection, index_col='distinct_id')
        vertical_dummies = pd.get_dummies(age_df['vertical'], prefix='vertical')
        df = pd.concat([age_df, vertical_dummies], axis=1)
        df.drop('vertical', inplace=True, axis=1)

        return df

    def __getstate__(self):
        return { 'df': self.df, '_clf': self._clf }

def main():
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    s3 = boto3.resource('s3')

    print "Starting Churn Classifier"
    clf = ChurnClassifier()

    print "Loading dataset"
    clf.load_dataset()

    print "Fitting model with {} rows".format(len(clf._X_train))
    clf.fit()

    # serialize_to_s3(clf, MODEL_FILEPATH, 'models/churn.pkl')

    # print "Making Predictions on current subscribers"
    # clf.subscriber_predictions()

    determine_top_features(clf, FEATURE_COLUMNS, 'data/churn_features.json')
    print "Model Score: {}".format(clf.score())

if __name__ == '__main__':
    main()
