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

module_path = os.path.abspath(os.path.join('../conversion_deck'))

if module_path not in sys.path:
    sys.path.append(module_path)

from src.database.sql import psql_connection, pandas_engine
FEATURES = [
    "account_age",
    "camp_deliveries",
    "Slide start",
    "vertical_educator",
    "Editor Opened",
    "Deck Created",
    "vertical_marketing",
    "Started Onboarding",
    "Export",
    "signin",
    "App Became Active",
    "Sign In",
    "View player page",
    "Land on Pricing Page",
    "Share",
    "Ended Onboarding",
    "Display Video Editor Modal",
    "Export PPTX",
    "Recorded Audio",
    "Land on Classroom Page",
    "vertical_real_estate",
    "Export PPT",
    "vertical_student",
    "Display Limit Notification",
    "Land on Zuru Page",
    "Start",
    "Land on Education Page",
    "Set Privacy Restricted",
    "Display Welcome Countdown",
    "Privacy",
    "vertical_coach",
    "vertical_health",
    "vertical_non-profit",
    "vertical_religious_organization",
    "vertical_professional",
    "Land on Checkout Page",
    "Click Button",
    "Downloaded Video",
    "Successfully completed edu signup",
    "Set Privacy Public",
    "vertical_sales",
    "Validation failed",
    "signup",
    "New Deck",
    "vertical_small_business_owner",
    "upgrade",
    "vertical_other",
    "Download PPTX",
    "Successfully completed pro signup",
    "Display Limit Modal",
    "Error on payment",
    "Limit Button",
    "Sign Up",
    "Successfully completed classroom upgrade",
    "Successfully completed edu upgrade",
    "Successfully completed pro upgrade",
    "vertical_hr",
    "vertical_lawyer",
]
MODEL_FILEPATH = 'data/churn_model.pkl'

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

        prediction_df['churn_proba'] = map(lambda prediction: prediction[1], self.predict(prediction_df[FEATURES].values))
        prediction_df = prediction_df[FEATURES+['churn_proba']].drop_duplicates()
        prediction_df.to_sql('churns', pandas_engine(), if_exists='replace')

    def _load_training_dataset(self):
        train_df = self.df.copy()

        self.y = train_df['churned'].values
        self.features = train_df[FEATURES].columns
        self.X = train_df[FEATURES].values

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

    print "Serializing Model"
    with open(MODEL_FILEPATH, 'w') as pkl:
        pickle.dump(clf, pkl)

    print "Uploading Model to S3"
    with open(MODEL_FILEPATH, 'r') as pkl:
        bucket = s3.Bucket('conversion-deck')
        bucket.put_object(Key='models/churn.pkl', Body=pkl)

    print "Making Predictions on current subscribers"
    clf.subscriber_predictions()

    feature_importances = zip(clf.features, clf._clf.feature_importances_)
    feature_importances = sorted(feature_importances, key=lambda tup: tup[1], reverse=True)

    for feature, imporance in feature_importances:
        print "{} \t {}".format(imporance, feature)

    print "Model Score: {}".format(clf.score())

if __name__ == '__main__':
    main()
