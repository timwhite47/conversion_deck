import os
import sys
import numpy as np
import pandas as pd
module_path = os.path.abspath(os.path.join('../conversion_deck'))

if module_path not in sys.path:
    sys.path.append(module_path)

from features import CONVERSION as FEATURE_COLUMNS
from queries import CONVERTED_AGE_QUERY, CONVERTED_EVENTS_QUERY, CONVERSION_PREDICTION_QUERY
from src.database.sql import psql_connection, pandas_engine
from sklearn.metrics import precision_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.utils import shuffle
from helpers import serialize_to_s3, determine_top_features

MODEL_FILEPATH = 'data/conversion_model.pkl'
LABEL_COLUMN = 'converted'

class ConversionClassifier(object):
    """docstring for ConversionClassifier."""
    def __init__(self, connection=None, clf=None):
        super(ConversionClassifier, self).__init__()

        if not connection:
            connection = psql_connection()

        if not clf:
            # max_features=sqrt, n_estimators=7500, learning_rate=0.01, max_depth=20, subsample=0.1, score=0.919028, total=  54.2s
            # max_features=log2, n_estimators=7500, learning_rate=0.01, max_depth=15, subsample=0.1, score=0.914980, total=  52.4s
            clf = GradientBoostingClassifier(
                learning_rate=0.01,
                n_estimators=2000,
                verbose=100,
                subsample=0.5,
                max_depth=20,
                max_features=None
            )

        self.connection = connection
        self._clf = clf

    def load_dataset(self, csv=False):
        if csv:
            self.df = pd.read_csv('data/conversion_df.csv')
        else:
            # Load Raw dataframe from sql
            raw_converted_events_df = pd.read_sql_query(CONVERTED_EVENTS_QUERY, self.connection)
            raw_converted_age_df = pd.read_sql_query(CONVERTED_AGE_QUERY, self.connection, index_col='distinct_id')
            converted_events_df = raw_converted_events_df.pivot(index='distinct_id', columns='type', values='count')
            raw_df = converted_events_df.join(raw_converted_age_df)

            # Handle balanacing the classes
            converted_df = raw_df[raw_df['converted'] == True]
            not_converted = raw_df[raw_df['converted'] == False].sample(len(converted_df))

            self.df = pd.concat([converted_df,not_converted])
            self.df.to_csv('data/conversion_df.csv')

        self.df = shuffle(self.df)

        # Set Train/Test Data
        self.X = self.df[FEATURE_COLUMNS].fillna(0)
        self.y = self.df[LABEL_COLUMN]

        (
            self._X_train,
            self._X_test,
            self._y_train,
            self._y_test
        ) = train_test_split(self.X.values, self.y.values)

    def fit(self):
        return self._clf.fit(self._X_train, self._y_train)

    def predict(self, X):
        return self._clf.predict_proba(X)

    def score(self):
        y_true = self._y_test
        y_pred = self._clf.predict(self._X_test)

        return precision_score(y_true, y_pred)

    def non_subscribers_predictions(self):
        conversion_query_df = pd.read_sql_query(CONVERSION_PREDICTION_QUERY, self.connection)
        conversion_df = (
            conversion_query_df
                .pivot(index='distinct_id', columns='type', values='count')
                .fillna(0)
        )

        blank_columns = set(FEATURE_COLUMNS) - set(conversion_df.columns)
        for col in blank_columns:
            conversion_df[col] = 0
        X = conversion_df[FEATURE_COLUMNS].values

        conversion_df['conversion_proba'] = map(
            lambda proba: proba[1],
            self.predict(X)
        )

        conversion_df[FEATURE_COLUMNS+['conversion_proba']].to_sql('conversions', pandas_engine(), if_exists='replace')

    def __getstate__(self):
        return { 'df': self.df, '_clf': self._clf }

def grid_search():
    parameters = {
        'learning_rate':(0.01, 0.001,),
        'n_estimators':[1000, 2500, 5000],
        'subsample': [1, 0.5, 0.25, 0.1],
        'max_depth': (5,10, 20,),
        'max_features': (None,'sqrt', 'log2',)
    }
    gbclf = GradientBoostingClassifier()
    grid_clf = GridSearchCV(gbclf, parameters, n_jobs=-1, verbose=100, scoring='precision')
    clf = ConversionClassifier(clf=grid_clf)
    print "Loading Dataset"
    clf.load_dataset(csv=True)
    print "Starting Grid Search"
    clf.fit()

    print "Grid Search Best Params"
    print grid_clf.best_params_
    print grid_clf.best_score_

def main():
    print "Starting Converstion Classifier"
    clf = ConversionClassifier()

    print "Loading dataset"
    clf.load_dataset(csv=True)

    print "Fitting model with {} rows".format(len(clf._X_train))
    clf.fit()

    serialize_to_s3(clf, MODEL_FILEPATH, 'models/conversion.pkl')

    print "Making Predictions on Basic users"
    clf.non_subscribers_predictions()

    determine_top_features(clf, FEATURE_COLUMNS, 'data/conversion_features.json')

    print "Model Score: {}".format(clf.score())


if __name__ == '__main__':
    # grid_search()
    main()
