from os import sys, path
import boto3
import cPickle as pickle
import json

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
s3 = boto3.resource('s3')
BUCKET = s3.Bucket('conversion-deck')

def serialize_to_s3(clf, model_filepath, s3_key):
    print "Serializing Model: \t {}".format(model_filepath)
    with open(model_filepath, 'w') as pkl:
        pickle.dump(clf, pkl)

    print "Uploading model to S3"
    with open(model_filepath, 'r') as pkl:
        BUCKET.put_object(Key=s3_key, Body=pkl)

def determine_top_features(clf, features, file_path):
    feature_importances = zip(features, clf._clf.feature_importances_)
    feature_importances = sorted(
        feature_importances,
        key=lambda tup: tup[1],
        reverse=True
    )

    with open(file_path, 'w') as features_file:
        json.dump(dict(feature_importances), features_file, indent=4)

    for feature, imporance in feature_importances:
        print "{} \t {}".format(imporance, feature)
