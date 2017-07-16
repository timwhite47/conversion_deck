import boto3
import botocore
from helpers import BUCKET

def download_models():
    model_types = ['churn', 'conversion']

    try:
        for m_type in model_types:
            key = 'models/{}.pkl'.format(m_type)
            target = 'data/{}_model.pkl'.format(m_type)
            print "Downloading {} to {}".format(key, target)
            BUCKET.download_file(key, target)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
