from os import environ
from flask import Flask, render_template, jsonify, request
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask(__name__)

PSQL_HOST = environ['CD_PSQL_HOST']
PSQL_PW = environ['CD_PSQL_PASSWORD']
PSQL_USER = environ['CD_PSQL_USERNAME']
PSQL_DB = environ['CD_PSQL_DB']

app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://{}:{}@{}/{}".format(PSQL_USER, PSQL_PW, PSQL_HOST, PSQL_DB)
db = SQLAlchemy(app)

from app.logic import conversions, churns

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/api/conversions')
def api_conversions():
    print request.args
    return jsonify({"data": conversions(request.args)})

@app.route('/api/churns')
def api_churns():
    return jsonify({"data": churns(request.args)})

if __name__ == '__main__':
    app.run(debug=True)
