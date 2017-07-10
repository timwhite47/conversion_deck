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

from app import logic

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/conversions')
def conversions():
    return render_template('index.html')

@app.route('/churns')
def churns():
    return render_template('index.html')

@app.route('/api/conversions')
def api_conversions():
    return jsonify({"data": logic.conversions(request.args)})

@app.route('/api/churns')
def api_churns():
    return jsonify({"data": logic.churns(request.args)})

if __name__ == '__main__':
    app.run(debug=True)