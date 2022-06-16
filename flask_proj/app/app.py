import json
from datetime import timedelta

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, QueryOptions)
import couchbase.subdocument as SD

import numpy as np
from flask import Flask, render_template, request
import csv

app = Flask(__name__)
cb_coll = None
cluster = None
@app.route('/')
def index():  # put application's code here

    tweet = {
        "author": "katyperry",
        "content": "@barackobama Thank you for your incredible grace in leadership and for being an exceptional… https://t.co/ZuQLZpt6df",
        "country": "",
        "date_time": "11 / 01 / 2017 08:38",
        "id": 8.19101E+17,
        "language": "en",
        "latitude": "",
        "longitude": "",
        "number_of_likes": 6969,
        "number_of_shares": 1380
    }

    # Update this to your cluster
    username = "admin"
    password = "123456"
    bucket_name = "Tweets"
    cert_path = "path/to/certificate"
    # User Input ends here.

    # Connect options - authentication
    auth = PasswordAuthenticator(
        username,
        password,
        # NOTE: If using SSL/TLS, add the certificate path.
        # We strongly reccomend this for production use.
        # cert_path=cert_path
    )

    # Get a reference to our cluster
    # NOTE: For TLS/SSL connection use 'couchbases://<your-ip-address>' instead
    global cluster
    cluster = Cluster('couchbase://172.17.0.4', ClusterOptions(auth))

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))

    # get a reference to our bucket

    cb = cluster.bucket(bucket_name)
    global cb_coll
    cb_coll = cb.scope("_default").collection("_default")

    # Get a reference to the default collection, required for older Couchbase server versions
    cb_coll_default = cb.default_collection()
    print("hallo Docker")

    return render_template('index.html')

@app.route('/upload')
def upload_file():
    csv_import('../app/ressources/tweets.csv')
    return  '<h1>test</h1>'

@app.route('/upload_account_new')
def upload_json_file():
    json_import('../app/ressources/data.json')
    return '<h1>test</h1>'


def csv_import(filename):
    try:
        sql_query = "select user_id from Tweets._default.new_accounts order by ARRAY_LENGTH(followers_id) desc limit 100"
        row_iter = cluster.query(
            sql_query)
        user_list = []
        for row in row_iter:
            user_list.append(row)
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile)
        for idx, row in enumerate(reader):
            row["user_id"] = str(user_list[np.random.randint(0, 99)]["user_id"])
            cb_coll.upsert(str(idx), row)


def json_import(filename):
    bucket_name = "Tweets"
    cb = cluster.bucket(bucket_name)
    cb_coll_lokal = cb.scope("_default").collection("new_accounts")
    with open(filename, 'r') as f:
        data = json.load(f)
        for ele in data:
            cb_coll_lokal.upsert(ele["user_id"],ele)

@app.route('/upload_con')
def upload_connections():
    txt_import('ressources/twitter_combined.csv')
    return '<h1>test</h1>'


def txt_import(filename):
    bucket_name = "Tweets"
    cb = cluster.bucket(bucket_name)
    cb_coll_lokal = cb.scope("_default").collection("accounts")
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=' ')
        for row in reader:

            try:
                old_value = cb_coll_lokal.get(row["user"])
            except:
                old_value = None
            if not old_value:
                cb_coll_lokal.upsert(row["user"], [row["follow"]])
            else:
                cb_coll_lokal.mutate_in(row["user"], (SD.array_append("", row["follow"]),))


@app.route('/query_top_100')
def query_top_100():
<<<<<<< HEAD
    query = "select user_id,ARRAY_LENGTH(followers_id) from Tweets._default.new_accounts order by ARRAY_LENGTH(followers_id) desc limit 10"
=======
    query = "select user_id,ARRAY_LENGTH(followers_id) from Tweets._default.new_accounts order by ARRAY_LENGTH(followers_id) desc limit 100"

>>>>>>> 9607410d6e9d0c2b7e92c44f244c189a0fb4e6c9
    val = lookup_query(cluster, query)
    return val

@app.route('/query_custom')
def query_custom():
    query = request.args.get("myquery")
    val = lookup_query(cluster, query)
    return val   

@app.route('/create_index_user')
def create_index_user():
    qstr= "CREATE PRIMARY INDEX `#primary` ON Tweets._default.new_accounts USING GSI;"
    try:
        ret = cluster.query(qstr).rows()
        return '<h1>' + "Index for Users created" + '</h1>'
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'

@app.route('/create_index_tweets')
def create_index_tweets():
    qstr= "CREATE PRIMARY INDEX `#primary` ON Tweets USING GSI;"
    try:
        ret = cluster.query(qstr).rows()
        return '<h1>' + "Index for Tweets created" + '</h1>'
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'

  

def lookup_query(cluster, query):
    print("\nLookup Result: ")
    try:
        sql_query = query
        row_iter = cluster.query(
            sql_query)
        test_string = ""
        for row in row_iter:
            test_string = test_string + str(row)
        test_string = '<h1>' + test_string + '</h1>'
        return test_string
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")