import json
from datetime import timedelta

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, QueryOptions)
import couchbase.subdocument as SD
from couchbase.management import buckets
from couchbase.management.buckets import CreateBucketSettings
import numpy as np
import pandas as pd
from flask import Flask, render_template, request, jsonify
import csv

app = Flask(__name__)
cb_coll = None
cluster = None


new_tweet = {
    "posts": {
    "author": "Oliver Hummel",
    "content": "Hyper Hyper, 1,0 für Gruppe G",
    "country": "",
    "date_time": "11 / 01 / 2017 08:38",
    "id": "8.19101E+17",
    "language": "en",
    "latitude": "",
    "longitude": "",
    "number_of_likes": "6969",
    "number_of_shares": "1380"
    }
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


@app.route('/')
def index():  # put application's code here

    # Get a reference to our cluster
    # NOTE: For TLS/SSL connection use 'couchbases://<your-ip-address>' instead
    global cluster
    cluster = Cluster('couchbase://172.17.0.4', ClusterOptions(auth))

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))

    # get a reference to our bucket

    cb = cluster.bucket(bucket_name)
    global cb_coll
    cb_coll = cb.scope("_default").collection("posts")

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
    cluster = setup_cluster()

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
    cluster = setup_cluster()

    bucket_name = "Tweets"
    cb = cluster.bucket(bucket_name)
    cb_coll_lokal = cb.scope("_default").collection("new_accounts")
    with open(filename, 'r') as f:
        data = json.load(f)
        for ele in data:
            cb_coll_lokal.upsert(ele["user_id"],ele)



@app.route('/query_top_100')
def query_top_100():
    cluster = setup_cluster()

    query = "select user_id,ARRAY_LENGTH(followers_id) from Tweets._default.new_accounts order by ARRAY_LENGTH(followers_id) desc limit 100"
    val = lookup_query(cluster, query)
    return val

@app.route('/query_custom')
def query_custom():
    cluster = setup_cluster()

    query = request.args.get("myquery")
    val = lookup_query(cluster, query)
    return val   

@app.route('/contains_word')
def contains_word():
    cluster = setup_cluster()

    query = request.args.get("words")
    querystring = "select * from Tweets._default.posts where content LIKE"
    searchwords = query.split(" ")
    for i in range (len(searchwords)):
        if i == 0:
            querystring = querystring +  " \"%"+searchwords[i]+'%\"' 
        else:
            querystring = querystring + " AND content LIKE" + " \"%"+searchwords[i]+'%\"' 

    querystring = querystring + " order by TO_NUMBER(number_of_likes) desc limit 25"
    val = lookup_query(cluster, querystring)

    return val 
    #return render_template('searchresults.html', title="page", jsonfile=json.dumps(val))
    #return jsonify(val)
  

@app.route('/create_index')
def create_index_user():
    cluster = setup_cluster()

    index_queries = []
    index_queries.append("CREATE PRIMARY INDEX `#primary` ON Tweets._default.new_accounts USING GSI;")
    index_queries.append("CREATE PRIMARY INDEX `#primary` ON Tweets._default.posts USING GSI;")
    index_queries.append("CREATE PRIMARY INDEX `#primary` ON Tweets._default.starting_page_cache USING GSI;")

    for string in index_queries:

        try:
            cluster.query(string).rows()            
        except Exception as e:
            return '<h1>' + str(e) + '</h1>'


    return '<h1>' + "Indexes created" + '</h1>'




@app.route('/create_bucket')
def createbucket():
    cluster = setup_cluster()

    try: 
        cb = cluster.bucket("Tweets")
        exist = 1

    except:
        exist = 0

    if exist ==0:
        try:
            cb_buckets = cluster.buckets()
            cb_buckets.create_bucket(CreateBucketSettings(name="Tweets", bucket_type="couchbase", ram_quota_mb=300))
            return "Bucket created successfully"
        except Exception as e:
            return '<h1>' + str(e) + '</h1>'
        
    if exist==1:
            return "already exists"

@app.route('/starting_page')
def starting_page():
    cluster = setup_cluster()

    current_user = request.args.get("current_user")
    cb_cache = cluster.bucket("Tweets").scope("_default").collection("starting_page_cache")    
    try:
        old_value = cb_cache.get(current_user)
        
    except:
        old_value = None

    if old_value:
       cache_query = "select starting_page_cache[-25:] from Tweets._default.starting_page_cache USE KEYS "+ "\""+current_user+"\""
       val= lookup_query(cluster, cache_query)
     
      

    else:
        val = build_starting_page(current_user)

    return str(val)


def build_starting_page(current_user):
    cluster = setup_cluster()

    current_user_clean = current_user
    current_user  = '"' + current_user + '"'
    
    try:
        num_followers_query = "select ARRAY_LENGTH(followers_id) from Tweets._default.new_accounts where user_id = " + str(current_user)
        row_iter = cluster.query(
            num_followers_query)
        res_list = []
        for row in row_iter:
            res_list.append(row)
        followers = str(res_list[0]["$1"])

    except Exception as e:
        return '<h1>' + str(e) + '</h1>'

    try:
        num_following_query = "select ARRAY_LENGTH(following_id) from Tweets._default.new_accounts where user_id = " + str(
            current_user)
        row_iter = cluster.query(
            num_following_query)
        res_list = []
        for row in row_iter:
            res_list.append(row)
        following = str(res_list[0]["$1"])
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'

    try:
        following_query = "select following_id from Tweets._default.new_accounts where user_id = " + str(
            current_user)
        row_iter = cluster.query(
            following_query)
        res_list = []
        for row in row_iter:
            res_list.append(row)
        following_accs = res_list[0]["following_id"]
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'

    try:
        posts_query = "select * from Tweets._default.posts  where user_id in " + str(following_accs) + "order by TO_NUMBER(number_of_likes) desc limit 25"
        row_iter = cluster.query(
            posts_query)
        res_list = []
        for row in row_iter:
            res_list.append(row)
        posts= res_list
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'

    
    cb = cluster.bucket("Tweets")
    cb_coll_lokal = cb.scope("_default").collection("starting_page_cache")

    for ele in posts:
        try:
            old_value = cb_coll_lokal.get(current_user_clean)
        except:
            old_value = None

        if not old_value:
            cb_coll_lokal.upsert(current_user_clean, [ele])

        else:
            cb_coll_lokal.mutate_in(current_user_clean, (SD.array_append("", ele),))    


    return '<h1>' + str(posts) + '</h1>'


def lookup_query(cluster, query):
    cluster = setup_cluster()

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


@app.route('/followers')
def upload_followers():
    val = get_most_followers()
    return '<h1>'+ str(val) +'</h1>'


@app.route('/create_post')
def create_post():
    cluster = setup_cluster()

    user_id = request.args.get("user_post")
    user_id = "\""+user_id+"\""

    try:
        following_query = "select following_id from Tweets._default.new_accounts where user_id = " + str(user_id)
        row_iter = cluster.query(
            following_query)
        res_list = []
        for row in row_iter:
            res_list.append(row)
        following_accs = res_list[0]["following_id"]
    except Exception as e:
        return '<h1>' + str(e) + '</h1>'

    cb_cache = cluster.bucket("Tweets").scope("_default").collection("starting_page_cache")
    for ele in following_accs:
        
        try:
            old_value = cb_cache.get(str(ele))
            
        except:
            old_value = None

        if old_value:
            cb_cache.mutate_in(ele, (SD.array_append("", new_tweet),)) 
              

    return "geklappt"

  


def get_most_followers():
    cluster = setup_cluster()

    acc_100 = []
    all_followers = []
    sql_query_100 = "select user_id from Tweets._default.new_accounts order by ARRAY_LENGTH(following_id) desc limit 100"
    sql_query_followers = "select following_id from Tweets._default.new_accounts"
    row_iter_100 = cluster.query(sql_query_100)
    row_iter_followers = cluster.query(sql_query_followers)
    for row in row_iter_100:
        acc_100.append(row)
    for row in row_iter_followers:
        all_followers.append(row)

    acc_100_df = pd.DataFrame.from_dict(acc_100)
    acc_100_list = acc_100_df.user_id.to_list()

    follow_data = pd.DataFrame.from_dict(all_followers)
    df_test = follow_data.explode('following_id')
    df1 = df_test[df_test.following_id.isin(acc_100_list)].value_counts().to_frame()

    #Zurückspeichern
    df1 = df1.rename_axis('user_id').reset_index()
    df1.columns = ['user_id', 'occourens']
    return pd.DataFrame.to_json(df1)
    # return df1


def setup_cluster():
    cluster = Cluster('couchbase://172.17.0.4', ClusterOptions(auth))

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))

    # get a reference to our bucket

    cb = cluster.bucket(bucket_name)
    global cb_coll
    cb_coll = cb.scope("_default").collection("posts")
    return cluster
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")