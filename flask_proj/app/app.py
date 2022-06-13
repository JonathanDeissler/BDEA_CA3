from flask import Flask, render_template

from datetime import timedelta

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)


app = Flask(__name__)


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
    # upsert document function
    def upsert_document(doc, cb_coll):
        print("\nUpsert CAS: ")
        try:
            # key will equal: "airline_8091"
            key = doc["author"] + "_" + str(doc["id"])
            result = cb_coll.upsert(key, doc)
            return result.cas
        except Exception as e:
            return (e)

    # get document function

    def get_airline_by_key(key, cb_coll):
        print("\nGet Result: ")
        try:
            result = cb_coll.get(key)
            return (result.content_as[str])
        except Exception as e:
            print(e)

    # query for new document by callsign

    def lookup_by_callsign(cs, cluster):
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT VALUE name FROM `travel-sample`.inventory.airline WHERE callsign = $1'
            row_iter = cluster.query(
                sql_query,
                QueryOptions(positional_parameters=[cs]))
            for row in row_iter:
                print(row)
        except Exception as e:
            print(e)
    # Update this to your cluster
    username = "Administrator"
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
    cluster = Cluster('couchbase://172.17.0.4', ClusterOptions(auth))

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))

    # get a reference to our bucket
    cb = cluster.bucket(bucket_name)

    cb_coll = cb.scope("_default").collection("_default")

    # Get a reference to the default collection, required for older Couchbase server versions
    cb_coll_default = cb.default_collection()
    print("hollo Docker")

    val = upsert_document(tweet, cb_coll)
    print(val)
    val = get_airline_by_key(tweet["author"] + "_" + str(tweet["id"]),cb_coll)

    #lookup_by_callsign("CBS",cluster)

    return render_template('index.html')

@app.route('/action_page')
def action_page(input=None):
    return "<h1>Biber in Braun nicht in lila</h1>"
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")