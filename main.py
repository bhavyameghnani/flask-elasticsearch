from datetime import datetime
from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch

es = Elasticsearch()
es = Elasticsearch(HOST="http://localhost", PORT=9200)

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():  
    return("Hello world")

@app.route('/create_index', methods=['GET'])
def create_index():
    result = es.indices.create(index='cities', ignore="True")
    return(result)


@app.route('/get_data', methods=['GET'])
def get_data():
    results = es.get(index='cities', doc_type='places', id='my-new-slug')
    return jsonify(results['_source'])


@app.route('/insert_data', methods=['GET'])
def insert_data():
    doc1 = {"city":"New Delhi", "country":"India"}
    doc2 = {"city":"London", "country":"England"}
    doc3 = {"city":"Los Angeles", "country":"USA"}

    #Inserting doc1 in id=1
    result = es.index(index="cities", doc_type="places", id=1, body=doc1)
    #{u'_type': u'places', u'_seq_no': 15, u'_shards': {u'successful': 1, u'failed': 0, u'total': 2}, u'_index': u'cities', u'_version': 16, u'_primary_term': 1, u'result': u'updated', u'_id': u'1'}

    #Inserting doc2 in id=2
    # result 2 = es.index(index="cities", doc_type="places", id=2, body=doc2)
    # #{u'_type': u'places', u'_seq_no': 12, u'_shards': {u'successful': 1, u'failed': 0, u'total': 2}, u'_index': u'cities', u'_version': 13, u'_primary_term': 1, u'result': u'updated', u'_id': u'2'}

    # #Inserting doc3 in id=3
    # result 3 = es.index(index="cities", doc_type="places", id=3, body=doc3)

    # result = es.index(index='contents', doc_type='title', id=slug, body=body)

    return jsonify(result)

@app.route('/search', methods=['GET'])
def search():
    keyword = "Delhi" #request.form['keyword']

    body = {
        "query": {
            "multi_match": {
                "query": keyword,
                "fields": ["city", "country"]
            }
        }
    }

    res = es.search(index="cities", doc_type="places", body=body)

    return jsonify(res['hits']['hits'])

app.run(port=5000, debug=True)