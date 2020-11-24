import getopt
import argparse
import json
import sys
import datetime
import os
from os import environ
from sodapy import Socrata
import datetime
from requests import get
#from elasticsearch import Elasticsearch, RequestsHttpConnection
#from requests_aws4auth import AWS4Auth
from requests.auth import HTTPBasicAuth
import requests
#client=Socrata(
#    'data.cityofnewyork.us',
#    'JEHitsOV8R2iLu8Wzih1mni1n')
#token = os.getenv('APP_KEY')
es_host=environ["es_host"]
es_username=environ["es_username"]
es_password=environ["es_password"]
APP_TOKEN=environ["APP_TOKEN"]
dataid = environ["dataset_id"]
client = Socrata("data.cityofnewyork.us",APP_TOKEN,timeout = 60 )
parser = argparse.ArgumentParser(description='Process data from payroll.')
parser.add_argument('--page_size', type=int,
                    help='how many rows to get per page', required=True)
parser.add_argument('--num_pages',
                    type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])

"""
def get_data(page_size, idx=None) ->dict:
	try:
		if idx is None:
			res = client.get(dataid, limit=page_size)
		else:
			res = client.get(dataid, limit=page_size, offset=idx*page_size)
		return res
	except Exception as e:
		print(f"Exception occured {e}")
		raise

		
def create_index(index_name):
    es = Elasticsearch(connection_class = RequestsHttpConnection)
    try:
        es.indices.create(index=index_name)
    except Exception:
        pass
    
page_s = 2
num_p = 0
fn = None 	
	
options, remainder = getopt.getopt(sys.argv[1:],'',['page_size=','num_pages=','output='])
options, remainder = getopt.getopt(sys.argv[1:],'',['page_size=','num_pages=','output='])
for opt, arg in options:
    if opt == '--page_size':
        page_s = arg
    elif opt == '--num_pages':
        num_p = int(arg)
        if num_p <=0:
            raise Exception("num_pages should larger than 0") 
    elif opt == '--output':
        fn = arg
if num_p <= 0:
    res = get_data(page_s)
else:
    res = []
    for i in range(num_p):
        r = get_data(page_s, i)
        res.extend(r)
"""
try:

        resp = requests.put(
            # this is the URL to create parking "index"
            # which is our elasticsearch database/table
            f"{es_host}/parking1",
            auth=HTTPBasicAuth(es_username, es_password),
            # these are the "columns" of this database/table
            json={
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 1
                 },
                "mappings": {
                    "properties": {
                        "plate": {"type": "text"},
                        "state": {"type": "text"},
                        "license_type": {"type": "text"},
                        "summons_number": {"type": "integer"},
                        "issue_date": {"type": "date"},
                        "violation_time": {"type": "date"},
                        "violation": {"type": "text"},
                        "fine_amount": {"type": "int"},
                        "penalty_amount": {"type": "float"},
                        "interest_amount": {"type": "float"},
                        "reduction_amount": {"type": "float"},
                        "payment_amount": {"type": "float"},
                        "amount_due": {"type": "float"},
                        "precinct": {"type": "integer"},
                        "county": {"type": "text"},
                        "summons_image": {"type": "ip"},
                        "description": {"type": "text"},
                    }
                },
            })
        resp.raise_for_status()
        print(resp.json())
except Exception:
        print("Index already exists! Skipping")	
client = Socrata(
        "data.cityofnewyork.us",
        APP_TOKEN,
    )
for i in range(0,args.num_pages):
    

    rows = client.get(dataid, limit=args.page_size,offset=i*(args.num_pages))
        
    for row in rows:
        try:
            # convert
            row["summons_number"] = int(row["summons_number"])
            row["fine_amount"] = int(row["fine_amount"])
            row["penalty_amount"] = float(row["penalty_amount"])
            row["interest_amount"] = float(row["interest_amount"])
            row["reduction_amount"] = float(row["reduction_amount"])
            row["payment_amount"] = float(row["payment_amount"])
            row["amount_due"] = float(row["amount_due"])
        except Exception as e:
            print(f"Error!: {e}, skipping row: {row}")
            continue

        try:
            # upload to elasticsearch by creating a doc
            resp = requests.post(
                # this is the URL to create a new payroll document
                # which is our "row" in elasticsearch databse/table
                f"{es_host}/parking1/_doc",
                json=row,
                auth=HTTPBasicAuth(es_username, es_password),
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"Failed to insert in ES: {e}, skipping row: {row}")
            continue
        
        print(resp.json())
