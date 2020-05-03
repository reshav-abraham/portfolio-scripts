import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account

import json
import logging

def pull_store_data(event, context):
	date_tag = datetime.today().strftime('%m%d%Y')[:-2]
	data = requests.get('http://eoddata.com/stocklist/NYSE.htm').content
	soup = BeautifulSoup(data)
	table = soup.find_all('table')[5]
	table_rows = table.find_all('tr')
	header = list(map(lambda x:x.text, table_rows[0].find_all('th')))
	eod_data = []
	for t in table_rows[1:]:
	    table_cells = t.find_all('td')
	    data = map(lambda x:x.text, table_cells)
	    data_row = list(data)[0:9]
	    data_row[6] = f"{data_row[6]}::{data_row[8]}"
	    eod_data.append(data_row[:7])
	df = pd.DataFrame(eod_data, columns = header[:-1]) 
	store_data(df)
	return

def store_data(df):
	cred = json.loads(os.environ["GCP_KEY"])
	credentials = service_account.Credentials.from_service_account_info(cred) 
	gcs = storage.Client(project="pmgmt-275419", credentials=credentials)
	f = StringIO()
	df.to_csv(f)
	f.seek(0)
	gcs.get_bucket('old-data-1').blob('nyse.csv').upload_from_file(f, content_type='text/csv')
	return

	
