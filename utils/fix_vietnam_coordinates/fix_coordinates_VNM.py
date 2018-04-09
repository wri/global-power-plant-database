# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
fix_coordinates_VNM.py
Convert coordinates in VNM data from minutes/seconds to decimal.
"""

import requests
import sys, os

sys.path.insert(0, os.pardir)
sys.path.insert(0, os.path.join(os.pardir, os.pardir))
import powerplant_database as pw

# params
API_KEY_FILE = pw.make_file_path(fileType = "resource", subFolder = "api_keys", filename = "fusion_tables_api_key.txt")
TABLE_ID = "10WHpc9fcqZzV0kxoKdsKjdvLY21MPMmcQ6dLjYUT"
URL = "https://www.googleapis.com/fusiontables/v2/query"

# retrieve table
with open(API_KEY_FILE, 'r') as f:
    API_KEY = f.readline()
payload = {}
payload['alt'] = 'csv'
payload['sql'] = "SELECT Latitude FROM " + TABLE_ID
payload['key'] = API_KEY
response = requests.post(URL,payload)
print("Current table data:")
print(response.text)

# update single cell
payload = {}
payload['alt'] = 'csv'
payload['sql'] = "UPDATE " + TABLE_ID + " SET Latitude = 0.99 WHERE ROWID = 7"
payload['key'] = API_KEY
response = requests.post(URL,payload)
print("Response for table update:")
print(response.text)
