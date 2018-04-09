# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
get_fusion_table_sources.py
Get source information used in fusion tables.
"""

import os
import csv
import sys
import httplib2
import json

from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.crypt import Signer
from apiclient.discovery import build
KEYFILE = '../../resources/api_keys/fusion_tables_service_key.json'
COUNTRY_FILE = '../../resources/country_information.csv'
SOURCE_FILE = './all_sources.csv'
ENCODING = 'utf-8'

scopes = ['https://www.googleapis.com/auth/fusiontables']
credentials = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE, scopes=scopes)
http_auth = credentials.authorize(httplib2.Http())

# build fusion table API
ft_api = build('fusiontables', 'v2', http_auth)
# get an object that can build API query requests
query_api = ft_api.query()

def get_rows_with_sources(ftid):
    """Query a fusion table and return a list of [ROWID, Source, URL]."""
    q = query_api.sql(sql="SELECT ROWID, Source, URL from {ftid}".format(ftid=ftid), hdrs=True, typed=False).execute()
    return q['rows']

# read the country information file
# [ ['primary_country_name','iso_country_code','iso_country_code2','has_api','use_geo','nation','geo_country','carma_country','idea_country','google_country','fusion_table_id'], [...], ...]
with open(COUNTRY_FILE) as fin:
    reader = csv.DictReader(fin)
    all_countries = [r for r in reader]
# get {ISO: table_id} dict
FTIDS = {r['iso_country_code']: r['fusion_table_id'] for r in all_countries}

#with open(SOURCE_FILE,'r') as fin:
#    reader = csv.DictReader(fin)
#    all_sources = [r for r in reader]
# get {Source: {info}} dict
#SOURCES = {r['source_name']: {'count':0, 'urls':[]} for r in all_sources}

SOURCES = {}
# main loop
for iso,ftid in FTIDS.iteritems():
    print iso

    # some countries do not have fusion tables; skip them
    if not ftid:
        continue

    # get all rows of country's fusion table
    current_rows = get_rows_with_sources(ftid)

    # loop through rows
    for plant in current_rows:
        rowid = plant[0]
        source_name = plant[1].encode(ENCODING)
        url = plant[2]
        if source_name in SOURCES:
            SOURCES[source_name]['count'] += 1
            if url not in SOURCES[source_name]['urls']:
                SOURCES[source_name]['urls'].append(url)
        else:
            SOURCES[source_name] = {'count':1,'urls':[url]}

# write result
with open(SOURCE_FILE,'w') as f:
    f.write('source_name,plant_count,urls')
    sources_file = csv.writer(f)
    for k,v in SOURCES.iteritems():
        try:
            sources_file.writerow([k,v['count'],','.join(v['urls'])])
        except:
            print(u"-Error with source {0}".format(k))
