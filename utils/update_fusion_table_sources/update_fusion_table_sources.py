# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
modify_fusion_tables_update_sources.py
Update sources used in FTs.
"""

import os
import csv
import sys
import httplib2
import time
import json

from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.crypt import Signer
from apiclient.discovery import build
KEYFILE = '../../resources/api_keys/fusion_tables_service_key.json'
COUNTRYFILE = '../../resources/country_information.csv'
SOURCEFILE = './all_sources.csv'
ENCODING = 'utf-8'
CHANGELOG = './changelog_sources.csv'

scopes = ['https://www.googleapis.com/auth/fusiontables']
credentials = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE, scopes=scopes)
http_auth = credentials.authorize(httplib2.Http())

# build fusion table API
ft_api = build('fusiontables', 'v2', http_auth)
# get an object that can build API query requests
query_api = ft_api.query()

def get_rows_with_sources(ftid):
    """Query a fusion table and return a list of [ROWID, Source]."""
    q = query_api.sql(sql="SELECT ROWID, Source, URL, \'Year of Data\' from {ftid}".format(ftid=ftid), hdrs=True, typed=False).execute()
    return q['rows']

def update_source(ftid, rowid, source):
    """Update a single fusion table row with new source information."""
    try:
        assert source
    except:
        print "[ERROR]", "Source is empty for {ftid}".format(ftid=ftid)
        return

    sql_statement = "UPDATE {ftid} SET Source = \'{source}\' WHERE ROWID = \'{rowid}\'"
    q = query_api.sql(sql=sql_statement.format(ftid=ftid,source=source,rowid=rowid)).execute()

# read the country information file
# [ ['primary_country_name','iso_country_code','iso_country_code2','has_api','use_geo','nation','geo_country','carma_country','idea_country','google_country','fusion_table_id'], [...], ...]
with open(COUNTRYFILE) as fin:
    reader = csv.DictReader(fin)
    all_countries = [r for r in reader]
# get {ISO: table_id} dict
FTIDS = {r['iso_country_code']: r['fusion_table_id'] for r in all_countries}

# read list of sources to modify and make dict by source name
# [ ['source_name','master_name','plant_count','iso3'], [...], ... ]
with open(SOURCEFILE) as fin:
    reader = csv.DictReader(fin)
    all_sources = [r for r in reader]
SOURCE_NAMES = {r['\xef\xbb\xbfsource_name']: r['master_name'] for r in all_sources}

# check read
for k,v in SOURCE_NAMES.iteritems():
    print(u"{0}: {1}".format(k,v))

"""
# main loop
with open(CHANGELOG,'a') as fout:
    changelog = csv.writer(fout)
    changed_list = []

    # loop through country-by-country 
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
            year_of_data = plant[3]

            if source_name in SOURCE_NAMES.keys():      # should always be true
                updated_source = SOURCE_NAMES[source_name]
                if updated_source and update_source != source_name:
                    # do update
                    try:
                        print "- Trying to update row", rowid, "(", iso, ") from source = ", source_name, " to source = ", updated_source, "..."
                        update_source(ftid,rowid,updated_source)
                        time.sleep(1.125)   # stay under rate limits
                    except Exception as e:
                        print '[UPDATE ERROR]', e.message
                        if 'Rate Limit' in e.message:
                            time.sleep(25)  # sleep off rate limit
                    else:
                        # log change
                        log_entry = [iso,rowid,'Updated source from {old_source} to {new_source}.'.format(old_source=source_name,new_source=updated_source)]
                        changed_list.append(log_entry)
                        changelog.writerow(log_entry)

            else:   # source name not in keys
                print "- Source name {0} not in source table.".format(source_name)
"""
