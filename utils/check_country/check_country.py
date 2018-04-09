# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
check_country.py
Use google maps reverse geocoding API to test if lat/long coordinates are in the correct country.
Add Google API key as API_KEY.
Searches for plants ranging from PLANT_START to PLANT_STOP in database (in order).
Incorrect country locations are logged in LOG_FILE.
"""

import sys
import os
import csv
import json
import requests
import argparse
from time import gmtime, strftime

sys.path.insert(0, os.path.join(os.pardir,os.pardir))
import powerplant_database as pw

# params
API_KEY_FILE = pw.make_file_path(fileType="resource",subFolder="api_keys",filename="fusion_tables_api_key.txt")
ENCODING = 'utf-8'
URL_BASE = "https://maps.googleapis.com/maps/api/geocode/json?"
LOG_FILE = "country_geolocation_errors.csv"
PLANT_START = 0
PLANT_STOP = 2500		# API appears limited to 2500 requests/day
						# 0-2450 checked on 1/11/18
						# 2500-5000 checked on 1/15/18
						# 5001-7500 checked on 1/16/18
						# 7501-10000 checked on 1/17/18
						# 10001-12500 checked on 1/18/18
						# 12501-15000 checked on 1/19/18
						# 15001-17500 checked on 1/22/18
						# 17501-20000 checked on 1/23/18
						# 20001-22500 checked on 1/24/18
						# 22501-25000 checked on 1/25/18
						# 25001-27000 checked on 1/26/18

# synomyms for countries used by Google that differ from the Database standard (incomplete list)
# google name: global power plant database name
country_synonyms = {	'United States':'United States of America',
						'Czechia':'Czech Republic',
						'Syria':'Syrian Arab Republic',
						'Martinique':'France',
						'Guadeloupe':'France',
						'Reunion':'France',
						'The Gambia':'Gambia',
						'Myanmar (Burma)':'Myanmar',
						'Hong Kong':'China',
						'Macau':'China',
						'Mayotte':'France',
						'French Guiana':'France',
						'Republic of the Congo':'Congo',
						'Macedonia':'Macedonia (FYROM)',
						'Puerto Rico':'United States of America',
						'Brunei':'Brunei Darussalam',
					}

# parse args
parser = argparse.ArgumentParser()
parser.add_argument("powerplant_database", help = "name of power plant csv file")
args = parser.parse_args()

# get API key
with open(API_KEY_FILE,'r') as f:
	API_KEY = f.readline().rstrip()

# open powerplant csv file
plants = {}
with open(args.powerplant_database,'rU') as f:
	datareader = csv.reader(f)
	headers1 = datareader.next()
	headers2 = datareader.next()
	for row in datareader:
		idval = row[1]
		country = row[4]
		try:
			latitude = float(row[8])
			longitude = float(row[9])
		except:
			continue
		plants[idval] = {'country':country,'latitude':latitude,'longitude':longitude}

# check coordinates
print("Checking plants...")
plant_count = 0
bad_geolocations = []

f = open(LOG_FILE,'a')
f.write('\nNew analysis starting at {0}\n'.format(strftime("%Y-%m-%d %H:%M:%S", gmtime())))
f.write('idval,latitude,longitude,pw_country,google_country\n')

for idval,plant in plants.iteritems():

	# only check plants in the start/stop range
	if plant_count < PLANT_START:
		plant_count += 1
		continue
	if plant_count > PLANT_STOP:
		break

	if plant_count % 50 == 0:
		print("...checked {0} plants...".format(plant_count))
	plant_count += 1
	country_gppd = plant['country']
	latitude = plant['latitude']
	longitude = plant['longitude']

	# build url and sent request
	URL = URL_BASE + "latlng={0},{1}".format(latitude,longitude) 
	URL = URL + "&key={0}".format(API_KEY)
	try:
		res = requests.get(URL)
		j = json.loads(res.text)
		address_components = j['results'][0]['address_components']
		for component in address_components:
			country_google = ''
			if "country" in component['types']:
				country_google = component['long_name']
				break
		if not country_google:
			# problem case
			print("Plant {0}: PW country: {1}; Google country not found.".format(idval,country_gppd))
			bad_geolocations.append(idval)
			f.write('{0},{1},{2},{3},not found\n'.format(idval,latitude,longitude,country_gppd))
		if country_gppd != country_google:
			# problem case unless it's a synonym
			if country_google in country_synonyms.keys():
				if country_synonyms[country_google] == country_gppd:
					continue
			print("Plant {0}: PW country: {1}; Google country: {2}".format(idval,country_gppd,country_google))
			bad_geolocations.append(idval)
			f.write('{0},{1},{2},{3},{4}\n'.format(idval,latitude,longitude,country_gppd,country_google))
	except:
		print("Error with plant {0}, skipping.".format(idval))
		"""
		try:
			f.write('{0},{1},{2},{3},unknown\n'.format(idval,latitude,longitude,country_gppd))
		except:
			print("...unable to save to log.")
		"""

# close log file
f.close()

# report location errors
print("Bad geolocations:")
for idval in bad_geolocations:
	print(idval)

print("Finished.")
