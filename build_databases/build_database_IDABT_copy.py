# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_IDABT.py
Get power plant data for coal plants in China from Industry About and convert to the Global Power Plant Database format.
Data Source: Industry About <industryabout.com>

Notes:
	- Uses a javascript asset to obtain the set of URLS to investigate.
	- Caches the scraped (but not parsed) webpage content in a sqlite3 database.
"""

import sys
import os
import json
import csv
from time import sleep
import sqlite3

import requests
from bs4 import BeautifulSoup as soup
from selenium import webdriver

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = 'China'
SOURCE_NAME = 'Industry About'
SAVE_CODE = 'IDABT'

CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_IDABT.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")

DATABASE_FILE = pw.make_file_path('raw', SAVE_CODE, 'Industry_About_China_Fossil_Fuel_Energy.sqlite')
JSON_URL = 'https://www.industryabout.com/index.php?option=com_contentmap&view=smartloader&type=json&filename=articlesmarkers&source=articles&owner=module&id=127&Itemid=181'
SLEEP_TIME = 2

# Field names that have typos/inconsistencies
FIELD_ALIAS = {
	u'4Type': 'Type',
	u'Activity Since': 'Activity since',
	u'Coodinates': 'Coordinates',
	u'Coordiantes': 'Coordinates',
	u'Kind of Coal': 'Kind of Fuel',
	u'Other names': 'Other name',
	u'TType': 'Type',
}

# Function that will be called with each field's value
HEADER_NAMES = [
	u'Activity since', #int,
	u'Activity until', #pw.format_string,
	u'Address', #pw.format_string,
	u'Area', #pw.format_string,
	u'Coordinates', #lambda x, #map(float, x.split(',')),
	u'Email', #pw.format_string,
	u'Kind of Fuel', #pw.format_string,
	u'Notes', #pw.format_string,
	u'Origin of Fuel', #pw.format_string,
	u'Other name', #pw.format_string,
	u'Owner', #pw.format_string,
	u'Phone', #pw.format_string,
	u'Power Capacity', #lambda x, #float(x.lstrip().split(' ')[0].replace(',', '')),
	u'Shareholders', #pw.format_string,
	u'Type', #pw.format_string,
	u'Web', #pw.format_string,
	u'Wikipedia', #pw.format_string
	u'__url__',
	u'__page_name__',
]

# Function that will be called with each field's value
FIELD_PROCESSING = {
	u'Activity since': int,
	u'Activity until': pw.format_string,
	u'Address': pw.format_string,
	u'Area': pw.format_string,
	u'Coordinates': lambda x: map(float, x.split(',')),
	u'Email': pw.format_string,
	u'Kind of Fuel': pw.format_string,
	u'Notes': pw.format_string,
	u'Origin of Fuel': pw.format_string,
	u'Other name': pw.format_string,
	u'Owner': pw.format_string,
	u'Phone': pw.format_string,
	u'Power Capacity': lambda x: float(x.lstrip().split(' ')[0].replace(',', '')),
	u'Shareholders': pw.format_string,
	u'Type': pw.format_string,
	u'Web': pw.format_string,
	u'Wikipedia': pw.format_string
}

# Optionally do the scraping
if '--download' in sys.argv:
	# Use selenium to get a live version of the page
	chrome = webdriver.Chrome()
	chrome.get(JSON_URL)
	html_content = chrome.page_source
	chrome.close()
	s = soup(html_content)
	# strip the leading variable assignment from the javascript asset
	# so that only viable JSON remains
	json_str = s.get_text('pre')[20:]
	assert json_str[0] == '{'
	map_data = json.loads(json_str)

	# make the sqlite database
	if os.path.exists(DATABASE_FILE):
		os.remove(DATABASE_FILE)
	conn = sqlite3.connect(DATABASE_FILE)
	c = conn.cursor()

	# create the sql table
	c.execute('''CREATE TABLE data (
					name TEXT,
					url TEXT,
					response INTEGER,
					content TEXT
			)''')

	# download the HTML content for each power plant webpage
	for i, item in enumerate(map_data['places']):
		if item['category'] != 'Fossil Fuels Energy':
			continue
		url = 'https://industryabout.com' + item['article_url']

		# HTTP GET
		r = requests.get(url)
		if not r.ok:
			content = None
		else:
			content = unicode(r.content.decode('utf-8'))

		# SQL INSERT
		stmt = u'''
			INSERT INTO data VALUES (
				?, ?, ?, ?
			)'''
		vals = (item['title'],
				url,
				r.status_code,
				content)
		c.execute(stmt, vals)
		sleep(SLEEP_TIME)  # rate limiting

	conn.close()
	# end of --download option


# Connect to the sqlite3 database
conn = sqlite3.connect(DATABASE_FILE)
c = conn.cursor()

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

writer = csv.DictWriter(open('/home/logan/WRI/tmp/industry_about/IA_ft_template.csv', 'wb'), fieldnames=HEADER_NAMES)
writer.writeheader()

for name, url, code, content in c.execute('SELECT * FROM data'):
	# skip
	if code != 200:
		print 'Error HTML status code for plant', name
		continue

	# make the HTML extractor
	s = soup(content)

	# Industy About asset ID is given in the URL
	plant_id = int(url.split('/')[-1].split('-')[0])

	# Page name is also the power plant name
	page_name = s.find('h2', itemprop='name').text.strip()

	# Get the main portion of the webpage that has usable data
	article = s.find('div', itemprop='articleBody')
	data = article.find_all('strong')

	# Store the field values in a dictionary
	data_dict = {'__url__': unicode(url).encode('utf-8'), '__page_name__': unicode(page_name).encode('utf-8')}
	for d in data:
		# Information is stored as text that looks like "Field Name: Value"
		try:
			field, value = d.text.split(': ')
		except:
			continue
		field = FIELD_ALIAS.get(field, field)
		data_dict[field] = unicode(value).encode('utf-8')


	# Skip undesirable plants
	if 'Coal' not in data_dict.get('Type', u''):
		continue

	if isinstance(data_dict.get('Activity until', 0), unicode):
		print 'Skipping plant {0} because it is shutdown'.format(page_name)
		continue

	if isinstance(data_dict.get('Activity since', 0), unicode):
		if 'Under Construction' in data_dict['Activity since']:
			print 'Skipping plant {0} because it is under construction'.format(page_name)
			continue
		if data_dict['Activity since'] == u'':
			data_dict['Activity since'] = pw.NO_DATA_NUMERIC


	writer.writerow(data_dict)
	continue


	if 'Coordinates' not in data_dict or isinstance(data_dict['Coordinates'], unicode):
		print 'Skipping plant {0} because there is no coordinate information'.format(page_name)
		continue

	if 'Coordinates' not in data_dict:
		print 'Skipping plant {0} because there is no coordinate information'.format(page_name)
		continue

	if 'Power Capacity' not in data_dict or not isinstance(data_dict['Power Capacity'], float):
		print 'Skipping plant {0} because there is no capacity information'.format(page_name)
		continue

	# Get the pw_idnr
	idnr = pw.make_id(SAVE_CODE, plant_id)

	# Get the geolocation
	lat, lon = data_dict['Coordinates']
	location = pw.LocationObject(pw.NO_DATA_UNICODE, lat, lon)

	# Construct the PowerPlant object
	new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=page_name, plant_country=COUNTRY_NAME,
		plant_fuel='Coal', plant_capacity=data_dict.get('Power Capacity', pw.NO_DATA_NUMERIC),
		plant_location=location, plant_coord_source=SOURCE_NAME,
		plant_source=SOURCE_NAME, plant_source_url=url,
		plant_owner=data_dict.get('Owner', pw.NO_DATA_UNICODE),
		plant_commissioning_year=data_dict.get('Activity since', pw.NO_DATA_NUMERIC),
	)
	plants_dictionary[idnr] = new_plant

# Close the sqlite3 database connection
conn.close()

# Report on number of plants included
print("Loaded {0} plants to database.".format(len(plants_dictionary)))

# Write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# Pickle database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print("Pickled database to {0}".format(SAVE_DIRECTORY))
