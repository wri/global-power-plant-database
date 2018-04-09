# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
get_national_generation.py
Extract data on total electrical generation by country and fuel. Units: GWh.
Data source: IEA, http://www.iea.org/statistics/statisticssearch/
"""

import sys
import os
import csv
import codecs
from lxml import etree, html
from selenium import webdriver

sys.path.insert(0, os.path.join(os.pardir,os.pardir))
import powerplant_database as pw

# params
ENCODING = 'utf-8'
URL_BASE = "http://www.iea.org/statistics/statisticssearch/report/?product=electricityandheat&year=2014"
COUNTRY_LIST = ['CHINA']
HTML_FILENAME_BASE = 'html_pages/iea_statistics_2014_'
SAVE_HTML = False
RESULTS_FILENAME = 'generation_by_country_by_fuel_2014.csv'

# make webdriver
chrome = webdriver.Chrome()

# make HTML parser
parser = etree.HTMLParser(encoding=ENCODING)

# make country name thesaurus and dict of country objects
country_names = pw.make_country_names_thesaurus()

# make dictionary to hold country- and fuel-level data
generation_data = {}

# iterate through countries
for country,country_aliases in country_names.iteritems():

	iea_country = country_aliases[2]
	if not iea_country:
		print("No IEA alias for {0}, skipping.".format(country))
		continue

	print("Getting data for {0}...".format(country))

	generation_data[country] = 	{ 	'Biomass': 0, 
									'Coal': 0, 
									'Cogeneration': 0, 
									'Gas': 0, 
									'Geothermal': 0, 
									'Hydro': 0, 
									'Nuclear': 0, 
									'Oil': 0, 
									'Other': 0, 
									'Petcoke': 0, 
									'Solar': 0, 
									'Waste': 0,
									'Wave_and_Tidal': 0, 
									'Wind': 0,
									'Total': 0
								}

	# get country-specific statistics page
	URL = URL_BASE + "&country={0}".format(iea_country).replace(" ", "%20")
	try:
		chrome.get(URL)
	except:
		print("Response failed for country {0}.".format(country))
		continue

	# save page source if specified
	if SAVE_HTML:
		fn = HTML_FILENAME_BASE + country + '.html'
		with codecs.open(fn,'w',ENCODING) as f:
			f.write(chrome.page_source)

	# parse page for generation data
	root = etree.fromstring(chrome.page_source.encode(ENCODING),parser=parser)

	# get rows of relevant table
	for row in root.findall("body/div/div/div/table/tbody/tr"):

		# ignore blank rows
		if len(row) == 0:
			continue

		if not row[0].text:
			continue

		# read electricity generation value for each fuel row
		if 'coal' in row[0].text:
			generation_data[country]['Coal'] = float(row[1].text)

		elif 'oil' in row[0].text:
			generation_data[country]['Oil'] = float(row[1].text)

		elif 'gas' in row[0].text:
			generation_data[country]['Gas'] = float(row[1].text)

		elif 'biofuels' in row[0].text:
			generation_data[country]['Biomass'] = float(row[1].text)
		
		elif 'waste' in row[0].text:
			generation_data[country]['Waste'] = float(row[1].text)

		elif 'nuclear' in row[0].text:
			generation_data[country]['Nuclear'] = float(row[1].text)

		elif 'hydro' in row[0].text:
			generation_data[country]['Hydro'] = float(row[1].text)

		elif 'geothermal' in row[0].text:
			generation_data[country]['Geothermal'] = float(row[1].text)

		elif 'solar PV' in row[0].text: 		# note that solar PV and solar thermal are separate in IEA data
			generation_data[country]['Solar'] += float(row[1].text)

		elif 'solar thermal' in row[0].text:
			generation_data[country]['Solar'] += float(row[1].text)

		elif 'wind' in row[0].text:
			generation_data[country]['Wind'] = float(row[1].text)

		elif 'tide' in row[0].text:
			generation_data[country]['Wave_and_Tidal'] = float(row[1].text)

		elif 'other' in row[0].text:
			generation_data[country]['Other'] = float(row[1].text)

		elif 'Total production' in row[0].text:
			generation_data[country]['Total'] = float(row[1].text)

	# test if total generation value was read
	if generation_data[country]['Total'] == 0:
		print("...Error: unable to read data.")

# write results to file
with open(RESULTS_FILENAME,'w') as f:
	f.write('country,fuel,generation_gwh_2014\n')
	for country,fuel_types in generation_data.iteritems():
		for fuel_type,gen_gwh in fuel_types.iteritems():
			line = '{0},{1},{2}\n'.format(country,fuel_type,gen_gwh)
			f.write(line)

# close webdriver
chrome.quit()
