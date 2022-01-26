# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_australia.py
Converts GIS data from Australian Renewable Energy Mapping Infrastructure (AREMI) 
to the Global Power Plant Database format.
Uses NGER data for generation.

Last download date: 2018-11-17
"""

import xml.etree.ElementTree as ET
import json
import sys, os
import csv

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Australia"
SAVE_CODE = u"AUS"
SOURCE_NAME = u"Australian Renewable Energy Mapping Infrastructure"
SOURCE_URL = u"https://www.nationalmap.gov.au/"
GENERATION_SOURCE = u"Australia Clean Energy Regulator"

NGER_URL_1718 = u"http://www.cleanenergyregulator.gov.au/DocumentAssets/Documents/Greenhouse%20and%20energy%20information%20for%20designated%20generation%20facilities%202017-18.csv"
NGER_FILENAME_1718 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="NGER_2017-2018.csv")

NGER_URL_1617 = u"http://www.cleanenergyregulator.gov.au/DocumentAssets/Documents/Greenhouse%20and%20energy%20information%20for%20designated%20generation%20facilities%202016-17.csv"
NGER_FILENAME_1617 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="NGER_2016-2017.csv")

NGER_URL_1516 = u"http://www.cleanenergyregulator.gov.au/DocumentAssets/Documents/Greenhouse%20and%20energy%20information%20for%20designated%20generation%20facilities%202015-16.csv"
NGER_FILENAME_1516 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="NGER_2015-2016.csv")

NGER_URL_1415 = u"http://www.cleanenergyregulator.gov.au/DocumentAssets/Documents/2014-15%20Greenhouse%20and%20energy%20information%20for%20designated%20generation%20facilities.csv"
NGER_FILENAME_1415 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="NGER_2014-2015.csv")

NGER_URL_1314 = u"http://www.cleanenergyregulator.gov.au/DocumentAssets/Documents/2013-14%20Greenhouse%20and%20energy%20information%20for%20designated%20generation%20facilities.csv"
NGER_FILENAME_1314 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="NGER_2013-2014.csv")

NGER_URL_1213 = u"http://www.cleanenergyregulator.gov.au/DocumentAssets/Documents/2012-13%20Greenhouse%20and%20energy%20information%20for%20designated%20generation%20facilities.csv"
NGER_FILENAME_1213 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="NGER_2012-2013.csv")

RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="australia_power_plants.geo.json")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_AUS.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
STATIC_ID_FILENAME = pw.make_file_path(fileType="resource", subFolder='AUS', filename="AUS_plants.csv")
STATIC_MATCH_FILENAME = pw.make_file_path(fileType="resource", subFolder='AUS', filename="AUS_plant_dimension.csv")

# other parameters
API_BASE = "https://services.ga.gov.au/gis/rest/services/Foundation_Electricity_Infrastructure/MapServer/0/query"
API_CALL = "geometry=-180%2C-90%2C180%2C90&geometryType=esriGeometryEnvelope&inSR=EPSG%3A4326&spatialRel=esriSpatialRelIntersects&outFields=*&returnGeometry=true=&f=geojson"

# optional raw file(s) download
URL = API_BASE + "?" + API_CALL
FILES = {RAW_FILE_NAME: URL,
		NGER_FILENAME_1718: NGER_URL_1718,
		NGER_FILENAME_1617: NGER_URL_1617,
		NGER_FILENAME_1516: NGER_URL_1516,
		NGER_FILENAME_1415: NGER_URL_1415,
		NGER_FILENAME_1314: NGER_URL_1314,
		NGER_FILENAME_1213: NGER_URL_1213,
		}
DOWNLOAD_FILES = pw.download(COUNTRY_NAME, FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# get permanent IDs for australian plants
generation_linking_table = {k['gppd_idnr']: k for k in csv.DictReader(open(STATIC_ID_FILENAME))}

id_linking_table = {int(k['objectid']): k for k in csv.DictReader(open(STATIC_MATCH_FILENAME)) if k['objectid']}

fuel_type_assurance = {
	# gppd_idnr: primary_fuel
	'AUS0000619': 'Solar',
	'AUS0000526': 'Solar',
	'AUS0000581': 'Solar',
	'AUS0000620': 'Wind'
}

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")
print(u"Reading NGER files to memory...")

# read NGER file into a list, so the facilities can be referenced by their index in the original file
nger_1718 = list(csv.DictReader(open(NGER_FILENAME_1718)))
nger_1617 = list(csv.DictReader(open(NGER_FILENAME_1617)))
nger_1516 = list(csv.DictReader(open(NGER_FILENAME_1516)))
nger_1415 = list(csv.DictReader(open(NGER_FILENAME_1415)))
nger_1314 = list(csv.DictReader(open(NGER_FILENAME_1314)))
nger_1213 = list(csv.DictReader(open(NGER_FILENAME_1213)))

# read data from XML file and parse
count = 0
with open(RAW_FILE_NAME, "rU") as fin:
	geojson = json.load(fin)


for plant in geojson['features']:
	plant_properties = plant['properties']
	name_original = pw.format_string(plant_properties['name'])
	plant_oid = plant_properties['objectid']

	# check if plant is already known, and skip if there is not a record (includes cases where AREMI has duplicated plants)
	if plant_oid not in id_linking_table:
		print(u"Error: Don't have prescribed ID for plant {0}; OID={1}.".format(name_original, plant_oid))
		continue

	# get the assigned GPPD identifier
	plant_idnr = id_linking_table[plant_oid]['gppd_idnr_assigned']
	if not plant_idnr:
		print(u"Warning: plant {0}; OID={1} will not be added, ID not found (possible exlucuded on purpose).".format(name_original, plant_oid))
		continue

	operational_status = plant_properties['operational_status']
	if operational_status != 'Operational':
		print(u"Warning: plant {0}; OID={1} will not be added, considered unoperational: {2}".format(name_original, plant_oid, operational_status))
		continue

	# override name
	name_enforced = id_linking_table[plant_oid]['name_enforced']

	try:
		owner = pw.format_string(plant_properties['owner'])
	except:
		owner = pw.NO_DATA_UNICODE

	try:
		primary_fuel = pw.standardize_fuel(plant_properties['primaryfueltype'], fuel_thesaurus)
	except:
		print(u"Error: Can't understand fuel {0} for plant {1}.".format(plant_properties['primaryfueltype'], name_original))
		primary_fuel = pw.NO_DATA_UNICODE

	if plant_idnr in fuel_type_assurance:
		print(u"Warning: overriding fuel for plant {0}.".format(name_original))
		primary_fuel = pw.standardize_fuel(fuel_type_assurance[plant_idnr], fuel_thesaurus)
	try:
		capacity = plant_properties['generationmw']
		capacity = float(capacity)
	except:
		print(u"Error: Can't read capacity for plant {0}.".format(name_original))
		capacity = pw.NO_DATA_NUMERIC

	coords = plant['geometry']['coordinates']
	try:
		longitude = float(coords[0])
		latitude = float(coords[1])
		geolocation_source = SOURCE_NAME
	except:
		longitude, latitude = pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC
		geolocation_source = pw.NO_DATA_UNICODE

	# get generation data (if any) from the NGER datasets
	generation = []
	for yr, lookup in zip(
			range(2013, 2019),
			[nger_1213, nger_1314, nger_1415, nger_1516, nger_1617, nger_1718]
		):
		index_title = 'nger_{0}-{1}_index'.format(yr-1, yr)
		# get the raw form of the nger indices field
		try:
			nger_indices_raw = generation_linking_table[plant_idnr][index_title]
		except:
			print(u"Warning: gppd idnr {0} not found in generation matching table".format(plant_idnr))
			break
		# if blank, continue to next year
		if not nger_indices_raw.rstrip():
			continue
		# get ampersand-separated list of nger indices
		nger_indices = nger_indices_raw.split('&')
		# convert to real integers usable for list indexing
		nger_indices = map(int, nger_indices)
		gwh = 0
		for idx in nger_indices:
			try:
				nger_row = lookup[idx]
			except:
				print("Error with looking up NGER row for {0} (year = {1}; NGER index = {2};)".format(name_original, yr, idx))
				continue
			gen_gj = nger_row['Electricity Production (GJ)']
			try:
				gen_gwh = float(gen_gj.replace(",", ""))  / 3600.
			except:
				print("Error with NGER generation for {0} (year = {1}; NGER index = {2}; value={3})".format(name_original, yr, idx, gen_gj))
				pass
			else:
				gwh += gen_gwh
		# TODO: give proper time bounds
		generation.append(pw.PlantGenerationObject.create(gwh, yr, source=GENERATION_SOURCE))


	new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)

	if primary_fuel:
		new_plant = pw.PowerPlant(plant_idnr=plant_idnr, plant_name=name_enforced, plant_owner=owner, 
			plant_country=COUNTRY_NAME,
			plant_location=new_location, plant_coord_source=geolocation_source,
			plant_primary_fuel=primary_fuel, plant_capacity=capacity,
			plant_generation=generation,
			plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL)
		plants_dictionary[plant_idnr] = new_plant
		count += 1

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
