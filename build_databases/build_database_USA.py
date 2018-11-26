# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_USA.py
Converts plant-level data from EIA-860 and EIA-923 to the Global Power Plant Database format.
Does not currently implement download of EIA spreadsheets.
Uses data from WRI Fusion tables for Puerto Rico and Guam.
"""

import xlrd
import sys, os
import csv

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"United States of America"
SAVE_CODE  = u"USA"
RAW_FILE_NAME_860_2 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="2___Plant_Y2016.xlsx")
RAW_FILE_NAME_860_3 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="3_1_Generator_Y2016.xlsx")
RAW_FILE_NAME_923_2 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx")
WRI_DATABASE = pw.make_file_path(fileType="src_bin", filename=u"WRI-Database.bin")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_USA.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
SOURCE_NAME = u"U.S. Energy Information Administration"
SOURCE_URL = u"http://www.eia.gov/electricity/data/browser/"
YEAR = 2016
GENERATION_CONVERSION_TO_GWH = 0.001	# generation values are given in MWh in the raw data
SUBSIDIARY_COUNTRIES = ["Puerto Rico", "Guam"]

COLS_860_2 = {'name':3, 'idnr':2, 'owner':1, 'lat':9, 'lng':10}
COLS_860_3 = {'idnr':2, 'capacity':15, 'primary_fuel':33, 'other_fuel':[34,35,36], 'operating_month':25, 'operating_year':26}
COLS_923_2 = {'idnr':0, 'generation':95}
TAB_NAME_860_2 = "Plant"
TAB_NAME_860_3 = "Operable"
TAB_NAME_923_2 = "Page 1 Generation and Fuel Data"

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# Open workbooks
print("Loading workbooks...")
print("Loading Form 923-2")
wb1 = xlrd.open_workbook(RAW_FILE_NAME_923_2)
ws1 = wb1.sheet_by_name(TAB_NAME_923_2)
print("Loading Form 860-2")
wb2 = xlrd.open_workbook(RAW_FILE_NAME_860_2)
ws2 = wb2.sheet_by_name(TAB_NAME_860_2)
print("Loading Form 860-3")
wb3 = xlrd.open_workbook(RAW_FILE_NAME_860_3)
ws3 = wb3.sheet_by_name(TAB_NAME_860_3)

# read in plants from File 2 of EIA-860
print("Reading in plants...")
plants_dictionary = {}
for row_id in xrange(2, ws2.nrows):
	rv = ws2.row_values(row_id) # row value
	name = pw.format_string(rv[COLS_860_2['name']])
	idnr = pw.make_id(SAVE_CODE, int(rv[COLS_860_2['idnr']]))
	capacity = 0.0
	generation = pw.PlantGenerationObject()
	owner = pw.format_string(str(rv[COLS_860_2['owner']]))
	try:
		latitude = float(rv[COLS_860_2['lat']])
	except:
		latitude = pw.NO_DATA_NUMERIC
	try:
		longitude = float(rv[COLS_860_2['lng']])
	except:
		longitude = pw.NO_DATA_NUMERIC
	location = pw.LocationObject(u"", latitude, longitude)
	new_plant = pw.PowerPlant(idnr, name, plant_country=COUNTRY_NAME,
		plant_location=location, plant_coord_source=SOURCE_NAME,
		plant_owner=owner, plant_capacity=capacity,
		plant_generation=generation,
		plant_cap_year=YEAR, plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL)
	plants_dictionary[idnr] = new_plant

# read in capacities from File 3 of EIA-860
print("Reading in capacities...")
commissioning_year_by_unit = {}	 # temporary method until PowerPlant object includes unit-level information

for row_id in xrange(2, ws3.nrows):
	rv = ws3.row_values(row_id)  # row value
	try:
		idnr = pw.make_id(SAVE_CODE, int(rv[COLS_860_3['idnr']]))
	except:
		continue
	if idnr in plants_dictionary.keys():
		unit_capacity = float(rv[COLS_860_3['capacity']])
		plants_dictionary[idnr].capacity += unit_capacity
		# todo: average commissioning year calculation

		unit_month = int(rv[COLS_860_3['operating_month']])
		unit_year_raw = int(rv[COLS_860_3['operating_year']])
		unit_year = 1.0 * unit_year_raw + unit_month / 12
		if idnr in commissioning_year_by_unit.keys():
			commissioning_year_by_unit[idnr].append([unit_capacity, unit_year])
		else:
			commissioning_year_by_unit[idnr] = [ [unit_capacity, unit_year] ]

		primary_fuel = pw.standardize_fuel(rv[COLS_860_3['primary_fuel']], fuel_thesaurus, as_set=False)
		plants_dictionary[idnr].primary_fuel = primary_fuel

		for i in COLS_860_3['other_fuel']:
			try:
				if rv[i] == "None":
					continue
				fuel_type = pw.standardize_fuel(rv[i], fuel_thesaurus, as_set=True)
				plants_dictionary[idnr].other_fuel.update(fuel_type)
			except:
				continue
	else:
		print("Can't find plant with ID: {0}".format(idnr))

# calculate and save average commissioning year
for idnr,unit_vals in commissioning_year_by_unit.iteritems():
	cap_times_year = 0
	total_cap = 0
	for unit in unit_vals:
		cap_times_year += unit[0]*unit[1]
		total_cap += unit[0]
	plants_dictionary[idnr].commissioning_year = cap_times_year / total_cap

print("...added plant capacities and commissioning year.")

# read in generation from File 2 of EIA-923
print("Reading in generation...")
for row_id in xrange(6, ws1.nrows):
	rv = ws1.row_values(row_id)
	idnr = pw.make_id(SAVE_CODE, int(rv[COLS_923_2['idnr']]))
	if idnr in plants_dictionary.keys():
		if not plants_dictionary[idnr].generation[0]:
			generation = pw.PlantGenerationObject.create(0.0, YEAR, source=SOURCE_URL)
			plants_dictionary[idnr].generation[0] = generation
		plants_dictionary[idnr].generation[0].gwh += float(rv[COLS_923_2['generation']]) * GENERATION_CONVERSION_TO_GWH
	else:
		print("Can't find plant with ID: {0}".format(idnr))
print("...Added plant generations.")

# read in subsidiary states (Puerto Rico, Guam)
print("Adding additional plants from Fusion Table data...")
fusion_table_data = pw.load_database(WRI_DATABASE)
for country in SUBSIDIARY_COUNTRIES:
	these_plants = {k:v for k,v in fusion_table_data.iteritems() if v.country == country}
	for k,v in these_plants.iteritems():
		v.country = COUNTRY_NAME
	plants_dictionary.update(these_plants)
print("...finished.")

# report on plants read from file
print(u"Loaded {0} plants to database.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# pickle database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print("Pickled database to {0}".format(SAVE_DIRECTORY))
