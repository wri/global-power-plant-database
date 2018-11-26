# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_argentina.py
Get power plant data from Argentina and convert to the Global Power Plant Database format.
Data Source: Ministerio de Energia y Mineria, Argentina
Data Portal: http://datos.minem.gob.ar/index.php
Generation Data : http://energia3.mecon.gov.ar/contenidos/archivos/Reorganizacion/informacion_del_mercado/publicaciones/mercado_electrico/estadisticosectorelectrico/2015/A1.POT_GEN_COMB_POR_CENTRAL_2015.xlsx
Additional information: http://datos.minem.gob.ar/api/search/dataset?q=centrales
Additional information: http://datos.minem.gob.ar/api/rest/dataset/
Additional information: https://www.minem.gob.ar/www/706/24621/articulo/noticias/1237/aranguren-e-ibarra-presentaron-el-portal-de-datos-abiertos-del-ministerio-de-energia-y-mineria.html
Issues:
- Several plants are listed as mobile ("movil"); the largest is http://www.enarsa.com.ar/index.php/es/energiaelectrica/386-unidades-de-emergencia-movil .
- These cannot be geolocated.
"""

import csv
import datetime
import xlrd
import sys
import os
import json

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Argentina"
SOURCE_NAME = u"Ministerio de Energía y Minería"
SOURCE_URL = u"http://energia3.mecon.gov.ar/contenidos/archivos/Reorganizacion/informacion_del_mercado/publicaciones/mercado_electrico/estadisticosectorelectrico/2015/A1.POT_GEN_COMB_POR_CENTRAL_2015.xlsx"
SAVE_CODE = u"ARG"
RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="A1.POT_GEN_COMB_POR_CENTRAL_2015.xlsx")
LOCATION_FILE_NAME = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE, filename="locations_ARG.csv")
COMMISSIONING_YEAR_FILE_NAME = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE, filename="commissioning_years_ARG.csv")
CSV_FILE_NAME = pw.make_file_path(fileType = "src_csv", filename = "database_ARG.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType = "src_bin")
YEAR_OF_DATA = 2015
CAPACITY_CONVERSION_TO_MW = 0.001       # capacity values are given in kW in the raw data
GENERATION_CONVERSION_TO_GWH = 0.001    # generation values are given in MWh in the raw data

# other parameters
COLS = {'owner': 1, 'name': 2, 'fuel': 3, 'grid': 4, 'capacity': 6, 'generation': 7}
TAB = "POT_GEN"
START_ROW = 8

gen_start = datetime.date(YEAR_OF_DATA, 1, 1)
gen_stop = datetime.date(YEAR_OF_DATA, 12, 31)

# optional raw file(s) download
downloaded = pw.download(COUNTRY_NAME, {RAW_FILE_NAME: SOURCE_URL})

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

# read locations
locations_dictionary = {}
with open(LOCATION_FILE_NAME, 'r') as f:
    datareader = csv.reader(f)
    headers = datareader.next()
    for row in datareader:
        locations_dictionary[pw.format_string(row[0])] = [float(row[1]), float(row[2])]

# read commissioning years
commissioning_years_dictionary = {}
with open(COMMISSIONING_YEAR_FILE_NAME, 'r') as f:
    datareader = csv.reader(f)
    headers = datareader.next()
    for row in datareader:
        commissioning_years_dictionary[pw.format_string(row[0])] = row[1]

# read data from csv and parse
count = 1

wb = xlrd.open_workbook(RAW_FILE_NAME)
ws = wb.sheet_by_name(TAB)

# treat first data row specially for plant name
#rv0 = ws.row_values(START_ROW)
#current_plant_name = pw.format_string(rv0[COLS['name']])
#current_owner = pw.format_string(rv0[COLS['owner']])
#current_fuel_type = pw.standardize_fuel(rv0[COLS['fuel']], fuel_thesaurus, as_set=False)
#current_capacity_sum = float(rv0[COLS['capacity']]) * CAPACITY_CONVERSION_TO_MW
#current_generation_sum = float(rv0[COLS['generation']]) * GENERATION_CONVERSION_TO_GWH

previous_owner = u'None'
previous_name = u'None'
plant_names = {}

for row_id in range(START_ROW, ws.nrows):

    rv = ws.row_values(row_id) 

    # get fuel
    fuel_string = pw.format_string(rv[COLS['fuel']], None)
    if not fuel_string:
        continue                    # row without fuel type is empty
    else:
        fuel_type = pw.standardize_fuel(fuel_string, fuel_thesaurus)

    # check for islanded generator
    grid_string = pw.format_string(rv[COLS['grid']], None)
    if grid_string == u"AISLADO":
        continue                    # don't add islanded generators (not grid-connected)

    # get name
    name_string = pw.format_string(rv[COLS['name']], None)
    if name_string:
        previous_name = name_string
    else:
        name_string = previous_name

    # get owner
    owner_string = pw.format_string(rv[COLS['owner']], None)
    if owner_string:
        previous_owner = owner_string
    else:
        owner_string = previous_owner

    # get capacity
    try:
        capacity_value = float(rv[COLS['capacity']]) * CAPACITY_CONVERSION_TO_MW
    except:
        print("Cant read capacity for plant {0}.".format(name_string))
        capacity_value = 0

    # check if we've seen this plant before
    if name_string not in plant_names.keys():

        # first time we've seen this plant
        idnr = pw.make_id(SAVE_CODE, count)
        new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name_string, plant_owner=owner_string,
            plant_country=COUNTRY_NAME, plant_capacity=capacity_value,
            plant_primary_fuel=fuel_type,
            plant_cap_year=YEAR_OF_DATA, plant_source=SOURCE_NAME, 
            plant_source_url=SOURCE_URL)
        plants_dictionary[idnr] = new_plant
        plant_names[name_string] = idnr

    else:

        # this row is an additional fuel type for a plant we've already seen
        idnr = plant_names[name_string]
        plants_dictionary[idnr].other_fuel.update(fuel_type)
        plants_dictionary[idnr].capacity += capacity_value

    # increment count
    count += 1


# now assign locations and commissioning years
location_not_found = 0
year_not_found = 0

for idnr,plant in plants_dictionary.iteritems():

    if plant.name in locations_dictionary.keys():
        coords = locations_dictionary[plant.name]
        plants_dictionary[idnr].location = pw.LocationObject(pw.NO_DATA_UNICODE, coords[0], coords[1])
        plants_dictionary[idnr].coord_source = SOURCE_NAME
    else:
        location_not_found += 1
        plants_dictionary[idnr].coord_source = pw.NO_DATA_UNICODE

    if plant.name in commissioning_years_dictionary.keys():
        plants_dictionary[idnr].commissioning_year = commissioning_years_dictionary[plant.name]
    else:
        year_not_found += 1

"""
print("Locations not found for these plants:")
location_not_found.sort(key = lambda x:x.capacity, reverse=True)
for plant in location_not_found:
    if 'MOVIL' not in plant.name:
        print(u"{0}, {1} MW".format(plant.name, plant.capacity))

print("Commissioning year not found for these plants:")
year_not_found.sort(key = lambda x:x.capacity, reverse=True)
for plant in year_not_found:
    print(u"{0}, {1} MW".format(plant.name, plant.capacity))
"""

print("Missing location for {0} plants; missing commissioning year for {1} plants.".format(location_not_found,year_not_found))

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
