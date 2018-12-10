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
PLANT_AUX_FILE = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE, filename="ARG_plants.csv")
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

# read auxilliary plant information
with open(PLANT_AUX_FILE, 'r') as f:
    reader = csv.DictReader(f)
    aux_plant_info = {pw.format_string(row['name']): row for row in reader}

# read data from csv and parse
count = 1

wb = xlrd.open_workbook(RAW_FILE_NAME)
ws = wb.sheet_by_name(TAB)

previous_owner = u'None'
previous_name = u'None'
plant_names = {}

for row_id in range(START_ROW, ws.nrows):

    rv = ws.row_values(row_id) 

    # check for islanded generator
    grid_string = pw.format_string(rv[COLS['grid']], None)
    if grid_string == u"AISLADO":
        continue                    # don't add islanded generators (not grid-connected)

    # get fuel
    fuel_string = pw.format_string(rv[COLS['fuel']], None)
    if not fuel_string:
        continue                    # row without fuel type is empty

    # get name
    name_string = pw.format_string(rv[COLS['name']], None)
    if name_string:
        previous_name = name_string
    else:
        name_string = previous_name

    if name_string not in aux_plant_info:
        print("Can't find plant <{0}> in auxiliary plant information file, skipping..".format(name_string))
        continue

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
    if name_string not in plant_names:
        # first time we've seen this plant
        fuel_type = pw.standardize_fuel(fuel_string, fuel_thesaurus, as_set=False)
        idnr = pw.format_string(aux_plant_info[name_string]['gppd_idnr'])
        new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name_string, plant_owner=owner_string,
            plant_country=COUNTRY_NAME, plant_capacity=capacity_value,
            plant_primary_fuel=fuel_type,
            plant_cap_year=YEAR_OF_DATA, plant_source=SOURCE_NAME, 
            plant_source_url=SOURCE_URL)
        plants_dictionary[idnr] = new_plant
        plant_names[name_string] = (idnr, {fuel_type: capacity_value})
        # increment count
        count += 1

    else:

        # this row is an additional fuel type for a plant we've already seen
        fuel_type = pw.standardize_fuel(fuel_string, fuel_thesaurus, as_set=False)
        idnr, fuel_capacity_dict = plant_names[name_string]
        fuel_capacity = fuel_capacity_dict.get(fuel_type, 0)
        fuel_capacity += capacity_value
        fuel_capacity_dict[fuel_type] = fuel_capacity
        plants_dictionary[idnr].capacity += capacity_value


# assign primary and other fuel types to each plant
for name, (idnr, fuel_capacity_dict) in plant_names.iteritems():
    primary_fuel = max(fuel_capacity_dict, key=lambda x: fuel_capacity_dict[x])
    other_fuels = set(fuel_capacity_dict.keys())
    other_fuels.remove(primary_fuel)
    plants_dictionary[idnr].primary_fuel = primary_fuel
    plants_dictionary[idnr].other_fuel = other_fuels

# now assign locations and commissioning years
location_not_found = 0
year_not_found = 0

for idnr, plant in plants_dictionary.iteritems():
    aux = aux_plant_info[plant.name]
    lat, lon = aux['latitude'], aux['longitude']
    try:
        flat = float(lat)
        flon = float(lon)
    except:
        location_not_found += 1
        plants_dictionary[idnr].coord_source = pw.NO_DATA_UNICODE
    else:
        plant.location = pw.LocationObject(pw.NO_DATA_UNICODE, flat, flon)
        plant.coord_source = SOURCE_NAME
    try:
        year = float(aux['commissioning_year'])
    except:
        year_not_found += 1
    else:
        plant.commissioning_year = year

print("Missing location for {0} plants; missing commissioning year for {1} plants.".format(location_not_found, year_not_found))


# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
asdf = pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
