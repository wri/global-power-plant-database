# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_MEX.py
Get power plant data from Mexico and convert to the Global Power Plant Database format.
Data Source 1: North American Cooperation on Energy Information
Data Source 2: Comisión Reguladora de Energía (database of valid generation permits)
- 
Explanation:
We use NACEI (government-supplied data) for all renewable plants and all conventional plants of 100MW or higher.
We supplement this with CRE permit data for smaller plants.
-
Issues:
- NACIE data are very good, but do not include conventional plants below 100 MW.
- CRE permit data can supplement NACIE with conventional plants < 100 MW, but does not include plant names in
all cases. 
- CRE permit data contain unclear status codes - need to confirm which are relevant.
- CRE permit data may not reflect actual used/installed capacity - might be just legal permits.
- CFE has data on large plants in Excel format here: http://egob2.energia.gob.mx/portal/electricidad.html
- Alternate data source with less information: http://www.cre.gob.mx/da/PermisosdeGeneracionOtorgadosporModalidad.csv
(Note that column titled "CAPACIDAD AUTORIZADA (MW)" is actually current status of permit.)
"""

import csv
import sys, os
import xlrd

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Mexico"
SOURCE_NAME = u"North American Cooperation on Energy Information and Comisión Reguladora de Energía"
SOURCE_NAME_CRE = u"Comisión Reguladora de Energía"
SAVE_CODE = u"MEX"

RAW_FILE_NAME_1 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="PowerPlantsAllGE100MW_NorthAmerica_201606.xlsx")
SOURCE_URL_1 = u"http://base.energia.gob.mx/nacei/Archivos/3%20P%C3%A1gina%20Datos%20de%20Infraestructura/Ingl%C3%A9s/PowerPlantsAllGE100MW_NorthAmerica_201606.xlsx"
RAW_FILE_NAME_2 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="PowerPlantsRenewGE1MW_NorthAmerica_201606.xlsx")
SOURCE_URL_2 = u"http://base.energia.gob.mx/nacei/Archivos/3%20P%C3%A1gina%20Datos%20de%20Infraestructura/Ingl%C3%A9s/PowerPlantsRenewGE1MW_NorthAmerica_201606.xlsx"
RAW_FILE_NAME_3 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="1814.xlsx")
SOURCE_URL_3 = u"http://www.cre.gob.mx/documento/1814.xlsx"

CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_MEX.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
SOURCE_YEAR = 2016
ENCODING = 'UTF-8'

# True if specified --download, otherwise False
FILES = {RAW_FILE_NAME_1: SOURCE_URL_1,
        RAW_FILE_NAME_2: SOURCE_URL_2,
        RAW_FILE_NAME_3: SOURCE_URL_3} # dictionary of saving directories and corresponding urls
DOWNLOAD_FILES = pw.download("NACEI and CRE data", FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

# specify column names and tabs used in raw file
COLNAMES_1 = [u"Country", u"Facility Name", u"Owner Name (Company)", u"Latitude", u"Longitude", u"Total Capacity (MW)",
                u"Primary Energy Source", u"Source Agency", u"Reference Period"]
TAB_NAME_1 = u"PowerPlantsAllGE100MW"

COLNAMES_2 = [u"Country", u"Facility Name", u"Owner Name (Company)", u"Latitude", u"Longitude", u"Total Capacity (MW)",
              u"Primary Energy Source", u"Source Agency", u"Reference Period"]
TAB_NAME_2 = u"PowerPlantsRenewGE1MW"

COLNAMES_3 = [u"Núm.", u"PERMISIONARIO", u"CENTRAL", u"MODALIDAD", u"CAP. AUTORIZADA (MW)", u"FECHA DE ENTRADA EN OPERACIÓN", 
              u"ENERGETICO PRIMARIO", u"ESTADO ACTUAL", u"UBICACION DE LA PLANTA"]
TAB_NAME_3 = u"Permisos administrados"

# 1: read in NACEI conventional plants
book = xlrd.open_workbook(RAW_FILE_NAME_1, encoding_override=ENCODING)
sheet = book.sheet_by_name(TAB_NAME_1)

rv = sheet.row_values(0)
country_col = rv.index(COLNAMES_1[0])
name_col = rv.index(COLNAMES_1[1])
owner_col = rv.index(COLNAMES_1[2])
latitude_col = rv.index(COLNAMES_1[3])
longitude_col = rv.index(COLNAMES_1[4])
capacity_col = rv.index(COLNAMES_1[5])
fuel_col = rv.index(COLNAMES_1[6])
source_col = rv.index(COLNAMES_1[7])
date_col = rv.index(COLNAMES_1[8])

print(u"Reading file 1...")

for i in xrange(1, sheet.nrows):

    # read in row
    row = sheet.row_values(i)

    if pw.format_string(row[country_col]) != COUNTRY_NAME:
        continue

    try:
        name = pw.format_string(row[name_col], None)     # already in unicode
        if not name:
            print(u"-Error: No name on row {0}".format(i+1))
            continue
    except:
        print(u"-Error: Can't read name of plant on row {0}".format(i + 1))
        name = pw.NO_DATA_UNICODE    # without this, next pass thru loop uses old name
        continue

    try:
        owner = pw.format_string(row[owner_col], None)
    except:
        print(u"-Error: Can't read owner of plant with name {0}".format(name))
        owner = pw.NO_DATA_UNICODE

    try:
        fuel = pw.standardize_fuel(row[fuel_col], fuel_thesaurus)
    except:
        print(u"-Error: Can't read fuel for plant with name {0}".format(name))
        fuel = pw.NO_DATA_SET

    try:
        capacity = float(row[capacity_col])
    except:
        print(u"-Error: Can't read capacity for plant with name {0}".format(name))
        capacity = pw.NO_DATA_NUMERIC

    try:
        latitude = float(row[latitude_col])
        longitude = float(row[longitude_col])
        geolocation_source = SOURCE_NAME
    except:
        print(u"-Error: Can't read lat/long for plant with name {0}".format(name))
        latitude = pw.NO_DATA_NUMERIC
        longitude = pw.NO_DATA_NUMERIC
        geolocation_source = pw.NO_DATA_UNICODE

    try:
        source = pw.format_string(row[source_col], None)
    except:
        print(u"-Error: Can't read data source for plant with name {0}".format(name))
        source = pw.NO_DATA_UNICODE

    try:
        data_date = (int(str(row[date_col])[0:4]))
    except:
        print(u"-Error:Can't read reference date for plant with name {0}".format(name))
        data_date = pw.NO_DATA_NUMERIC

    # assign ID number
    idnr = pw.make_id(SAVE_CODE, i)
    new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
    new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=COUNTRY_NAME,
        plant_owner=owner, plant_cap_year=data_date,
        plant_location=new_location, plant_coord_source=geolocation_source,
        plant_fuel=fuel, plant_capacity=capacity,
        plant_source=source, plant_source_url=SOURCE_URL_1)
    plants_dictionary[idnr] = new_plant

# use this for id incrementing in next file
max_id = i

# 2: read in NACEI renewable plants
book = xlrd.open_workbook(RAW_FILE_NAME_2, encoding_override=ENCODING)
sheet = book.sheet_by_name(TAB_NAME_2)

rv = sheet.row_values(0)
country_col = rv.index(COLNAMES_2[0])
name_col = rv.index(COLNAMES_2[1])
owner_col = rv.index(COLNAMES_2[2])
latitude_col = rv.index(COLNAMES_2[3])
longitude_col = rv.index(COLNAMES_2[4])
capacity_col = rv.index(COLNAMES_2[5])
fuel_col = rv.index(COLNAMES_2[6])
source_col = rv.index(COLNAMES_2[7])
date_col = rv.index(COLNAMES_2[8])

print(u"Reading file 2...")

for i in xrange(1, sheet.nrows):

    # read in row
    row = sheet.row_values(i)

    if pw.format_string(row[country_col]) != COUNTRY_NAME:
        continue

    try:
        capacity = float(row[capacity_col])
        if capacity >= 100:
            continue                # already read in all plants >= 100 MW in 1st file
    except:
        print(u"-Error: Can't read capacity for plant with name {0}".format(name))
        capacity = pw.NO_DATA_NUMERIC

    try:
        name = pw.format_string(row[name_col], None)     # already in unicode
        if not name:
            print(u"-Error: No name on row {0}".format(i + 1))
            continue
    except:
        print(u"-Error: Can't read name of plant on row {0}".format(i + 1))
        name = pw.NO_DATA_UNICODE    # without this, next pass thru loop uses old name
        continue

    try:
        owner = pw.format_string(row[owner_col], None)
    except:
        print(u"-Error: Can't read owner of plant with name {0}".format(name))
        owner = pw.NO_DATA_UNICODE

    try:
        fuel = pw.standardize_fuel(row[fuel_col], fuel_thesaurus)
    except:
        print(u"-Error: Can't read fuel for plant with name {0}".format(name))
        fuel = pw.NO_DATA_SET

    try:
        latitude = float(row[latitude_col])
        longitude = float(row[longitude_col])
        geolocation_source = SOURCE_NAME
    except:
        print(u"-Error: Can't read lat/long for plant with name {0}".format(name))
        latitude = pw.NO_DATA_NUMERIC
        longitude = pw.NO_DATA_NUMERIC
        geolocation_source = pw.NO_DATA_UNICODE

    try:
        source = pw.format_string(row[source_col], None)
    except:
        print(u"-Error: Can't read data source for plant with name {0}".format(name))
        source = pw.NO_DATA_UNICODE

    try:
        data_date = (int(str(row[date_col])[0:4]))
    except:
        print(u"-Error:Can't read reference date for plant with name {0}".format(name))
        data_date = pw.NO_DATA_NUMERIC

    # assign ID number
    idnr = pw.make_id(SAVE_CODE, i + max_id)
    new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
    new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=COUNTRY_NAME,
        plant_owner=owner, plant_cap_year=data_date,
        plant_location=new_location, plant_coord_source=geolocation_source,
        plant_fuel=fuel, plant_capacity=capacity,
        plant_source=source, plant_source_url=SOURCE_URL_2)
    plants_dictionary[idnr] = new_plant

# use this id for incrementing in the next file
max_id = max_id + i

# 3: read in conventional plants under 100MW from CRE permit data
modalities = [u"GEN.", u"COG.", u"P.P.", u"P.I.E."]

book = xlrd.open_workbook(RAW_FILE_NAME_3, encoding_override=ENCODING)
sheet = book.sheet_by_name(TAB_NAME_3)

rv = sheet.row_values(1)
idval_col = rv.index(COLNAMES_3[0])
owner_col = rv.index(COLNAMES_3[1])
name_col = rv.index(COLNAMES_3[2])
mode_col = rv.index(COLNAMES_3[3])
capacity_col = rv.index(COLNAMES_3[4])
commissioning_col = rv.index(COLNAMES_3[5])
fuel_col = rv.index(COLNAMES_3[6])
status_col = rv.index(COLNAMES_3[7])
location_col = rv.index(COLNAMES_3[8])

print(u"Reading file 3...")

for i in xrange(2, sheet.nrows):

    # read in row
    row = sheet.row_values(i)

    try:
        idval = int(row[idval_col])
    except:
        #print(u"-Error: Can't read ID val on row {0}".format(i+1))
        continue

    try:
        mode = pw.format_string(row[mode_col], None)
        if mode not in modalities:
            continue
    except:
        print(u"-Error: Can't read mode for plant with ID val {0}".format(idval))
        continue

    try:
        capacity = float(row[capacity_col])
        if capacity >= 100:
            continue            # read all data on 100MW+ plants in previous files
    except:
        print(u"-Error: Can't read capacity for plant with name {0}".format(name))
        capacity = pw.NO_DATA_NUMERIC

    try:
        name = pw.format_string(row[name_col], None)     # already in unicode
        if not name:
            #print(u"-Error: No name on row {0}".format(i+1))
            continue
    except:
        print(u"-Error: Can't read name of plant on row {0}".format(i + 1))
        name = pw.NO_DATA_UNICODE    # without this, next pass thru loop uses old name
        continue

    try:
        owner = pw.format_string(row[owner_col], None)
    except:
        print(u"-Error: Can't read owner of plant with name {0}".format(name))
        owner = pw.NO_DATA_UNICODE

    try:
        fuel = pw.standardize_fuel(row[fuel_col], fuel_thesaurus)
    except:
        print(u"-Error: Can't read fuel for plant with name {0}".format(name))
        fuel = pw.NO_DATA_SET

    try:
        location = pw.format_string(row[location_col], None)
        latitude = pw.NO_DATA_NUMERIC
        longitude = pw.NO_DATA_NUMERIC
        geolocation_source = SOURCE_NAME_CRE
    except:
        print(u"-Error: Can't read location for plant with name {0}".format(name))
        location = pw.NO_DATA_UNICODE
        latitude = pw.NO_DATA_NUMERIC
        longitude = pw.NO_DATA_NUMERIC
        geolocation_source = pw.NO_DATA_UNICODE

    try:
        com_date = row[rv[commissioning_col]]
    except:
        #print(u"-Error:Can't read reference date for plant with name {0}".format(name))
        com_date = pw.NO_DATA_UNICODE

    # assign ID number
    idnr = pw.make_id(SAVE_CODE, i + max_id)   # probably should use idval somehow
    new_location = pw.LocationObject(location, latitude, longitude)
    new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=COUNTRY_NAME,
        plant_owner=owner, plant_cap_year=SOURCE_YEAR,
        plant_location=new_location, plant_coord_source=geolocation_source,
        plant_fuel=fuel, plant_capacity=capacity,
        plant_source=SOURCE_URL_3, plant_source_url=SOURCE_URL_3)
    plants_dictionary[idnr] = new_plant

# report on plants read from file
print(u"Loaded {0} plants to database.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
