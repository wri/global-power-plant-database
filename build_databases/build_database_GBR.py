# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_GBR.py
Get power plant data from UK and convert to the Global Power Plant Database format.
Data Source: Department for Business, Energy & Industrial Strategy
Additional information: https://www.gov.uk/government/collections/digest-of-uk-energy-statistics-dukes
Additional information: https://www.gov.uk/government/publications/renewable-energy-planning-database-monthly-extract
Issues:
- DUKES data includes operational power stations but no location data. Not complete.
- Renewable Energy Planning Database includes only renewables. Does have location data, but it
is based on UK Ordinance Grid. Must convert to lat/long using projection.
- Must generate matching between plants to integrate two data sets.
- Must use GEO or CARMA matching for locations when DUKES plants cannot be matched to REPD sites.
- Unclear how to treat "Advanced Conversion Technologies" fuel type. Possibly biomass; see here
for possible guidance: https://www.ofgem.gov.uk/publications-and-updates/advanced-conversion-technology-act-fuel-measurement-and-sampling-fms-questionnaire-and-guidance-note .
"""

import argparse
import csv
import sys, os

import xlrd
import pyproj as pyproj

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"United Kingdom"
SOURCE_NAME = u"Department for Business Energy & Industrial Strategy"
SOURCE_NAME_REPD = u"UK Renewable Energy Planning Database"
SOURCE_URL = u"https://www.gov.uk/government/collections/digest-of-uk-energy-statistics-dukes;https://www.gov.uk/government/collections/renewable-energy-planning-data"
SAVE_CODE_GBR = u"GBR"
SAVE_CODE_GEO = u"GEODB"
SAVE_CODE_CARMA = u"CARMA"
RAW_FILE_NAME_REPD = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE_GBR, filename="Public_Database_-_Jan_2018.csv")
RAW_FILE_NAME_DUKES = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE_GBR, filename="DUKES_5.11.xls")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_GBR.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
GEO_DATABASE_FILE = pw.make_file_path(fileType="src_bin", filename="GEODB-Database.bin")
CARMA_DATABASE_FILE = pw.make_file_path(fileType="src_bin", filename="CARMA-Database.bin")
REPD_YEAR = 2016
DUKES_YEAR = 2016

# other params
PLANT_MATCHES = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE_GBR, filename="matches_GBR.csv")

# REDP information
URL_REPD = "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/682029/Public_Database_-_Jan_2018.csv"
COLNAMES_REPD = ["Ref ID", "Site Name", "Technology Type", "Installed Capacity (MWelec)",
            "Development Status", "X-coordinate", "Y-coordinate", "Operator (or Applicant)"]

# DUKES information
#COLS_2 = {"owner": 0, "name": 1, "fuel_type": 2, "capacity": 3, "operational_year": 4}
URL_DUKES = "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/632849/DUKES_5.11.xls"
COLNAMES_DUKES = ["Company Name", "Station Name", "Fuel", "Installed Capacity (MW)",
            "Year of commission or year generation began"]
TAB_NUMBER_DUKES = 2

# set up projection transformation
# thanks to John A. Stevenson: http://all-geo.org/volcan01010/2012/11/change-coordinates-with-pyproj/
wgs84 = pyproj.Proj("+init=EPSG:4326") # LatLon with WGS84 datum used by GPS units and Google Earth
osgb36 = pyproj.Proj("+init=EPSG:27700") # UK Ordnance Survey, 1936 datum

# raw files download
# True if specified --download, otherwise False
FILES = {RAW_FILE_NAME_REPD: URL_REPD, RAW_FILE_NAME_DUKES: URL_DUKES}
DOWNLOAD_FILES = pw.download(u"UK Renewable Energy Planning Database and DUKES", FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# load GEO and CARMA for matching coordinates
geo_database = pw.load_database(GEO_DATABASE_FILE)
print("Loaded {0} plants from GEO database.".format(len(geo_database)))
carma_database = pw.load_database(CARMA_DATABASE_FILE)
print("Loaded {0} plants from CARMA database.".format(len(carma_database)))

# read in plant matches file
with open(PLANT_MATCHES, "rbU") as f:
    f.readline() # skip headers
    csvreader = csv.reader(f)
    plant_matches = {}
    for row in csvreader:
        dukes_name = str(row[0])
        geo_id = pw.make_id(SAVE_CODE_GEO, int(row[1])) if row[1] else ""
        carma_id = pw.make_id(SAVE_CODE_CARMA, int(row[2])) if row[2] else ""
        repd_id = int(row[3]) if row[3] else ""
        plant_matches[dukes_name] = {"geo_id": geo_id, "carma_id": carma_id, "repd_id": repd_id}

# load and process Renewable Energy Planning Database
print(u"Reading in plants...")
country = COUNTRY_NAME
with open(RAW_FILE_NAME_REPD, "rU") as f:
    datareader = csv.reader(f)
    headers = datareader.next()
    while "Ref ID" not in headers:  # find header row
        headers = datareader.next()
    id_col = headers.index(COLNAMES_REPD[0])
    name_col = headers.index(COLNAMES_REPD[1])
    fuel_col = headers.index(COLNAMES_REPD[2])
    capacity_col = headers.index(COLNAMES_REPD[3])
    status_col = headers.index(COLNAMES_REPD[4])
    x_coordinate_col = headers.index(COLNAMES_REPD[5])
    y_coordinate_col = headers.index(COLNAMES_REPD[6])
    owner_col = headers.index(COLNAMES_REPD[7])

    # read each row in the file
    count = 1
    for row in datareader:
        if u"Operational" not in row[status_col]:
            continue                            # don't load non-operatioal plants
        try:
            name = pw.format_string(row[name_col])
        except:
            print(u"-Error: Can't read plant name.")
            continue                       # must have plant name - don't read plant if not
        try:
            idnr = int(row[id_col])
        except:
            print(u"-Error: Can't read ref id.")
            continue                        # must have ID number
        try:
            capacity = float(pw.format_string(row[capacity_col]))   # note: may need to convert to MW
        except:
            print(u"-Error: Can't read capacity for plant {0}.".format(name))
            capacity = 0.0
        try:
            fuel_type = pw.standardize_fuel(row[fuel_col], fuel_thesaurus)
            if not fuel_type:
                print("-Error: No fuel type for {0}.".format(row[fuel_col]))
        except:
            print("-Error: Can't read fuel for plant {0}.".format(name))
            fuel_type = set([])
        try:
            x_coordinate = float(row[x_coordinate_col].replace(",", ""))
            y_coordinate = float(row[y_coordinate_col].replace(",", ""))
            longitude, latitude = pyproj.transform(osgb36, wgs84, x_coordinate, y_coordinate)
            geolocation_source = SOURCE_NAME_REPD
        except:
            print(u"-Error: Can't read location for plant {0}.".format(name))
            latitude, longitude = pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC
            geolocation_source = pw.NO_DATA_UNICODE
        try:
            owner = pw.format_string(row[owner_col])
        except:
            print(u"-Error: Can't read owner for plant {0}.".format(name))
            owner = u""

        # now process plant
        plant_idnr = pw.make_id(SAVE_CODE_GBR, idnr)
        new_location = pw.LocationObject(latitude=latitude, longitude=longitude)
        new_plant = pw.PowerPlant(plant_idnr=plant_idnr, plant_name=name,
            plant_owner=owner, plant_country=country,
            plant_capacity=capacity, plant_cap_year=REPD_YEAR,
            plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL,
            plant_location=new_location, plant_coord_source=geolocation_source,
            plant_fuel=fuel_type)
        plants_dictionary[plant_idnr] = new_plant

# load and process DUKES file
book = xlrd.open_workbook(RAW_FILE_NAME_DUKES)
sheet = book.sheet_by_index(TAB_NUMBER_DUKES)

count = 1000001     # use this starting number for IDs when plant is not matched to REPD plant
first_data_row = 0
for i in xrange(sheet.nrows):
    # find headers
    rv = sheet.row_values(i)
    if "Company Name" in rv:
        owner_col = rv.index(COLNAMES_DUKES[0])
        name_col = rv.index(COLNAMES_DUKES[1])
        fuel_col = rv.index(COLNAMES_DUKES[2])
        capacity_col = rv.index(COLNAMES_DUKES[3])
        year_col = rv.index(COLNAMES_DUKES[4])
        first_data_row = i+1
        break

for i in xrange(first_data_row, sheet.nrows):
    # read in row
    rv = sheet.row_values(i)
    try:
        name = pw.format_string(rv[name_col]) # DUKES uses a * after name to indicate a CHP plant - address in fuel below
        if not name:
            continue        # don't read rows that lack a plant name (footnotes, etc)
    except:
        print(u"-Error: Can't read plant name for plant on row {0}.".format(i))
        continue
    try:
        capacity = float(rv[capacity_col])
    except:
        try:
            capacity = eval(rv[capacity_col])
        except:
            print("-Error: Can't read capacity for plant {0}".format(name))
            capacity = pw.NO_DATA_NUMERIC
    try:
        fuel_type = pw.standardize_fuel(rv[fuel_col], fuel_thesaurus)
        if not fuel_type:
            print(u"-Error: No fuel type for {0}.".format(row[fuel_col]))
        # Test name for * to see if it's a CHP plant
        if u"*" in name:
            name = name.replace(u"*","").strip()
            fuel_type.add(u"Cogeneration")
    except:
        print("-Error: Can't read fuel type of plant {0}.".format(name))
        fuel_type = pw.NO_DATA_SET
    try:
        owner = pw.format_string(rv[owner_col])
    except:
        print("-Error: Can't read owner for plant {0}.".format(name))
        owner = pw.NO_DATA_UNICODE

    # check if this is matched to a REPD plant
    create_new_plant = True
    if name in plant_matches.keys():
        # it's matched to a REPD plant; update plant info
        match_info = plant_matches[name]
        if match_info["repd_id"]:
            plant_id = pw.make_id(SAVE_CODE_GBR, int(plant_matches[name]["repd_id"]))
            plant = plants_dictionary[plant_id]
            create_new_plant = False
            # TODO: check if fuel type, capacity matches
            # decide what to add/adjust/change
        # test if matched to GEO
        elif match_info["geo_id"]:
                location = geo_database[match_info["geo_id"]].location
                geolocation_source = u"GEODB"
            # test if matched to CARMA
        elif match_info["carma_id"]:
            # debug - need to fix CARMA db
            try:
                location = carma_database[match_info["carma_id"]].location
                geolocation_source = u"CARMA"
            except:
                location = pw.LocationObject()
                geolocation_source = pw.NO_DATA_UNICODE
            # end debug
        else:
            # not matched
            location = pw.LocationObject()
            geolocation_source = pw.NO_DATA_UNICODE
    else:
        location = pw.LocationObject()
        geolocation_source = pw.NO_DATA_UNICODE

    if create_new_plant:
        plant_idnr = pw.make_id(SAVE_CODE_GBR, count)
        count += 1
        new_plant = pw.PowerPlant(plant_idnr=plant_idnr, plant_name=name,
            plant_owner=owner, plant_country=COUNTRY_NAME,
            plant_capacity=capacity, plant_cap_year=DUKES_YEAR,
            plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL,
            plant_location=location, plant_coord_source=geolocation_source,
            plant_fuel=fuel_type)
    plants_dictionary[plant_idnr] = new_plant

# report on plants read from file
print(u"Loaded {0} plants to database.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE_GBR, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
