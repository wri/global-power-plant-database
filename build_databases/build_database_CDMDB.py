# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_CDMDB.py
Get power plant data from CDM and convert to the Global Power Plant Database format.
Data Source: Clean Development Mechanism
Additional information: https://cdm.unfccc.int
Issues: [etc]
"""

import argparse
import csv
import sys, os
import xlrd
from lxml import etree

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Global"
SOURCE_NAME = u"Clean Development Mechanism"
SOURCE_URL = u"https://cdm.unfccc.int"
SAVE_CODE = u"CDMDB"
RAW_FILE_NAME1 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="Database for PAs and PoAs.xlsx")
RAW_FILE_NAME2 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="projectsLocationAll.xml")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_CDMDB.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
URL1 = "https://cdm.unfccc.int/Statistics/Public/files/Database%20for%20PAs%20and%20PoAs.xlsx"
URL2 = "https://cdm.unfccc.int/Projects/MapApp/projectsLocationAll.xml"

# project file specfications
TAB_NAME = "CDM activities"
COLNAMES = ["CDM project reference number", "Unique project identifier (traceable with Google)",
    "Registration project title", "Project type (UNEP Risoe)", "Website project status",
    "List of Host countries (ISO 2)", "Installed capacity (MW elec/thermal)", "DOE"]

# project types to read
PROJECT_TYPES_TO_READ = [   "Biogas", 
                            "Biomass energy", 
                            "Coal bed/mine methane",
                            "EE industry",
                            "Fugitive",
                            "Geothermal", 
                            "Hydro", 
                            "Landfill gas", 
                            "Methane avoidance",
                            "Mixed renewables", 
                            "Solar", 
                            "Tidal", 
                            "Wind"
                            ]

# download raw files if --download specified
FILES = {RAW_FILE_NAME1: URL1, RAW_FILE_NAME2: URL2}
DOWNLOAD_FILES = pw.download(SOURCE_NAME, FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name and iso dictionary
country_dictionary = pw.make_country_dictionary()
iso2_to_country_names = {}
for name,country_object in country_dictionary.iteritems():
    iso2 = country_object.iso_code2
    iso2_to_country_names[iso2] = name

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plant locations...")

# load and process CDM projects locations file
tree = etree.parse(RAW_FILE_NAME2)
root = tree.getroot()
project_locations = {}
for state in root.findall('state'):
    if state.get('id') == 'point':
        name_str = state.find('name').text
        name = name_str.split(':')[-1].strip()
        ref_str = state.find('url').text
        ref = int(ref_str.split("=")[-1])
        loc_str = state.find('loc').text
        loc_vals = loc_str.split(',')
        latitude = float(loc_vals[0])
        longitude = float(loc_vals[1])
        project_locations[ref] = {'name': name, 'latitude': latitude, 'longitude': longitude}

print("Loaded {0} project locations.".format(len(project_locations)))

# load and process CDM projects details file
print("Reading in plants...")
book = xlrd.open_workbook(RAW_FILE_NAME1)
sheet = book.sheet_by_name(TAB_NAME)

# read headers
rv = sheet.row_values(0)
ref_col = rv.index(COLNAMES[0])
id_col = rv.index(COLNAMES[1])
name_col = rv.index(COLNAMES[2])
type_col = rv.index(COLNAMES[3])
status_col = rv.index(COLNAMES[4])
countries_col = rv.index(COLNAMES[5])
capacity_col = rv.index(COLNAMES[6])
owner_col = rv.index(COLNAMES[7])

for i in xrange(1, sheet.nrows):
    rv = sheet.row_values(i)
    try:
        ref = int(rv[ref_col])
        if not ref:
            print("-Error reading ref from: {0}".format(rv[ref_col]))
            continue
    except:
        continue

    try:
        project_type = pw.format_string(rv[type_col])
        if project_type not in PROJECT_TYPES_TO_READ:  # don't read all project types
            continue

        try:
            fuel = pw.standardize_fuel(project_type, fuel_thesaurus)
        except:
            print("-Error reading fuel: {0}".format(project_type))
            fuel = pw.NO_DATA_SET
    except:
        print(u"-Error: Can't read project type for project {0}.".format(ref))
        continue

    try:
        status = pw.format_string(rv[status_col])
        if status != u"Registered":
            continue
    except:
        print(u"-Error: Can't read project status for project {0}.".format(ref))
        continue

    try:
        capacity = float(rv[capacity_col])
    except:
        continue

    try:
        name = pw.format_string(rv[name_col])
        if not name:
            continue        # don't read rows that lack a plant name (footnotes, etc)
    except:
        print(u"-Error: Can't read plant name for plant with ref {0}.".format(ref))
        continue

    """
    # Note: The DOE field represents the certifying body, not the plant owner
    try:
        owner = pw.format_string(rv[owner_col])
    except:
        owner = pw.NO_DATA_UNICODE
    """
    owner = pw.NO_DATA_UNICODE

    if ref in project_locations.keys():
        latitude = project_locations[ref]['latitude']
        longitude = project_locations[ref]['longitude']
    else:
        latitude = pw.NO_DATA_NUMERIC
        longitude = pw.NO_DATA_NUMERIC

    try:
        countries = pw.format_string(rv[countries_col])
        country_list_iso2 = countries.split(";")
        country_list = []
        for iso2 in country_list_iso2:
            country_list.append(iso2_to_country_names[iso2.strip()])
        country = " ".join(country_list)
    except:
        print(u"-Error: Can't read countries from string {0}; list: {1}.".format(countries, country_list_iso2))
        country = pw.NO_DATA_UNICODE

    # assign ID number
    idnr = pw.make_id(SAVE_CODE,ref)
    new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
    new_plant = pw.PowerPlant(plant_idnr=idnr,plant_name=name, plant_country=country,
        plant_location=new_location, plant_coord_source=SOURCE_NAME,
        plant_fuel=fuel, plant_capacity=capacity,
        plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL, plant_owner=owner)
    plants_dictionary[idnr] = new_plant

# report on plants read from file
print(u"Loaded {0} plants to database.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
