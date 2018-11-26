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
import sys, os
import csv

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Australia"
SAVE_CODE = u"AUS"
SOURCE_NAME = u"Australian Renewable Energy Mapping Infrastructure"
SOURCE_URL = u"http://services.ga.gov.au/site_3/rest/services/Electricity_Infrastructure/MapServer"

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

RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="australia_power_plants.xml")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_AUS.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
STATIC_ID_FILENAME = pw.make_file_path(fileType="resource", subFolder='AUS', filename="AUS_plants.csv")

# other parameters
API_BASE = "http://services.ga.gov.au/site_3/services/Electricity_Infrastructure/MapServer/WFSServer"
API_CALL = "service=WFS&version=1.1.0&request=GetFeature&typeName=National_Major_Power_Stations"

# optional raw file(s) download
URL = API_BASE + "?" + API_CALL
FILES = {RAW_FILE_NAME: URL,
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
linking_table = {k['aremi_oid']: k for k in csv.DictReader(open(STATIC_ID_FILENAME))}

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")
print(u"Reading NGER files to memory...")

# read NGER file into a list, so the facilities can be referenced by their index in the original file
nger_1617 = list(csv.DictReader(open(NGER_FILENAME_1617)))
nger_1516 = list(csv.DictReader(open(NGER_FILENAME_1516)))
nger_1415 = list(csv.DictReader(open(NGER_FILENAME_1415)))
nger_1314 = list(csv.DictReader(open(NGER_FILENAME_1314)))
nger_1213 = list(csv.DictReader(open(NGER_FILENAME_1213)))

# create a dictinary of namespaces
ns = {"gml": "http://www.opengis.net/gml",
    "Electricity_Infrastructure": "WFS"}

# read data from XML file and parse
count = 1
with open(RAW_FILE_NAME, "rU") as f:
    tree = ET.parse(f)
    root = tree.getroot()
    for station in tree.findall("gml:featureMember", ns):
        plant = station.find("Electricity_Infrastructure:National_Major_Power_Stations", ns)
        name = pw.format_string(plant.find("Electricity_Infrastructure:NAME", ns).text)

        # get object id from AREMI (variable through time)
        plant_oid = plant.find("Electricity_Infrastructure:OBJECTID", ns).text
        # check if plant is already known, and skip if there is not a record (includes cases where AREMI has duplicated plants)
        if plant_oid not in linking_table:
            print(u"Error: Don't have prescribed ID for plant {0}; OID={1}.".format(name, plant_oid))
            continue
        # get the assigned GPPD IDNR as an int, stripping the 'AUS' prefix
        plant_id = int(linking_table[plant_oid]['gppd_idnr'][3:])

        try:
            owner = pw.format_string(plant.find("Electricity_Infrastructure:OWNER", ns).text)
        except:
            owner = pw.NO_DATA_UNICODE
        fuel = pw.standardize_fuel(plant.find("Electricity_Infrastructure:PRIMARYFUELTYPE", ns).text,fuel_thesaurus)
        try:
            capacity = plant.find("Electricity_Infrastructure:GENERATIONMW", ns).text
            capacity = float(capacity)
        except:
            print(u"Error: Can't read capacity for plant {0}.".format(name))
            capacity = pw.NO_DATA_NUMERIC
        coords = plant.find("Electricity_Infrastructure:SHAPE/gml:Point/gml:pos", ns).text.split(" ")
        try:
            longitude = float(coords[0])
            latitude = float(coords[1])
            geolocation_source = SOURCE_NAME
        except:
            longitude, latitude = pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC
            geolocation_source = pw.NO_DATA_UNICODE

        # # Additional information for future interest
        # operational_status = plant.find('Electricity_Infrastructure:OPERATIONALSTATUS', ns).text)
        # technology = plant.find('Electricity_Infrastructure:GENERATIONTYPE', ns).text)
        # try:
            # subfuel = plant.find('Electricity_Infrastructure:PRIMARYSUBFUELTYPE', ns).text
        # except:
            # subfuel = fuel

        # date_updated format after split: YYYY-MM-DD
        try:
            year_updated = int(plant.find("Electricity_Infrastructure:REVISED", ns).text.split("T")[0][0:4])
        except:
            year_updated = pw.NO_DATA_NUMERIC

        # get generation data (if any) from the NGER datasets
        generation = []
        for yr, lookup in zip(
                range(2013, 2018),
                [nger_1213, nger_1314, nger_1415, nger_1516, nger_1617]
            ):
            index_title = 'nger_{0}-{1}_index'.format(yr-1, yr)
            # get the raw form of the nger indices field
            nger_indices_raw = linking_table[plant_oid][index_title]
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
                    print("Error with looking up NGER row for {0} (year = {1}; NGER index = {2};)".format(name, yr, idx))
                    continue
                gen_gj = nger_row['Electricity Production (GJ)']
                try:
                    gen_gwh = float(gen_gj.replace(",", ""))  / 3600.
                except:
                    print("Error with NGER generation for {0} (year = {1}; NGER index = {2}; value={3})".format(name, yr, idx, gen_gj))
                    pass
                else:
                    gwh += gen_gwh
            # TODO: give proper time bounds
            generation.append(pw.PlantGenerationObject.create(gwh, yr))


        # assign ID number
        idnr = pw.make_id(SAVE_CODE, plant_id)
        new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
        new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_owner=owner, 
            plant_country=COUNTRY_NAME,
            plant_location=new_location, plant_coord_source=geolocation_source,
            plant_fuel=fuel, plant_capacity=capacity,
            plant_generation=generation,
            plant_source=SOURCE_NAME, plant_cap_year=year_updated,
            plant_source_url=SOURCE_URL)
        plants_dictionary[idnr] = new_plant
        count += 1

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
