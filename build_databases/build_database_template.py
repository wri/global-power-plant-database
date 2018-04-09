# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_[country].py
Get power plant data from [country] and convert to the Global Power Plant Database format.
Data Source: [Institution(s) providing these data sets]
Additional information: [URL]
Issues: [etc]

Notes:
- Unicode: All unicode conversion occurs in the powerplant_database.py module function format_string().
        Do not use packages such as unicodecsv.
- XML parsing: use xml.etree
- HTML parsing: use html
- Location coordinates conversion: TBD
"""

import csv
import sys, os

# import [other packages]

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"NAME OF COUNTRY OR GLOBAL"
SOURCE_NAME = u"NAME OF ORGANIZATION OR DATABASE[, OTHER ORGANIZATION OR DATABASE]"
SOURCE_URL = u"PRIMARY URL"
SAVE_CODE = u"3-LETTER SAVE CODE HERE (ISO CODE FOR COUNTRIES)"
YEAR_POSTED = 2017  # Year of data posted on line
RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="RAW FILE HERE")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_{0}.csv".format(SAVE_CODE))
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
#LOCATION_FILE_NAME = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE, filename="locations_{0}.csv".format(SAVE_CODE))

# other parameters as needed

# specific functions here
# def xyz():
#     pass

# optional raw file(s) download
# True if specified --download, otherwise False
FILES = {RAW_FILE_NAME: URL} # dictionary of saving directories and corresponding urls
DOWNLOAD_FILES = pw.download(NAME_OF_DATABASE, FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

# example for reading CSV file
# specify column names used in raw file
COLNAMES = ["NAME COL", "CAPACITY COL", "FUEL COL", "LATITUDE COL", "LONGITUDE COL", "ETC"]

# read file line-by-line
with open(RAW_FILE_NAME, 'rU') as f:
    datareader = csv.reader(f)
    headers = [x.lower() for x in datareader.next()]
    name_col = headers.index(COLNAMES[0])
    capacity_col = headers.index(COLNAMES[1])
    fuel_col = headers.index(COLNAMES[2])
    latitude_col = headers.index(COLNAMES[3])
    longitude_col = headers.index(COLNAMES[4])
    # additional columns here

    # if data source is for a single country
    country = SOURCE_NAME

    # read each row in the file
    count = 1
    for row in datareader:
        try:
            name = pw.format_string(row[name_col])
        except:
            print(u"Error: Can't read plant name.")
            continue                       # must have plant name - don't read plant if not
        try:
            capacity = float(pw.format_string(row[capacity_col]))   # note: may need to convert to MW
        except:
            print(u"Error: Can't read capacity for plant {0}.".format(name))
        try:
            fuel = pw.standardize_fuel(row[fuel_col], fuel_thesaurus)
        except:
            print(u"Error: Can't read fuel type for plant {0}.".format(name))
        try:
            latitude = float(row[latitude_col])
            longitude = float(row[longitude_col])
        except:
            latitude, longitude = pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC

        # assign ID number
        idnr = pw.make_id(SAVE_CODE, count)
        new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
        new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=country,
            plant_location=new_location, plant_fuel=fuel, plant_capacity=capacity,
            plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL)
        plants_dictionary[idnr] = new_plant
        count += 1

# report on plants read from file
print(u"Loaded {0} plants to database.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
