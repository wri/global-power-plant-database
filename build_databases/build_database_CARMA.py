# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_CARMA.py
Get power plant data from CARMA and convert to the Global Power Plant Database format.
Data Source: CARMA
Additional information: CARAM data api: http://carma.org/api/
"""

import csv
import sys, os

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"GLOBAL"
SOURCE_NAME = u"CARMA"
SOURCE_URL = "http://carma.org/"
SAVE_CODE = u"CARMA"
RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="Full_CARMA_2009_Dataset_Power_Watch.csv")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_CARMA.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
YEAR_UPDATED = 2009
COLNAMES = ['plant.id', 'plant', 'lat', 'lon', 'iso3']

# optional raw file(s) download
# note: API documentation says to specify "limit=0" to download entire dataset. 
# this doesn't work; it only downloads 2000 plants. 
# full dataset has about 51k plants, so specify 60k for limit to be safe.
#URL = "http://carma.org/javascript/ajax-lister.php?type=plant&sort=carbon_present%20DESC&page=1&time=present&m1=world&m2=plant&m3=&m4=&export=make"
#URL = "http://carma.org/api/1.1/searchPlants?raw=1&limit=20000"
#FILES = {RAW_FILE_NAME: URL}
#DOWNLOAD_FILES = pw.download(SOURCE_NAME, FILES)
print("Download disabled; using local raw database file.")

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

print(u"Reading in plants...")
coord_skip_count = 0
# read file line-by-line
with open(RAW_FILE_NAME, 'rU') as f:
    datareader = csv.reader(f)
    headers = [x.lower() for x in datareader.next()]
    id_col = headers.index(COLNAMES[0])
    name_col = headers.index(COLNAMES[1])
    latitude_col = headers.index(COLNAMES[2])
    longitude_col = headers.index(COLNAMES[3])
    country_col = headers.index(COLNAMES[4])
 
    for row in datareader:

        idval = int(row[id_col])
        name = pw.format_string(row[name_col])
        try:
            latitude = float(row[latitude_col])
            longitude = float(row[longitude_col])
        except:
            coord_skip_count += 1
            continue
        country = row[country_col]     # note: this is the ISO3 code so no need to convert

        # assign ID number
        idnr = pw.make_id(SAVE_CODE, idval)
        new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
        new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name,  plant_country=country,
            plant_location=new_location, plant_coord_source=SOURCE_NAME,
            plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL)
        plants_dictionary[idnr] = new_plant


# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))
print("Skipped {0} plants because of missing lat/long coordinates.".format(coord_skip_count))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)
#
# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
