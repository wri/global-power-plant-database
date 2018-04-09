# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_finland.py
Get power plant data from Energiavirasto (Finland's energy authority) and convert to the Global Power Plant Database format.
See http://www.energiavirasto.fi/en/voimalaitosrekisteri for description of the dataset
Unicode: Using unicodecsv, so we can read and write more easily to/from unicode.
"""

import xlrd
import sys, os
import re

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Finland"
SOURCE_NAME = u"Energiavirasto"
SOURCE_URL = u"http://www.energiavirasto.fi/en/voimalaitosrekisteri"
SAVE_CODE = u"FIN"
RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="finland_power_plants.xlsx")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="finland_database.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
DATASET_URL = "http://www.energiavirasto.fi/documents/10191/0/Energiaviraston+Voimalaitosrekisteri+010117.xlsx/fa2c9bd2-e436-4dbb-a2e5-074ecbf11d23"

COLS = {"name": 0, "owner": 1, "capacity_max": 17, "capacity_avg": 18,
        "gen_type": 7, "fuel_type":[20, 21, 22]}
TAB_NAME = "English"

pattern = r" -?\D?\d+.?" # space, optional hyphen or letter followed by 0-3 digits followed by any characters except newline
def regex_match(name_1, name_2):
    if re.sub(pattern, "", name_1) == re.sub(pattern, "", name_2):
        return re.sub(pattern, "''", name_1)
    return False

# optional raw file(s) download
DOWNLOAD_FILES = pw.download(COUNTRY_NAME, {RAW_FILE_NAME: DATASET_URL})

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# create dictionary for power plant objects
units_dictionary = {}
plants_dictionary = {}

# Parse url to read the data year
time_updated = re.search("([0-9]{6}\.xlsx)", DATASET_URL).group(0)
year_updated = "20" + time_updated[-7:-5]   # 4-digit year

# Open the workbook
wb = xlrd.open_workbook(RAW_FILE_NAME)
ws = wb.sheet_by_name(TAB_NAME)

print("Reading in plants...")
count_unit = 1
header_row = True
for row_id in xrange(0, ws.nrows):
    if header_row == True:
        try:
            if ws.cell(row_id, 0).value == "Name":
                header_row = False
            else:
                continue
        except:
            continue
    else:   # data rows
        rv = ws.row_values(row_id)
        try:
            name = pw.format_string(rv[COLS["name"]])
        except:
            print(u"-Error: Can't read plant name.")
            continue
        try:
            owner = pw.format_string(rv[COLS["owner"]], None)
        except:
            owner = pw.NO_DATA_UNICODE
            print(u"-Error: Can't read plant owner.")
        try:
            capacity_max = float(rv[COLS["capacity_max"]])
        except:
            capacity_max = pw.NO_DATA_NUMERIC
            print(u"-Error: Can't read capacity_max for plant {0}.".format(name))
        try:
            gen_type = pw.format_string(rv[COLS["gen_type"]]) # generation technology type
        except:
            gen_type = pw.NO_DATA_UNICODE
            print u"-Error: Can't read plant generation technology."
        if gen_type.lower() == u"hydro power":
            fuels = set([u"Hydro"])
        elif gen_type.lower() == u"wind power":
            fuels = set([u"Wind"])
        else:
            fuels = pw.NO_DATA_SET
            for i in COLS["fuel_type"]:
                try:
                    if rv[i] == "None": continue
                    fuel = pw.standardize_fuel(rv[i], fuel_thesaurus)
                    fuels.update(fuel)
                except:
                    continue

        new_location = pw.LocationObject(pw.NO_DATA_UNICODE, pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC)
        idnr = u"{:4}{:06d}".format("REF", count_unit)
        new_unit = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_owner=owner, plant_fuel=fuels,
                plant_country=unicode(COUNTRY_NAME), plant_capacity=capacity_max, plant_cap_year=year_updated,
                plant_source=SOURCE_NAME, plant_source_url=DATASET_URL, plant_location=new_location)
        units_dictionary[idnr] = new_unit
        count_unit += 1

# Aggregate units to plant level
sorted_units = sorted(units_dictionary.values(), key = lambda x: x.name)    # units are sorted by name
count_plant = 1
i = 0
while i < len(sorted_units)-1:
    j = i + 1
    idnr = pw.make_id(SAVE_CODE, count_plant)
    matched_name = regex_match(sorted_units[i].name, sorted_units[j].name)  # return a string if there is a match, otherwise False
    plant_name = matched_name if matched_name else sorted_units[i].name
    owner = sorted_units[i].owner
    fuels = sorted_units[i].fuel
    country = sorted_units[i].country
    capacity = sorted_units[i].capacity
    source = sorted_units[i].source
    location = sorted_units[i].location
    while matched_name and owner == sorted_units[j].owner:
        fuels = fuels | sorted_units[j].fuel
        capacity += sorted_units[j].capacity
        j += 1
    i = j
    plants_dictionary[idnr] = pw.PowerPlant(plant_idnr=idnr, plant_name=plant_name, plant_owner=owner, plant_fuel=fuels,
            plant_country=country, plant_capacity=capacity, plant_cap_year=year_updated,
            plant_source=source, plant_source_url=DATASET_URL, plant_location=location)
    count_plant += 1

pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# pickle database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print("Pickled database to {0}".format(SAVE_DIRECTORY))
