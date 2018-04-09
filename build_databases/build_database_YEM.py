# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_yemen.py
Converts plant-level data from Arab Union of Electricity to the Global Power Plant Database format.
Note: The AUE data is in .xls format. We need to use xlrd to read this.
Note: xlrd decodes values to unicode as it reads them.
"""

import xlrd
import sys, os

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = 'Yemen'
RAW_SOURCE = "AUE"
SAVE_CODE = u"YEM"
RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=RAW_SOURCE, filename="Manual Of Power Stations2016.xls")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="yemen_database.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
URL = "http://www.auptde.org/Article_Files/Manual%20Of%20Power%20Stations2016.xls"

COLS = {'name': 0, 'capacity': 3, 'year_built': 4, 'fuel_type': 5}
START_ROW = 4
END_ROW = 41
TAB_NUMBER = 0

# optional raw file download
DOWNLOAD_FILES = pw.download(COUNTRY_NAME, {RAW_FILE_NAME: URL})

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# load file
book = xlrd.open_workbook(RAW_FILE_NAME)
sheet = book.sheet_by_index(TAB_NUMBER)
capacity_list = []
year_built_list = []

for i in range(START_ROW, END_ROW):
    # reset variables
    capacity = None
    year_built = None
    fuel_type = None

    # read in row
    rv = sheet.row_values(i-1)
    name_str = rv[COLS['name']]
    capacity_val = rv[COLS['capacity']]
    year_built_val = rv[COLS['year_built']]
    fuel_type_str = rv[COLS['fuel_type']]

    if capacity_val:
        try:
            capacity = float(capacity_val)
        except:
            try:
                capacity = eval(capacity_val)
            except:
                print("Could not evaluate {0}".format(capacity_val))

    if year_built_val:
        year_built = int(year_built_val)

    if fuel_type_str:
        if '+' in fuel_type_str:
            fuel_type = None
        else:
            fuel_type = pw.standardize_fuel(fuel_type_str, fuel_thesaurus)

    if name_str:  # if true, this row begins a new plant
        # first process the previous plant unless this is the first entry
        if i > START_ROW:
            print i
            print fuel_type_set
            raw_input()
            total_capacity = sum(capacity_list)
            average_year_built = sum(year_built_list) / len(year_built_list)  # TODO: fix this
            new_location = pw.LocationObject(latitude=0.0, longitude=0.0)
            new_plant = pw.PowerPlant(plant_idnr=plant_idnr, plant_name=name,
                            plant_country=COUNTRY_NAME, plant_capacity=total_capacity,
                            plant_fuel=fuel_type_set, plant_source=URL, plant_location=new_location)
            plants_dictionary[plant_idnr] = new_plant
            print("Recording plant {0} with ID: {1}, capacity: {2}, fuel: {3}".format(name, plant_idnr, total_capacity, fuel_type_set))

        # next process this plant

        name = pw.format_string(name_str)
        plant_idnr = pw.make_id(SAVE_CODE, i)
        capacity_list = [capacity]
        year_built_list = [year_built]
        fuel_type_set = fuel_type

    else:    # not a new plant, just a new line
        if capacity_val:
            capacity_list.append(capacity)
        if year_built_val:
            year_built_list.append(year_built)
        if fuel_type and fuel_type not in fuel_type_set:
            fuel_type_set.update(fuel_type)

# complete loop, add final plant
total_capacity = sum(capacity_list)
average_year_built = sum(year_built_list) / len(year_built_list)  # TODO: fix this
new_location = pw.LocationObject(latitude=0.0, longitude=0.0)
new_plant = pw.PowerPlant(plant_idnr=plant_idnr, plant_name=name, plant_country=COUNTRY_NAME,
		plant_capacity=total_capacity, plant_fuel=fuel_type_set,
		plant_source=URL, plant_location=new_location)
plants_dictionary[plant_idnr] = new_plant
print("Recording plant {0} with ID: {1}, capacity: {2}, fuel: {3}".format(name, plant_idnr, total_capacity, fuel_type_set))

print("Loaded {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print("Pickled database to {0}".format(SAVE_DIRECTORY))
