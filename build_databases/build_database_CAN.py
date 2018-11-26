# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_CAN.py
Get power plant data from Canada and convert to the Global Power Plant Database format.
Data Source 1: North American Cooperation on Energy Information (via Natural Resources Canada)
- 
Explanation:
We use NACEI (government-supplied data) for all renewable plants and all conventional plants of 100MW or higher.
We supplement this with Fusion Table data manually collected for conventional plants < 100 MW.
-
Issues:
- NACIE data are very good, but do not include conventional plants below 100 MW.
- Source Agency is very heterogenous for Canadian data in NACIE. We make this all Natural Resources Canada.
- However, we keep the year of reported data for each source, and use this for reported data year.
"""

import csv
import sys, os
import xlrd

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Canada"
SOURCE_NAME_1 = u"Natural Resources Canada"
SAVE_CODE = u"CAN"

RAW_FILE_NAME_1 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="PowerPlantsAllGE100MW_NorthAmerica_201708.xlsx")
SOURCE_URL_1 = u"http://ftp.maps.canada.ca/pub/nacei_cnaie/energy_infrastructure/PowerPlantsAllGE100MW_NorthAmerica_201708.xlsx"
RAW_FILE_NAME_2 = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="PowerPlantsRenewGE1MW_NorthAmerica_201708.xlsx")
SOURCE_URL_2 = u"ftp://ftp.maps.canada.ca/pub/nacei_cnaie/energy_infrastructure/PowerPlantsRenewGE1MW_NorthAmerica_201708.xlsx"
FUSION_TABLE_FILE = pw.make_file_path(fileType="raw", subFolder="WRI", filename="Canada.csv")

CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_CAN.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
SOURCE_YEAR = 2017
ENCODING = 'UTF-8'

# True if specified --download, otherwise False
FILES = {RAW_FILE_NAME_1: SOURCE_URL_1,
        RAW_FILE_NAME_2: SOURCE_URL_2}
DOWNLOAD_FILES = pw.download("NRC data", FILES)

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
        print(u"-Error: Can't read name of plant on row {0}".format(i+1))
        name = pw.NO_DATA_UNICODE    # without this, next pass thru loop uses old name
        continue

    try:
        owner = pw.format_string(row[owner_col], None)
    except:
        print(u"-Error: Can't read owner of plant with name {0}".format(name))
        owner = pw.NO_DATA_UNICODE

    try:
        primary_fuel = pw.standardize_fuel(row[fuel_col], fuel_thesaurus, as_set=False)
    except:
        print(u"-Error: Can't read fuel for plant with name {0}".format(name))
        primary_fuel = pw.NO_DATA_UNICODE

    try:
        capacity = float(row[capacity_col])
    except:
        print(u"-Error: Can't read capacity for plant with name {0}".format(name))
        capacity = pw.NO_DATA_NUMERIC

    try:
        latitude = float(row[latitude_col])
        longitude = float(row[longitude_col])
        geolocation_source = SOURCE_NAME_1
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
        plant_primary_fuel=primary_fuel, plant_capacity=capacity,
        plant_source=SOURCE_NAME_1, plant_source_url=SOURCE_URL_1)
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
            print(u"-Error: No name on row {0}".format(i+1))
            continue
    except:
        print(u"-Error: Can't read name of plant on row {0}".format(i+1))
        name = pw.NO_DATA_UNICODE    # without this, next pass thru loop uses old name
        continue

    try:
        owner = pw.format_string(row[owner_col], None)
    except:
        print(u"-Error: Can't read owner of plant with name {0}".format(name))
        owner = pw.NO_DATA_UNICODE

    try:
        primary_fuel = pw.standardize_fuel(row[fuel_col], fuel_thesaurus, as_set=False)
    except:
        print(u"-Error: Can't read fuel for plant with name {0}".format(name))
        primary_fuel = pw.NO_DATA_UNICODE

    try:
        latitude = float(row[latitude_col])
        longitude = float(row[longitude_col])
        geolocation_source = SOURCE_NAME_1
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
        plant_primary_fuel=primary_fuel, plant_capacity=capacity,
        plant_source=SOURCE_NAME_1, plant_source_url=SOURCE_URL_2)
    plants_dictionary[idnr] = new_plant

# 3: read in conventional plants under 100MW from Fusion Table data
COLNAMES = ["Power Plant ID", "Name", "Fuel", "Capacity (MW)", "Location", "Plant type", "Commissioning Date",
                "Units", "Owner", "Annual Generation (GWh)", "Source", "URL", "Country", "Latitude",
                "Longitude", "Geolocation Source"]

with open(FUSION_TABLE_FILE,'rU') as f:
    datareader = csv.reader(f)
    headers = datareader.next()
    id_col = headers.index(COLNAMES[0])
    name_col = headers.index(COLNAMES[1])
    fuel_col = headers.index(COLNAMES[2])
    capacity_col = headers.index(COLNAMES[3])
    location_col = headers.index(COLNAMES[4])
    commissioning_year_col = headers.index(COLNAMES[6])
    owner_col = headers.index(COLNAMES[8])
    generation_col = headers.index(COLNAMES[9])
    source_col = headers.index(COLNAMES[10])
    url_col = headers.index(COLNAMES[11])
    country_col = headers.index(COLNAMES[12])
    latitude_col = headers.index(COLNAMES[13])
    longitude_col = headers.index(COLNAMES[14])
    geolocation_source_col = headers.index(COLNAMES[15])

    # read each row in the file
    for row in datareader:
        try:
            name = pw.format_string(row[name_col])
            if not name:  # ignore accidental blank lines
               continue
        except:
            print(u"-Error: Can't read plant name.")
            continue  # must have plant name - don't read plant if not
        try:
            idnr = int(row[id_col])  # no chance of overlap - these start at 1,000,000
            if not idnr:  # must have plant ID - don't read plant if not
                print(u"-Error: Null ID for plant {0}.".format(name))
                continue
        except:
            print(u"-Error: Can't read ID for plant {0}.".format(name))
            continue  # must have plant ID - don't read plant if not
        try:
            capacity = float(pw.format_string(row[capacity_col].replace(",", "")))  # note: may need to convert to MW
        except:
            print(u"-Error: Can't read capacity for plant {0}; value: {1}".format(name, row[capacity_col]))
        try:
            primary_fuel = pw.standardize_fuel(row[fuel_col], fuel_thesaurus, as_set = False)
        except:
            print(u"-Error: Can't read fuel type for plant {0}.".format(name))
            primary_fuel = pw.NO_DATA_UNICODE
        try:
            latitude = float(row[latitude_col])
            longitude = float(row[longitude_col])
        except:
            latitude, longitude = pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC
        try:
            location = pw.format_string(row[location_col])
        except:
            location = pw.NO_DATA_UNICODE
        try:
            gen_gwh = float(pw.format_string(row[generation_col].replace(",", "")))
            generation = pw.PlantGenerationObject(gen_gwh)
        except:
            generation = pw.NO_DATA_OTHER
        try:
            owner = pw.format_string(row[owner_col])
        except:
            owner = pw.NO_DATA_UNICODE
        try:
            source = pw.format_string(row[source_col])
            if source == u"Open Government Portal":  # avoid duplication (can remove after updating FT)
                continue
        except:
            print(u"-Error: Can't read source for plant {0}.".format(name))
            source = pw.NO_DATA_UNICODE
        try:
            url = pw.format_string(row[url_col])
        except:
            print(u"-Error: Can't read URL for plant {0}.".format(name))
            url = pw.NO_DATA_UNICODE
        try:
            commissioning_year_string = row[commissioning_year_col].replace('"', '')
            if not commissioning_year_string:
                commissioning_year = pw.NO_DATA_NUMERIC
            elif (u"-" in commissioning_year_string) or (u"-" in commissioning_year_string) or (u"," in commissioning_year_string):  # different hyphen characters?
                commissioning_year_1 = float(commissioning_year_string[0:3])
                commissioning_year_2 = float(commissioning_year_string[-4:-1])
                commissioning_year = 0.5 * (commissioning_year_1 + commissioning_year_2)  # todo: need a better method
            else:
                commissioning_year = float(commissioning_year_string)
            if (commissioning_year < 1900) or (commissioning_year > 2020):  # sanity check 
                commissioning_year = pw.NO_DATA_NUMERIC
        except:
            print(u"-Error: Can't read commissioning year for plant {0} {1}.".format(country, str(idnr)))
            commissioning_year = pw.NO_DATA_NUMERIC

        new_location = pw.LocationObject(location, latitude, longitude)

        try:
            geolocation_source_string = row[geolocation_source_col]
        except:
            geolocation_source_string = pw.NO_DATA_UNICODE

        # add plant to database
        idnr_full = pw.make_id(SAVE_CODE, idnr)
        new_location = pw.LocationObject(location, latitude, longitude)
        new_plant = pw.PowerPlant(plant_idnr=idnr_full, plant_name=name, plant_country=COUNTRY_NAME,
                    plant_location=new_location, plant_coord_source=geolocation_source_string,
                    plant_primary_fuel=primary_fuel, plant_capacity=capacity,
                    plant_owner=owner, plant_generation=generation,
                    plant_source=source, plant_source_url=url,
                    plant_commissioning_year=commissioning_year)
        plants_dictionary[idnr_full] = new_plant

# report on plants read from file
print(u"Loaded {0} plants to database.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
