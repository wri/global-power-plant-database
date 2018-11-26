# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_IND.py
Get power plant data from India and convert to the Global Power Plant Database format.
Data Sources:
- Central Electricity Authority (for all conventional plants)
- WRI Fusion Table data (for all non-conventional plants)

Additional information: [URL]

Issues:
Additional possible sources:
- Capacity, generation and coal consumption data appear to be available via API here: https://data.gov.in/catalog/coal-statement-thermal-power-stations
- Solar data appear available here: https://data.gov.in/catalog/commissioned-grid-solar-power-projects
- Some data appear to be available here, with subscription: http://www.indiastat.com/power/26/generation/112/stats.aspx
- Outdated power system data are available here: http://www.cercind.org/powerdata.htm
- Annual generation by plant appears to be available here: http://cea.nic.in/reports/monthly/executivesummary/2016/exe_summary-08.pdf
- Might get wind data from CDM
- Might get CSP data from NREL

Further possible data sources:
- Ladakh Renewable Energy Development Agency (map, but all projects appear to be proposed, not completed): http://ladakhenergy.org/projects/map/
- There appear to be almost 30 other such agencies. See https://en.wikipedia.org/wiki/Ministry_of_New_and_Renewable_Energy#State_Nodal_Agencies
- An old (2011) list of grid-tied solar PV plants: http://mnre.gov.in/file-manager/UserFiles/powerplants_241111.pdf
- This appears to be a list of approved solar parks (although unclear if they're operational): http://www.seci.gov.in/content/innerpage/statewise-solar-parks.php
"""

import csv
import sys
import os
from zipfile import ZipFile
import lxml.html as LH
import xlrd

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"India"
SOURCE_NAME = u"Central Electricity Authority"
SOURCE_URL = u"http://www.cea.nic.in/"
SOURCE_URL2 = u"https://www.recregistryindia.nic.in/"
GEOLOCATION_SOURCE_CEA = u"WRI"
SAVE_CODE = u"IND"
RAW_FILE_NAME_CEA = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="database_13.zip")
RAW_FILE_NAME_CEA_UZ = pw.make_file_path(fileType="raw", filename=SAVE_CODE)
RAW_FILE_NAME_REC = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="accredited_rec_generators.html")
WRI_DATABASE = pw.make_file_path(fileType="src_bin", filename=u"WRI-Database.bin")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_IND.csv")
PLANT_LOCATIONS_FILE = pw.make_file_path(fileType="resource", subFolder="IND", filename="CEA_plants.csv")

SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
LOCATION_FILE = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE, filename="plant_locations_IND.csv")
TAB_NAME = u"Data"
DATA_YEAR = 2017  # capacity data from CEA

# optional raw files to download
FILES = {
    RAW_FILE_NAME_CEA: "http://www.cea.nic.in/reports/others/thermal/tpece/cdm_co2/database_13.zip",
    RAW_FILE_NAME_REC: "https://www.recregistryindia.nic.in/index.php/general/publics/accredited_regens"
}
DOWNLOAD_FILES = pw.download(u'CEA and RECS', FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

def get_CEA_generation(row, col, year, source_url):
    """Extract a generation data point from CEA data."""
    try:
        if row[col] == u'-':
            generation = pw.PlantGenerationObject()
        else:
            gen_gwh = float(row[col])
            generation = pw.PlantGenerationObject.create(gen_gwh, year, source=source_url)
    except:
        generation = pw.PlantGenerationObject()
    return generation

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

# load location information from static file
plant_locations = {}
with open(PLANT_LOCATIONS_FILE, 'rU') as f:
    reader = csv.DictReader(f)
    for row in reader:
        row['match_key'] = int(row['id_2016-2017'])
        try:
            row['latitude'] = float(row['latitude'])
            row['longitude'] = float(row['longitude'])
        except:
            pass
        if row['match_key'] in plant_locations:
            print(u"-Error: Duplicated ID for 2016-2017: {0}".format(row['match_key']))
        else:
            plant_locations[row['match_key']] = row
print("Read location coordinates of {0} CEA-listed plants...".format(len(plant_locations)))

# specify column names used in raw file
COLNAMES = {
    'serial_id': u"S_NO",
    'name': u"NAME",
    'unit': u"UNIT_NO",
    'year': u"DT_ COMM",
    'capacity': u"CAPACITY MW AS ON 31/03/2017",
    'type':    u"TYPE",
    'primary_fuel': u"FUEL 1",
    'other_fuel': u"FUEL 2",
    'gen_13-14': u"2013-14\n\nNet \nGeneration \nGWh",
    'gen_14-15': u"2014-15\n\nNet \nGeneration \nGWh",
    'gen_15-16': u"2015-16\n\nNet \nGeneration \nGWh",
    'gen_16-17': u"2016-17\n\nNet \nGeneration \nGWh",
}

# prepare list of units
unit_list = {}

# unzip CEA file
with ZipFile(RAW_FILE_NAME_CEA, 'r') as myzip:
    fn = myzip.namelist()[0]
    f = myzip.extract(fn, RAW_FILE_NAME_CEA_UZ)

# open excel file
book = xlrd.open_workbook(f)
sheet = book.sheet_by_name(TAB_NAME)

# get the column indices
rv = sheet.row_values(0)
serial_id_col = rv.index(COLNAMES['serial_id'])
name_col = rv.index(COLNAMES['name'])
unit_col = rv.index(COLNAMES['unit'])
year_col = rv.index(COLNAMES['year'])
capacity_col = rv.index(COLNAMES['capacity'])
type_col = rv.index(COLNAMES['type'])
primary_fuel_col = rv.index(COLNAMES['primary_fuel'])
other_fuel_col = rv.index(COLNAMES['other_fuel'])
gen_13_14_col = rv.index(COLNAMES['gen_13-14'])
gen_14_15_col = rv.index(COLNAMES['gen_14-15'])
gen_15_16_col = rv.index(COLNAMES['gen_15-16'])
gen_16_17_col = rv.index(COLNAMES['gen_16-17'])


# parse each row
for i in xrange(1, sheet.nrows):

    # read in row
    rv = sheet.row_values(i)

    try:
        name = pw.format_string(rv[name_col])
        if not name:
            continue        # don't read rows that lack a plant name (footnotes, etc)
    except:
        print(u"-Error: Can't read plant name for plant on row {0}.".format(i))
        continue

    try:
        serial_id_val = int(rv[serial_id_col])
        if not serial_id_val:
            continue        # don't read rows that lack an ID (footnotes, etc)
    except:
        print(u"-Error: Can't read ID for plant on row {0}.".format(i))
        continue

    try:
        capacity = float(rv[capacity_col])
    except:
        try:
            capacity = eval(rv[capacity_col])
        except:
            print("-Error: Can't read capacity for plant {0}".format(name))
            capacity = pw.NO_DATA_NUMERIC

    if not capacity:
        continue        # don't include zero-capacity plants

    # Unit "0" is used for the entire plant; other lines are individual units
    # If this line is a unit, just read its year/capacity for later averaging
    if rv[unit_col] == 0:
        unit_list[serial_id_val] = []
    else:
        date_number = rv[year_col]
        year = pw.excel_date_as_datetime(date_number).year
        unit_list[serial_id_val].append({'capacity': capacity, 'year': year})
        continue   # don't continue reading this line b/c it's not a full plant

    # try to load generation data
    # TODO: organize this into fiscal year (april through march)
    generation_13 = get_CEA_generation(rv, gen_13_14_col, 2013, SOURCE_URL)
    generation_14 = get_CEA_generation(rv, gen_14_15_col, 2014, SOURCE_URL)
    generation_15 = get_CEA_generation(rv, gen_15_16_col, 2015, SOURCE_URL)
    generation_16 = get_CEA_generation(rv, gen_16_17_col, 2016, SOURCE_URL)
    generation = [generation_13, generation_14, generation_15, generation_16]

    try:
        plant_type = pw.format_string(rv[type_col])
        if plant_type in [u"HYDRO", u"NUCLEAR"]:
            primary_fuel = pw.standardize_fuel(plant_type, fuel_thesaurus, as_set=False)
        elif plant_type == u"THERMAL":
            primary_fuel = pw.standardize_fuel(rv[primary_fuel_col], fuel_thesaurus, as_set=False)
            if rv[other_fuel_col] and rv[other_fuel_col] != 'n/a':
                other_fuel = pw.standardize_fuel(rv[other_fuel_col], fuel_thesaurus, as_set=True)
            else:
                other_fuel = pw.NO_DATA_SET
        else:
            print("Can't identify plant type {0}".format(plant_type))
    except:
        print(u"Can't identify plant type for plant {0}".format(name))

    # look up location
    if serial_id_val in plant_locations:
        latitude = plant_locations[serial_id_val]["latitude"]
        longitude = plant_locations[serial_id_val]["longitude"]
        geolocation_source = GEOLOCATION_SOURCE_CEA
    else:
        print("-Error: Can't find CEA ID {0} in plant location file.".format(serial_id_val))
        latitude = pw.NO_DATA_NUMERIC
        longitude = pw.NO_DATA_NUMERIC
        geolocation_source = pw.NO_DATA_UNICODE

    # assign ID number from CEA locations file; 
    # maintains IDs generated from previous CEA files
    idnr = plant_locations[serial_id_val]["gppd_id"]

    new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
    new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=COUNTRY_NAME,
        plant_location=new_location, plant_coord_source=geolocation_source,
        plant_primary_fuel=primary_fuel, plant_other_fuel=other_fuel,
        plant_capacity=capacity, plant_cap_year=DATA_YEAR,
        plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL,
        plant_generation=generation)
    plants_dictionary[idnr] = new_plant

# now find average commissioning year weighted by capacity
for serial_id_val, units in unit_list.iteritems():

    # get plant from dictionary 
    plant_id = plant_locations[serial_id_val]["gppd_id"]
    plant = plants_dictionary[plant_id]
    if plant.capacity == 0:
        print(u"Warning: Plant {0} has zero capacity.".format(plant_id))
        # just use average of years
        unit_year_sum = sum(map(lambda x: x['year'], units))
        plant.commissioning_year = int(unit_year_sum / len(units))
        continue

    # find capacity-weighted average commissioning year
    weighted_year= sum(map(lambda x: x['capacity'] * x['year'], units))
    total_capacity = sum(map(lambda x: x['capacity'], units))
    commissioning_year = weighted_year / total_capacity
    plant.commissioning_year = int(commissioning_year)

    # sanity checks
    if commissioning_year < 1920 or commissioning_year > DATA_YEAR:
        print(u'Commissioning year of {0} is {1}'.format(plant.name, commissioning_year))

    if plant.capacity:
        capacity_ratio_check = total_capacity / plant.capacity
        if capacity_ratio_check < 0.999 or capacity_ratio_check > 1.001:

            print(u'-Error: Plant {0} total capacity ({1}) does not match unit capacity sum ({2}).'.format(plant.name, total_capacity, plant.capacity))

# now add plants from WRI Fusion Tables (non-conventional/not included in CEA data)

# read in additional data from Fusion Table file
print("Adding additional plants from WRI manually gathered data...")
wri_database = pw.load_database(WRI_DATABASE)
plants_dictionary.update({k: v for k, v in wri_database.iteritems() if v.country == 'India'})
print("...finished.")

# load and process RECS file - NOT IMPLEMENTED
#tree = LH.parse(RAW_FILE_NAME_CEA)
#print([td.text_content() for td in tree.xpath('//td')])

#ns = {"kml":"http://www.opengis.net/kml/2.2"}   # namespace
#parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
#tree = etree.parse(RAW_FILE_NAME_REC, parser)
#rows = iter(table)
#for row in rows:
#    print row

#    root = tree.getroot()
#    for child in root[0]:
#        if u"Folder" in child.tag:

# report on plants read from file
print(u"Loaded {0} plants to database.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
