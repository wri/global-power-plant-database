# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_WRI.py
Download power plant data from WRI Fusion Tables and convert to the Global Power Plant Database format.
Data Source: World Resources Institute (manually assembled from multiple sources).
Additional information: https://github.com/wri/global-power-plant-database
Issues: Requires an API key to retrieve data from Fusion Tables.
"""

import argparse
import csv
import sys, os

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"GLOBAL"
SOURCE_NAME = u"WRI"
SAVE_CODE = u"WRI"
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_WRI.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
RAW_FILE_DIRECTORY = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE)
API_KEY_FILE = pw.make_file_path(fileType="resource", subFolder="api_keys", filename="fusion_tables_api_key.txt")
OVERLAP_FILE = "Power_Plant_ID_overlaps.csv"

# other parameters as needed
URL_BASE = "https://www.googleapis.com/fusiontables/v2/query?alt=csv&sql=SELECT * FROM "

# set up country dictionary (need this for fusion table keys)
country_dictionary = pw.make_country_dictionary()

if '--download' in sys.argv:
    # get API key
    with open(API_KEY_FILE, 'r') as f:
        API_KEY = f.readline().rstrip()
    FILES = {}
    for country_name, country_info in country_dictionary.iteritems():
        if country_info.fusion_table_id:
            RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename=country_name + ".csv")
            URL = URL_BASE + country_info.fusion_table_id + "&key=" + API_KEY
            FILES[RAW_FILE_NAME] = URL
    # optional raw file download
    DOWNLOAD_FILES = pw.download(SOURCE_NAME, FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

# specify column names used in raw file
COLNAMES = ["Power Plant ID", "Name", "Fuel", "Secondary Fuel", "Capacity (MW)",
			"Location", "Operational Status", "Commissioning Date",
			"Units", "Owner", "Annual Generation (GWh)", "Source", "URL", "Country",
			"Latitude", "Longitude", "Geolocation Source", "Year of Data"]

# track IDs that are assigned to plants in two different countries (likely an error)
overlapping_ids = {}
countries_with_zero_plants = []
geolocation_sources_mw = {"Located, no source": 0.0, "Not located": 0.0}

for afile in os.listdir(RAW_FILE_DIRECTORY):
    if afile.endswith(".csv"):
        country = afile.replace(".csv", "")
        country_plant_count = 0

        with open(os.path.join(RAW_FILE_DIRECTORY, afile), 'rU') as f:
            datareader = csv.reader(f)
            headers = datareader.next()
            try:
                id_col = headers.index(COLNAMES[0])
                name_col = headers.index(COLNAMES[1])
                primary_fuel_col = headers.index(COLNAMES[2])
                other_fuel_col = headers.index(COLNAMES[3])
                capacity_col = headers.index(COLNAMES[4])
                location_col = headers.index(COLNAMES[5])
                status_col = headers.index(COLNAMES[6])
                commissioning_year_col = headers.index(COLNAMES[7])
                owner_col = headers.index(COLNAMES[9])
                generation_col = headers.index(COLNAMES[10])
                source_col = headers.index(COLNAMES[11])
                url_col = headers.index(COLNAMES[12])
                country_col = headers.index(COLNAMES[13])
                latitude_col = headers.index(COLNAMES[14])
                longitude_col = headers.index(COLNAMES[15])
                geolocation_source_col = headers.index(COLNAMES[16])
                year_of_data_col = headers.index(COLNAMES[17])
            except:
                print(u"- ERROR: One or more columns missing in {0}, skipping...".format(afile))
                continue

            # read each row in the file
            for row in datareader:
                # skip plants that aren't operational
                status = row[status_col]
                if status not in ['Operational', 'Operating', '']:
                    continue
                try:
                    name = pw.format_string(row[name_col])
                    if not name:  # ignore accidental blank lines
                        continue
                except:
                    print(u"-Error: Can't read plant name.")
                    continue  # must have plant name - don't read plant if not
                try:
                    idnr = str(row[id_col])
                    if not idnr:  # must have plant ID - don't read plant if not
                        print(u"-Error: Null ID for plant {0}.".format(name))
                        continue
                except:
                    print(u"-Error: Can't read ID for plant {0}.".format(name))
                    continue  # must have plant ID - don't read plant if not
                try:
                    capacity = float(pw.format_string(row[capacity_col].replace(",", "")))   # note: may need to convert to MW
                except:
                    print(u"-Error: Can't read capacity for plant {0}; value: {1}".format(name, row[capacity_col]))
                    capacity = pw.NO_DATA_NUMERIC
                try:
                    primary_fuel = pw.standardize_fuel(row[primary_fuel_col], fuel_thesaurus, as_set=False)
                except:
                    print(u"-Error: Can't read primary fuel type for plant {0}.".format(name))
                    primary_fuel = pw.NO_DATA_UNICODE
                try:
                    if row[other_fuel_col]:
                        other_fuel = pw.standardize_fuel(row[other_fuel_col], fuel_thesaurus, as_set=True)
                    else:
                        other_fuel = pw.NO_DATA_SET
                except:
                    print(u"-Error: Can't read secondary fuel type for plant {0}.".format(name))
                    other_fuel = pw.NO_DATA_SET
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
                    gen_year = int(pw.format_string(row[year_of_data_col]))
                    generation = pw.PlantGenerationObject.create(gen_gwh, year=gen_year)
                except:
                    generation = pw.NO_DATA_OTHER
                try:
                    owner = pw.format_string(row[owner_col])
                except:
                    owner = pw.NO_DATA_UNICODE
                try:
                    source = pw.format_string(row[source_col])
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

                try:
                    geolocation_source_string = row[geolocation_source_col]
                    if geolocation_source_string.startswith(u'WRI'):  # map 'WRI [Name]' to 'WRI'
                        geolocation_source_string = u'WRI'
                except:
                    geolocation_source_string = pw.NO_DATA_UNICODE

                # track geolocation source
                if (not latitude) or (not longitude):
					try:
						geolocation_sources_mw[u"Not located"] += capacity
					except:
						print(u" - Warning: plant {0} has no capacity".format(idnr))
                elif geolocation_source_string:
                    try:
                        cap = float(capacity)
                    except:
                        cap = 0
                    if geolocation_source_string in geolocation_sources_mw:
                        geolocation_sources_mw[geolocation_source_string] += cap
                    else:
                        geolocation_sources_mw[geolocation_source_string] = cap
                else:
                    try:
                        geolocation_sources_mw[u"Located, no source"] += capacity
                    except:
						print(u" - Warning: plant {0} has no capacity".format(idnr))

                # assign ID number
                # have to use a hack for United Kingdom/GBR because we previously used an automated script
                # original ID codes for GBR plants start with GBR, not WRI
                if country == "United Kingdom":
                    idnr_full = pw.make_id("GBR", int(idnr))
                else:
                    idnr_full = pw.make_id(SAVE_CODE, int(idnr))

                # check if this ID is already in the dictionary - if so, this is a unit
                if idnr_full in plants_dictionary:

                    # first check this isn't an ID overlap
                    country2 = plants_dictionary[idnr_full].country
                    if country != country2:
                        if idnr_full not in overlapping_ids.keys():
                            overlapping_ids[idnr_full] = {'country1': country, 'country2': country2}
                        # don't treat this as a unit
                        continue

                    # update plant
                    existing_plant = plants_dictionary[idnr_full]
                    existing_plant.capacity += capacity
                    existing_plant.other_fuel.update(other_fuel)
                    # append generation object - may want to sum generation instead?
                    if generation:
                        if not isinstance(existing_plant.generation, list):
                            existing_plant.generation = [pw.PlantGenerationObject(),]
                        existing_plant.generation.append(generation)
                    # if lat/long for this unit, overwrite previous data - may want to change this
                    if latitude and longitude:
                        new_location = pw.LocationObject(location, latitude, longitude)
                        existing_plant.location = new_location

                    # unclear how to handle owner, source, url, commissioning year

                else:
                    new_location = pw.LocationObject(location, latitude, longitude)
                    new_plant = pw.PowerPlant(plant_idnr=idnr_full, plant_name=name, plant_country=country,
                        plant_location=new_location, plant_coord_source=geolocation_source_string,
                        plant_primary_fuel=primary_fuel, plant_other_fuel=other_fuel,
                        plant_capacity=capacity,
                        plant_owner=owner, plant_generation=generation,
                        plant_source=source, plant_source_url=url,
                        plant_commissioning_year=commissioning_year)
                    plants_dictionary[idnr_full] = new_plant
                    country_plant_count += 1

        print("Read {:4d} plants from file {:}.".format(country_plant_count, afile))
        if country_plant_count == 0:
            countries_with_zero_plants.append(afile)

# report on overlapping IDs
if len(overlapping_ids) > 0:
    print(u"ERROR: ID overlaps for {0} plants in different countries; written to {1}.".format(len(overlapping_ids), OVERLAP_FILE))
    with open(OVERLAP_FILE, 'w') as f:
        f.write('idnr,country1,country2\n')
        for idnr, overlap in overlapping_ids.iteritems():
            f.write('{0},{1},{2}\n'.format(idnr, overlap['country1'], overlap['country2']))

# report on fusion table files with zero plants read
if len(countries_with_zero_plants) > 0:
    print(u"ERROR: The following files yielded no plants for the database:")
    for fn in countries_with_zero_plants:
        print(fn)

# report on geolocation sources
print(u"Geolocation sources:")
for source, capacity in sorted(geolocation_sources_mw.iteritems(),key=lambda (k,v):(v,k), reverse=True):
    print(u" - {:12,.1f} MW: {:20}".format(capacity,source))

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
