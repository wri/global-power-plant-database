# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_WRI.py
Convert manually-collected tabular data to the Global Power Plant Database format.
Data Source: World Resources Institute (manually assembled from multiple sources).
Additional information: https://github.com/wri/global-power-plant-database
"""

import argparse
import csv
import sys, os
import re

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"GLOBAL"
SOURCE_NAME = u"WRI"
SAVE_CODE = u"WRI"
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_WRI.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
RAW_FILE_DIRECTORY = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE)
OVERLAP_FILE = "Power_Plant_ID_overlaps.csv"


# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

# track IDs that are assigned to plants in two different countries (likely an error)
overlapping_ids = {}
countries_with_zero_plants = []
geolocation_sources_mw = {"Located, no source": 0.0, "Not located": 0.0}

for afile in os.listdir(RAW_FILE_DIRECTORY):
    if afile.endswith(".csv"):
        country = afile.replace(".csv", "")
        country_plant_count = 0

        # capacities by plant, by fuel
        plant_fuel_capacities = {}

        with open(os.path.join(RAW_FILE_DIRECTORY, afile), 'rU') as f:
            datareader = csv.DictReader(f)
            try:
                id_col = "Power Plant ID"
                name_col = "Name"
                primary_fuel_col = "Fuel"
                other_fuel_col = "Secondary Fuel"
                capacity_col = "Capacity (MW)"
                location_col = "Location"
                status_col = "Operational Status"
                commissioning_year_col = "Commissioning Date"
                owner_col = "Owner"
                generation_col = "Annual Generation (GWh)"
                generation_source_col = "Generation Data Source"
                source_col = "Source"
                url_col = "URL"
                country_col = "Country"
                latitude_col = "Latitude"
                longitude_col = "Longitude"
                geolocation_source_col = "Geolocation Source"
                year_of_data_col = "Year of Data"
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
                # primary fuel
                try:
                    primary_fuel = pw.standardize_fuel(row[primary_fuel_col], fuel_thesaurus, as_set=False)
                except:
                    print(u"-Error: Can't read fuel type for plant {0}.".format(name))
                    primary_fuel = pw.NO_DATA_UNICODE
                # other fuels
                try:
                    if row[other_fuel_col]:
                        other_fuel = pw.standardize_fuel(row[other_fuel_col], fuel_thesaurus, as_set=True)
                    else:
                        other_fuel = pw.NO_DATA_SET.copy()
                except:
                    print(u"-Error: Can't read secondary fuel type for plant {0}.".format(name))
                    other_fuel = pw.NO_DATA_SET.copy()
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
                    gen_source = pw.format_string(row[generation_source_col])
                except:
                    generation = pw.NO_DATA_OTHER
                else:
                    generation = pw.PlantGenerationObject.create(gen_gwh, year=gen_year, source=gen_source)
                try:
                    owner = pw.format_string(row[owner_col])
                    # remove percentage ownership in owner name for denmark plants
                    if country == "Denmark":
                        owner = re.sub("[0-9]*(\%)+ ", "", owner)
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
                    geolocation_source_string = row[geolocation_source_col].decode(pw.UNICODE_ENCODING)
                    if geolocation_source_string.startswith(u'WRI'):  # map 'WRI [Name]' to 'WRI'
                        geolocation_source_string = u'WRI'
                except:
                    geolocation_source_string = pw.NO_DATA_UNICODE
                    print(u"-Error: Can't read geolocation source for plant {0} {1}".format(country, str(idnr)))

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


                # store the capacity by fuel for this plant
                fuel_capacities = plant_fuel_capacities.get(idnr_full, {})
                fuel_capacity = fuel_capacities.get(primary_fuel, 0)
                if capacity:
                    fuel_capacity += capacity
                for fuel_type in other_fuel:
                    if fuel_type not in fuel_capacities:
                        fuel_capacities[fuel_type] = 0
                fuel_capacities[primary_fuel] = fuel_capacity
                plant_fuel_capacities[idnr_full] = fuel_capacities

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
                        plant_capacity=capacity,
                        plant_owner=owner, plant_generation=generation,
                        plant_source=source, plant_source_url=url,
                        plant_commissioning_year=commissioning_year)
                    plants_dictionary[idnr_full] = new_plant
                    country_plant_count += 1

            # figure out primary and other fuels once all units/plants have been read in
            for gppd_idnr, fuel_capacities in plant_fuel_capacities.iteritems():
                # get primary, which is the fuel with the highest capacity
                try:
                    primary_fuel = max(fuel_capacities, key=lambda x: fuel_capacities[x])
                except ValueError:  # tried max of empty sequence - plants w/o operable units
                    primary_fuel = pw.NO_DATA_UNICODE
                    print('-ERROR: could not determine primary fuel for plant <{0}>'.format(gppd_idnr))
                # set primary
                plants_dictionary[gppd_idnr].primary_fuel = primary_fuel
                # make a set out of all fuels encountered for this plant
                other_fuels = set(fuel_capacities)
                # remove the already-named primary fuel
                try:
                    other_fuels.remove(primary_fuel)
                except:
                    pass
                else:
                    plants_dictionary[gppd_idnr].other_fuel = other_fuels

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

# report on files with zero plants read
if len(countries_with_zero_plants) > 0:
    print(u"ERROR: The following files yielded no plants for the database:")
    for fn in countries_with_zero_plants:
        print(fn)

# report on geolocation sources
print(u"Geolocation sources:")
for source, capacity in sorted(geolocation_sources_mw.items(), key=lambda (k,v):(v,k), reverse=True):
    print(u" - {:12,.1f} MW: {:20}".format(capacity,source))

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
