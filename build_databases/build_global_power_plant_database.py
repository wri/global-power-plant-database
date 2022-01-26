# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_global_power_plant_database.py
Builds the Global Power Plant Database from various data sources.
- Log build to DATABASE_BUILD_LOG_FILE
- Use country and fuel information as specified in powerplant_database.py
- Use matches/concordances as specified in powerplants_database.py

TO-DOS:
- Alias list for power plants
"""

import csv
import time
import argparse
import sys, os

sys.path.insert(0, os.pardir)
import powerplant_database as pw

### PARAMETERS ###
COUNTRY_DATABASE_FILE = pw.make_file_path(fileType="src_bin", filename="COUNTRY-Database.bin")
WRI_DATABASE_FILE = pw.make_file_path(fileType="src_bin", filename="WRI-Database.bin")
GEO_DATABASE_FILE = pw.make_file_path(fileType="src_bin", filename="GEODB-Database.bin")
CARMA_DATABASE_FILE = pw.make_file_path(fileType="src_bin", filename="CARMA-Database.bin")
DATABASE_CSV_SAVEFILE = pw.make_file_path(fileType="output", filename="global_power_plant_database.csv")
DATABASE_BUILD_LOG_FILE = pw.make_file_path(fileType="output", filename="database_build_log.txt")
DATABASE_CSV_DUMPFILE = pw.make_file_path(fileType="output", filename="global_power_plant_database_data_dump.csv")
MINIMUM_CAPACITY_MW = 1

parser = argparse.ArgumentParser()
parser.add_argument("--dump", help="dump all the data", action="store_true")
DATA_DUMP = True if parser.parse_args().dump else False

# open log file
f_log = open(DATABASE_BUILD_LOG_FILE, 'a')
f_log.write('Starting Global Power Plant Database build run at {0}.\n'.format(time.ctime()))

# print summary
print("Starting Global Power Plant Database build; minimum plant size: {0} MW.".format(MINIMUM_CAPACITY_MW))

# make country dictionary
country_dictionary = pw.make_country_dictionary()

# make powerplants dictionary
core_database = {}
datadump = {}

# make plant condcordance dictionary
plant_concordance = pw.make_plant_concordance()
print("Loaded concordance file with {0} entries.".format(len(plant_concordance)))
carma_id_used = []	# Record matched carma_ids

# STEP 0: Read in source databases.
# Identify countries with automated data from .automated flag.
print("Loading source databases...")
country_databases = {}
for country_name, country in country_dictionary.iteritems():
	if country.automated == 1:
		country_code = country.iso_code
		database_filename = COUNTRY_DATABASE_FILE.replace("COUNTRY", country_code)
		country_databases[country_name] = pw.load_database(database_filename)
		print("Loaded {0} plants from {1} database.".format(len(country_databases[country_name]), country_name))

# Load multi-country databases.
wri_database = pw.load_database(WRI_DATABASE_FILE)
print("Loaded {0} plants from WRI database.".format(len(wri_database)))
geo_database = pw.load_database(GEO_DATABASE_FILE)
print("Loaded {0} plants from GEO database.".format(len(geo_database)))
carma_database = pw.load_database(CARMA_DATABASE_FILE)
print("Loaded {0} plants from CARMA database.".format(len(carma_database)))

# Track counts using a dict with keys corresponding to each data source
db_sources = country_databases.keys()
db_sources.extend(["WRI","GEO","WRI with GEO lat/long data","WRI with CARMA lat/long data"])
database_additions = {dbname: {'count': 0, 'capacity': 0} for dbname in db_sources}

# STEP 1: Add all data (capacity >= 1MW) from countries with automated data to the Database
for country_name, database in country_databases.iteritems():
	country_code = country_dictionary[country_name].iso_code
	print("Adding plants from {0}.".format(country_dictionary[country_name].primary_name))
	for plant_id, plant in database.iteritems():
		datadump[plant_id] = plant
		if plant.capacity >= MINIMUM_CAPACITY_MW:
			if (plant.location.latitude and plant.location.longitude) and (plant.location.latitude != 0 and plant.location.longitude != 0):
				core_database[plant_id] = plant
				database_additions[country_name]['count'] += 1
				database_additions[country_name]['capacity'] += plant.capacity
			else:
				plant.idnr = plant_id + u",No"
		else:
			plant.idnr = plant_id + u",No"

# STEP 2: Go through WRI database and triage plants
print("Adding plants from WRI internal database.")
for plant_id, plant in wri_database.iteritems():
	# Cases to skip
	if not isinstance(plant, pw.PowerPlant):
		f_log.write('Error: plant {0} is not a PowerPlant object.\n'.format(plant_id))
		continue
	if plant.country not in country_dictionary.keys():
		f_log.write('Error: country {0} not recognized.\n'.format(plant.country))
		continue
	# Skip plants with data loaded from an automated script
	if country_dictionary[plant.country].automated:
		continue
	# Skip plants in countries where we will use GEO data
	if country_dictionary[plant.country].use_geo:
		continue
	# skip plants in countries where WRI-collected data is already handled in an automated script
	if country_dictionary[plant.country].wri_data_built_in:
		continue

	datadump[plant_id] = plant

	# Skip plants below minimum capacity cutoff
	if plant.capacity < MINIMUM_CAPACITY_MW:
		continue

	# STEP 2.1: If plant has lat/long information, add it to the Database
	if (plant.location.latitude and plant.location.longitude) and (plant.location.latitude != 0 and plant.location.longitude != 0):
		plant.idnr = plant_id
		#plant.coord_source = u"WRI data"
		core_database[plant_id] = plant
		database_additions['WRI']['count'] += 1
		database_additions['WRI']['capacity'] += plant.capacity
		continue

	# STEP 2.2: If plant is matched to GEODB, add to the Database using GEODB lat/long
	if plant_id in plant_concordance:
		matching_geo_id = plant_concordance[plant_id]['geo_id']
		if matching_geo_id:
			try:
				plant.location = geo_database[matching_geo_id].location
			except:
				f_log.write("Matching error: no GEO location for WRI plant {0}, GEO plant {1}\n".format(plant_id, matching_geo_id))
				continue
			if plant.location.latitude and plant.location.longitude:
				plant.idnr = plant_id
				plant.coord_source = u"GEODB"
				core_database[plant_id] = plant
				database_additions["WRI with GEO lat/long data"]['count'] += 1
				database_additions["WRI with GEO lat/long data"]['capacity'] += plant.capacity
				continue

	# STEP 2.3: If plant is matched to CARMA, add to the Database using CARMA lat/long
	if plant_id in plant_concordance:
		matching_carma_id = plant_concordance[plant_id]['carma_id']
		if matching_carma_id:
			try:
				plant.location = carma_database[matching_carma_id].location
			except:
				f_log.write("Matching error: no CARMA location for WRI plant {0}, CARMA plant {1}\n".format(plant_id,matching_carma_id))
				continue
			if plant.location.latitude and plant.location.longitude:
				plant.idnr = plant_id
				plant.coord_source = u"CARMA"
				core_database[plant_id] = plant
				carma_id_used.append(matching_carma_id)
				database_additions["WRI with CARMA lat/long data"]['count'] += 1
				database_additions["WRI with CARMA lat/long data"]['capacity'] += plant.capacity
				continue
	# Note: Would eventually like to refine CARMA locations - known to be inaccurate in some cases

# STEP 3: Go through GEO database and add plants from small countries
# Plants in this database only have numeric ID (no prefix) because of concordance matching
for plant_id,plant in geo_database.iteritems():
	# Catch errors if plants do not have a correct country assigned
	datadump[plant_id] = plant
	if plant.country not in country_dictionary.keys():
		print("Plant {0} has country {1} - not found.".format(plant_id,plant.country))
		continue
	if country_dictionary[plant.country].use_geo:
		if plant.capacity < 1:
			continue
		if (plant.location.latitude and plant.location.longitude) and (plant.location.latitude != 0 and plant.location.longitude != 0):
			#plant.coord_source = u"GEO"
			plant.idnr = plant_id
			try:
				database_additions['GEO']['capacity'] += plant.capacity
			except:
				f_log.write("Attribute Warning: GEO plant {0} does not have valid capacity information <{1}>\n".format(plant_id, plant.capacity))
			else:
				core_database[plant_id] = plant
				database_additions['GEO']['count'] += 1

# STEP 3.1: Append another multinational database
wiki_solar_file = pw.make_file_path(fileType="raw", subFolder="Wiki-Solar", filename="wiki-solar-plant-additions-2019.csv")
wiki_solar_exclusion = pw.make_file_path(fileType="resource", filename="wiki-solar-exclusion.csv")
country_lookup = {cc.iso_code: cc.primary_name for cc in country_dictionary.values()}
# FIXME: patch lookup with additional geographies relevant in the wikisolar dataset
country_lookup.update({
	# Bonaire, Sint Eustatius and Saba
	"BES": "Netherlands",
	# Cayman Islands
	"CYM": "United Kingdom",
	# Puerto Rico
	"PRI": "United States of America",
	# Reunion
	"REU": "France",
	# The Virgin Islands of the United States
	"VIR": "United States of America",
})
wiki_solar_skip = {
	'United States of America': (0, 0)
}
wiki_solar_whitelist = ['PRI']

wiki_solar_count = 0
_exclude_list = [row['id'] for row in csv.DictReader(open(wiki_solar_exclusion))]
with open(wiki_solar_file) as fin:
	wiki_solar = csv.DictReader(fin)
	for solar_plant in wiki_solar:
		if solar_plant['id'] in _exclude_list:
			continue
		country = country_lookup.get(solar_plant['country'], '')
		plant_idnr = 'WKS{0:07d}'.format(int(solar_plant['id']))
		plant_location = pw.LocationObject(latitude=float(solar_plant['lat']), longitude=float(solar_plant['lon']))
		plant = pw.PowerPlant(
				plant_idnr=plant_idnr,
				plant_name=solar_plant['name'],
				plant_country=country,
				plant_capacity=float(solar_plant['capacity']),
				plant_location=plant_location,
				plant_coord_source='Wiki-Solar',
				plant_source='Wiki-Solar',
				plant_source_url='https://www.wiki-solar.org',
				plant_primary_fuel = 'Solar'
		)
		if (country in wiki_solar_skip) and \
		(solar_plant["country"] not in wiki_solar_whitelist):
			_n, _capacity = wiki_solar_skip[country]
			wiki_solar_skip[country] = (_n + 1, _capacity + plant.capacity)
			continue
		core_database[plant_idnr] = plant
		wiki_solar_count += 1
print("Loaded {0} plants from Wiki-Solar database.".format(wiki_solar_count))
for _country, _vals in wiki_solar_skip.iteritems():
	if _vals[0] != 0:
		print("...skipped {0} plants ({1} MW) for {2}.".format(_vals[0], _vals[1], _country))


# STEP 3.9: Add in multinational generation datasets
COUNTRY_DATABASE_FILE = pw.make_file_path(fileType="src_bin", filename="COUNTRY-Database.bin")
JRC_OPEN_PERFORMANCE = pw.make_file_path('raw', 'JRC-PPDB-OPEN', 'JRC_OPEN_PERFORMANCE.csv')
JRC_OPEN_UNITS = pw.make_file_path('raw', 'JRC-PPDB-OPEN', 'JRC_OPEN_UNITS.csv')
JRC_OPEN_LINKAGES = pw.make_file_path('raw', 'JRC-PPDB-OPEN', 'JRC_OPEN_LINKAGES.csv')
JRC_OPEN_TEMPORAL = pw.make_file_path('raw', 'JRC-PPDB-OPEN', 'JRC_OPEN_TEMPORAL.csv')
JRC_BLACKLIST = set([
	# blacklist created looking at obviously-wrong matches based on country designation
	# eic_g,  # bad_wri_id
	'50WG00000001097W',  # 'BRA0030768'
	'48W000000SUTB-1P',  # 'USA0060878'
	'26WUCNTRLDSCND24',  # 'CAN0008429'
	'26WUCNTRLDSCND16',  # 'CAN0008429'
	'50WG000000019861',  # 'BRA0029858'
	'50WG000000019853',  # 'BRA0029858'
	'50WGI00000019875',  # 'BRA0029858'
	'48W000000ROOS-1P',  # 'USA0006202'
])

# {wri_id: [eic_g_1, eic_g_2, ...], ...}
gppd_ppdb_link = {}
with open(JRC_OPEN_LINKAGES) as fin:
	r = csv.DictReader(fin)
	for row in r:
		wri_id = row['WRI_id']
		gen_id = row['eic_g']
		if gen_id:  # some blank gen_ids, which currently don't have wri_id matches
			gppd_ppdb_link[wri_id] = gppd_ppdb_link.get(wri_id, []) + [gen_id]

# {yr: {eic_g: (gen, time_coverage), ...}, ...}
ppdb_generation = {str(yr): {} for yr in [2015, 2016, 2017, 2018]}
with open(JRC_OPEN_TEMPORAL) as fin:
	r = csv.DictReader(fin)
	skipped_generation = 0
	for row in r:
		year_data = ppdb_generation[row['cyear']]
		# value is in MWh according to `datapackage.json` in JRC-PPDB-OPEN
		year_data[row['eic_g']] = (row['Generation'], row['time_coverage'])

# desired lookup structure: {plant1: {year1: val, year2: val2, ...}, ...}
agg_gen_by_gppd = {}
# per-unit time availability
time_threshold = '0.950'  # yes this is a string
# WRI plants that aren't having the estimation applied [(plant1, yearA), ...]
jrc_skipped_plants = []
for wri_id, gen_ids in gppd_ppdb_link.items():
	plant_totals = {}
	for year in map(str, [2015, 2016, 2017]):
		year_data = ppdb_generation[year]
		year_gen_val = 0
		accepted_gen_ids = []
		for gen_id in gen_ids:
			gen, time_coverage = year_data.get(gen_id, (0, '0.000'))
			if time_coverage < time_threshold or gen_id in JRC_BLACKLIST:
				jrc_skipped_plants.append((wri_id, int(year)))
				break
			year_gen_val += float(gen)
			accepted_gen_ids.append(gen_id)
		if set(accepted_gen_ids) == set(gen_ids):
			# convert MWh to GWh and assign value for the year
			plant_totals[int(year)] = year_gen_val / 1000
	agg_gen_by_gppd[wri_id] = plant_totals

for pid, pp in core_database.items():
	if agg_gen_by_gppd.get(pid, {}):
		new_generation = []
		for yr, val in agg_gen_by_gppd[pid].items():
			gen = pw.PlantGenerationObject.create(val, year=yr, source='JRC-PPDB-OPEN')
			new_generation.append(gen)
		if new_generation:
			pp.generation = new_generation
#print("Added {0} plants ({1} MW) from {2}.".format(data['count'], data['capacity'], dbname))

# STEP 4: Estimate generation for plants without reported generation for target year
count_plants_with_generation = 0
#for plant_id,plant in core_database.iteritems():
#	if plant.generation != pw.NO_DATA_OTHER:
#		count_plants_with_generation += 1
#print('Of {0} total plants, {1} have reported generation data.'.format(len(core_database),count_plants_with_generation))
print('Estimating generation...')
estimated_plants = pw.estimate_generation(core_database)
print('...estimated for {0} plants.'.format(estimated_plants))

# STEP 4.1: Add WEPP ID matches
pw.add_wepp_id(core_database)
if DATA_DUMP:
	pw.add_wepp_id(datadump)

# STEP 5: Write the Global Power Plant Database
for dbname, data in database_additions.iteritems():
	print("Added {0} plants ({1} MW) from {2}.".format(data['count'], data['capacity'], dbname))

f_log.close()
print("Loaded {0} plants to the Global Power Plant Database.".format(len(core_database)))
pw.write_csv_file(core_database, DATABASE_CSV_SAVEFILE)
print("Global Power Plant Database built.")

# STEP 6: Dump Data
if DATA_DUMP:
	print("Dumping all the data...")
	# STEP 6.1: Label plants in datadump
	pw_idnrs = core_database.keys()
	for plant_id,plant in datadump.iteritems():
		if plant_id in pw_idnrs:
			plant.idnr = plant_id + ",Yes"
		else:
			plant.idnr = plant_id + ",No"

	# STEP 6.2: Add unused CARMA plants
	for plant_id,plant in carma_database.iteritems():
		plant.coord_source = u"CARMA data"
		if plant_id in carma_id_used:
			continue
		else:
			plant.idnr = plant_id + ",No"
			datadump[plant_id] = plant

	print("Dumped {0} plants.".format(len(datadump)))
	pw.write_csv_file(datadump, DATABASE_CSV_DUMPFILE,dump=True)
	print("Data dumped.")

print("Finished.")
