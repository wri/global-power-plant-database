# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
assemble_coordinates.py
Use plant matches from master concordance table to extract coordinates from GEO and CARMA.
Output table of all plants matched to GEO or CARMA and their coordinates from those sources.
"""

import argparse
import csv
import sys, os

sys.path.insert(0, os.path.join(os.pardir,os.pardir))
import powerplant_database as pw

# params
OUTPUT_FILE = "assembled_coordinates.csv"
GEO_DATABASE_FILE = pw.make_file_path(fileType = "src_bin", filename = "GEODB-Database.bin")
CARMA_DATABASE_FILE = pw.make_file_path(fileType = "src_bin", filename = "CARMA-Database.bin")

# make concordance dictionary
plant_concordance = pw.make_plant_concordance()
print("Loaded concordance file with {0} entries.".format(len(plant_concordance)))

# create dictionary for power plants and coordinate information
plants_dictionary = {}

# load GEO and CARMA for matching coordinates
geo_database = pw.load_database(GEO_DATABASE_FILE)
print("Loaded {0} plants from GEO database.".format(len(geo_database)))
carma_database = pw.load_database(CARMA_DATABASE_FILE)
print("Loaded {0} plants from CARMA database.".format(len(carma_database)))

# create list of key outcomes
correct_geo_keys = []
incorrect_geo_keys = []
correct_carma_keys = []
incorrect_carma_keys = []

# iterate through concordance matches to find coordinates
for plant,matches in plant_concordance.iteritems():
    if matches['geo_id']:
        try:
            loc = geo_database[matches['geo_id']].location
            plants_dictionary[plant] = {'source':'GEO','latitude':loc.latitude,'longitude':loc.longitude}
            correct_geo_keys.append(matches['geo_id'])
        except:
            incorrect_geo_keys.append(matches['geo_id'])
            print(u"Plant {0}: bad GEO key {1}".format(plant,matches['geo_id']))
    elif matches['carma_id']:
        try:
            loc = carma_database[matches['carma_id']].location
            plants_dictionary[plant] = {'source':'CARMA','latitude':loc.latitude,'longitude':loc.longitude}
            correct_carma_keys.append(matches['carma_id'])
        except:
            incorrect_carma_keys.append(matches['carma_id'])
    else:
        print(u"Error: No match for plant {0}".format(plant))

# report on key errors
print(u"GEO: Found {0} matches with good key; {1} with bad key.".format(len(correct_geo_keys),len(incorrect_geo_keys)))
print(u"CARMA: Found {0} matches with good key; {1} with bad key.".format(len(correct_carma_keys),len(incorrect_carma_keys)))

#print(u"Bad keys in GEO:")
#for k,v in incorrect_geo_keys.iteritems():
#    print(u"{0}: {1}".format(k,v))

# write out csv file
with open(OUTPUT_FILE,'w') as f:
    f.write('wri_id,coord_source,latitude,longitude\n')
    for p,m in plants_dictionary.iteritems():
        f.write('{0},{1},{2},{3}\n'.format(p,m['source'],m['latitude'],m['longitude']))

print('Finished.')
