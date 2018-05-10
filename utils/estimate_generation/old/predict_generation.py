# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
predict_generation.py
Use a trained Gradient Boosted Regression Tree (GBRT) model to estimate/predict power plant electricity 
generation.
"""

import csv
import numpy as np
import matplotlib.pyplot as plt
import pickle
from sklearn.ensemble import GradientBoostingRegressor
import argparse

import sys, os 
sys.path.insert(0, os.path.join(os.pardir,os.pardir))
import powerplant_database as pw

# parse args
parser = argparse.ArgumentParser()
parser.add_argument("powerplant_database", help = "powerplant database (CSV)")
parser.add_argument("model_filename", help = "pickle file of trained model")
parser.add_argument("country_generation", help = "generation by country by fuel")
args = parser.parse_args()

# params
CSV_SAVEFILE = 'plants_with_estimated_generation.csv'
CF_CONVERSION_FACTOR = 1 / float( 0.001 * 24 * 365 )

# make country names thesaurus
country_names_thesaurus = pw.make_country_names_thesaurus()

# read country-level generation file
generation_by_country_by_fuel = {}
with open(args.country_generation,'r') as f:
	datareader = csv.reader(f)
	header = datareader.next()
	for row in datareader:
		country_name_iea = row[0]
		# translate name to standard version
		for country_name,other_names in country_names_thesaurus.iteritems():
			if country_name_iea in other_names:
				standard_country_name = country_name
				continue
		if standard_country_name not in generation_by_country_by_fuel.keys():
			generation_by_country_by_fuel[standard_country_name] = {}
		fuel_type = row[1]
		if fuel_type not in generation_by_country_by_fuel[standard_country_name].keys():
			generation_by_country_by_fuel[standard_country_name][fuel_type] = float(row[2])
		else:
			print("Error with generation file.")

# load estimation model
with open(args.model_filename,'r') as f:
	model_data = pickle.load(f)
est = model_data['model']
params = model_data['params']
num_folds = model_data['num_folds']
fuel_types = model_data['fuel_types']
print("Loaded trained generation estimation model from {0}.".format(args.model_filename))
for k,v in params.iteritems():
	print(" - {0}: {1}".format(k,v))
print(" - num_folds: {0}".format(num_folds))
print("Fuel types: {0}".format(fuel_types))

# load powerplant database
if args.powerplant_database.endswith('.csv'):
	plants = pw.read_csv_file_to_dict(args.powerplant_database)
else:
	plants = pw.load_database(args.powerplant_database)

print("Loaded {0} plants from file {1}.".format(len(plants),args.powerplant_database))

# set up arrays
fuel_type_list = []
capacity_by_country_by_fuel = {}				# will hold total capacity by country and fuel
feature_name_list = ['fuel_type','capacity_mw','commissioning_year','fuel_avg_cf','cap_sh_country','cap_sh_country_fuel']

# read data from plant database
count_full_data = 0
count_partial_data = 0
plants_for_generation_estimation = {}
for plant_id,plant in plants.iteritems():

	# deal with country
	if plant.country not in capacity_by_country_by_fuel.keys():
		capacity_by_country_by_fuel[plant.country] = {}

	# get fuel type and capacity
	fuel_type = next(iter(plant.fuel))   # TODO: deal with multi-fuel plants better
	if fuel_type not in fuel_type_list:
		fuel_type_list.append(fuel_type)
	fuel_index = fuel_type_list.index(fuel_type)

	if fuel_type in capacity_by_country_by_fuel[plant.country].keys():
		capacity_by_country_by_fuel[plant.country][fuel_type] += plant.capacity
	else:
		capacity_by_country_by_fuel[plant.country][fuel_type] = plant.capacity 

	# try to get commissioning year
	if plant.commissioning_year != pw.NO_DATA_NUMERIC:
		count_full_data += 1
	else:
		count_partial_data += 1
		continue

	plants_for_generation_estimation[plant_id] = [plant.country,fuel_index,plant.capacity,plant.commissioning_year]

print("Full data: {0}; partial data: {1}".format(count_full_data,count_partial_data))
print(capacity_by_country_by_fuel)

# calculate average capacity factor by fuel for each country
average_capacity_factors = {}
for country,fuel_list in capacity_by_country_by_fuel.iteritems():
	average_capacity_factors[country] = {}
	for fuel_type,total_capacity in fuel_list.iteritems():
		gen = generation_by_country_by_fuel[country][fuel_type]
		capacity_factor = CF_CONVERSION_FACTOR * gen / float(total_capacity)
		average_capacity_factors[country][fuel_type] = capacity_factor

# calculate total capacity by country
capacity_total_by_country = {}
for country, fuel_list in capacity_by_country_by_fuel.iteritems():
	capacity_total_by_country[country] = 0
	for fuel,capacity in fuel_list.iteritems():
		capacity_total_by_country[country] += capacity

# now include these values in plant independent variable list
for plant_id,plant_info in plants_for_generation_estimation.iteritems():

	country = plant_info[0]
	fuel_index = plant_info[1]
	fuel_type = fuel_type_list[fuel_index]
	capacity = plant_info[2]
	year = plant_info[3]

	fuel_av_cf = average_capacity_factors[country][fuel_type]
	plants_for_generation_estimation[plant_id].append(fuel_av_cf)

	cap_sh_country = capacity / float(capacity_total_by_country[country])
	plants_for_generation_estimation[plant_id].append(cap_sh_country)

	cap_sh_country_fuel = capacity / float(capacity_by_country_by_fuel[country][fuel_type])
	plants_for_generation_estimation[plant_id].append(cap_sh_country_fuel)

	# now make prediction; translate from cf to generation (GWh)
	X_data = np.array(plants_for_generation_estimation[plant_id][1:]).reshape(1,-1)
	est_cf = est.predict(X_data)[0]
	if est_cf < 0 or est_cf > 1:
		print(u'ERROR: Estimated capacity factor outside of [0,1]')
	est_gen_gwh = est_cf * capacity / CF_CONVERSION_FACTOR
	plants[plant_id].estimated_generation_gwh = est_gen_gwh

# now write the result
pw.write_csv_file(plants,CSV_SAVEFILE)
print(u"Wrote data file with {0} total plants; {1} with estimated generation.".format(len(plants),len(plants_for_generation_estimation)))

