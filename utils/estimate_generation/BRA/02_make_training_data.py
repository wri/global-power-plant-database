# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
02_make_training_data.py
Convert annual generation data into training data for ML algorithm, for Brazil.
"""

import csv

def latinize_and_upper(s):
	return unidecode(s).upper()

# set general parameters
ENCODING = 'utf-8'
annual_generation_data = "annual_data/BRA_generation_data_annual.csv"
general_data = "../../../source_databases_csv/database_BRA.csv"
output_file = "training_data_BRA.csv"

# main

# read in general power plant data (capacity, etc)
power_plants = {}
with open(general_data,'rU') as f:
	datareader = csv.reader(f)
	header1 = next(datareader)
	header2 = next(datareader)
	for row in datareader:
		name = row[0]
		idval = row[1]
		capacity = float(row[2])
		try:
			latitude = float(row[8])
			longitude = float(row[9])
		except:
			latitude,longitude = None,None
		if row[10]:
			commissioning_year = int(row[10])
		else:
			commissioning_year = None
		fuel = row[11]
		power_plants[idval] = {'name':name,'capacity':capacity,'latitude':latitude,
			'longitude':longitude,'commissioning_year':commissioning_year,'fuel':fuel}

print(u"Loaded {0} plants from main database.".format(len(power_plants)))

# load generation data
generation_output = {}
with open(annual_generation_data,'rU') as f:
	datareader = csv.reader(f)
	header = next(datareader)
	for row in datareader:
		idval = row[0]
		gen2012 = float(row[8])
		gen2013 = float(row[9])
		gen2014 = float(row[10])
		gen2015 = float(row[11])
		gen2016 = float(row[12])
		if idval in power_plants.keys():
			mp = power_plants[idval]
			generation_output[idval] = {'name':mp['name'],'fuel':mp['fuel'],
				'commissioning_year':mp['commissioning_year'],
				'capacity':mp['capacity'],'latitude':mp['latitude'],'longitude':mp['longitude'],
				'gen2012':gen2012,'gen2013':gen2013,'gen2014':gen2014,
				'gen2015':gen2015,'gen2016':gen2016}
		else:
			print(u"-Error: Can't find plant with ID {0}".format(idval))

# write output
with open(output_file,'w') as f:
	f.write(u"id,country,capacity_mw,commissioning_year,latitude,longitude,generation_gwh,capacity_factor,fuel1,fuel2,fuel3,fuel4\n")
	for idval,plant in generation_output.iteritems():
		name = plant['name']
		country = u"Brazil"
		capacity_mw = plant['capacity']
		fuel = plant['fuel']
		commissioning_year = plant['commissioning_year']
		if commissioning_year is None:
			continue
		latitude = plant['latitude']
		longitude = plant['longitude']
		if latitude is None or longitude is None:
			continue
		for year in ['2014','2015','2016']:
			generation_gwh = plant['gen'+year]
			cap_factor = generation_gwh / (24 * 365 * capacity_mw) 
			if cap_factor > 1.0:
				print(u'-Error: Capacity factor for ID: {0} is {1}'.format(idval,cap_factor))
				continue
			f.write(u"{0},{1},{2},{3},{4},{5},{6},{7},{8}\n".format(idval,country,capacity_mw,
				commissioning_year,latitude,longitude,generation_gwh,cap_factor,fuel))

print('Finished')
