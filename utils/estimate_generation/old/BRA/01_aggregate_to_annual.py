# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
01_aggregate_to_annual.py
Aggregate weekly generation data to annual for Brazil.
Read in name matches from match file. 
Test whether plants with duplicate IDs in match file have data in the same year.
If so, report this when it occurs for more than one year.
"""

import csv

# set general parameters
input_filenames = {"weekly_data/BRA_wind_plants.csv":u"Wind",
					"weekly_data/BRA_hydro_plants.csv":u"Hydro",
					"weekly_data/BRA_thermal_plants.csv":u"Thermal"}

output_filename = "annual_data/BRA_generation_data_annual.csv"

match_filename = "name_matches_BRA.csv"

# main
aggregated_data = {}

# load matches
matches = {u'Hydro':{},u'Thermal':{},u'Wind':{}}
with open(match_filename,'rU') as f:
	datareader = csv.reader(f)
	headers = datareader.next()
	for row in datareader:
		gen_name = row[0]
		fuel_type = row[1]
		match_name = row[2]
		match_id = row[3]
		if gen_name not in matches[fuel_type].keys():
			matches[fuel_type][gen_name] = match_id

# load data

matched_count = 0
not_matched_count = 0

for input_file,fuel_type in input_filenames.iteritems():

	with open(input_file,'rU') as f:
		datareader = csv.reader(f)
		header = datareader.next()

		# determine year for each column
		col_years = {}
		for i in range(1,len(header)):
			year = int(header[i][0:4])
			col_years[i] = year

		for row in datareader:

			name = row[0]

			# check if this name is matched - only output if so
			if name in matches[fuel_type].keys() and matches[fuel_type][name]:

				matched_count += 1

				match_id = matches[fuel_type][name]

				# sum cells in row by year
				a = { 2005:0.0, 2006:0.0, 2007:0.0, 2008:0.0, 
						2009:0.0, 2010:0.0, 2011:0.0, 2012:0.0, 
						2013:0.0, 2014:0.0, 2015:0.0, 2016:0.0,
						2017:0.0 }

				for col,year in col_years.iteritems():
					val = (0.0 if not row[col] else float(row[col]))
					a[year] += val

				if match_id not in aggregated_data:
					aggregated_data[match_id] = a

				else:
					# sum values with previous entry
					# also test if values overlap in time
					overlap_years = []
					for year,total in a.iteritems():

						if total != 0 and aggregated_data[match_id][year] != 0:
							overlap_years.append(year)

						aggregated_data[match_id][year] += total

					if len(overlap_years) > 1:
						print(u"Found {0} overlapped years for duplicate ID: {1}".format(len(overlap_years),match_id))

			else:   # name not matched
				not_matched_count += 1

# write output to csv
with open(output_filename,'w') as f:
	f.write(u'id,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016\n')
	for idval,v in aggregated_data.iteritems():
		f.write(u'{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}\n'.format(idval,
			v[2005],v[2006],v[2007],v[2008],v[2009],v[2010],v[2011],v[2012],v[2013],v[2014],v[2015],v[2016]))

print(u"Read in data for {0} matched plants; {1} un-matched plants.".format(matched_count,not_matched_count))
print('...finished.')
