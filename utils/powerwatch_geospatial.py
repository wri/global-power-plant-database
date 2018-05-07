"""Create geospatial vector format of the powerwatch database."""
import sys
import os
import argparse
import json

import fiona

sys.path.insert(0, os.pardir)
import powerplant_database as pw

DEFAULT_DATABASE_FILE = os.path.join(pw.OUTPUT_DIR, "global_power_plant_database.csv")
DEFAULT_OUTPUT_FILE = os.path.join(pw.OUTPUT_DIR, "global_power_plant_database.geojson")
DEFAULT_VECTOR_FORMAT = 'GeoJSON'
DEFAULT_COUNTRY_BOUNDS_FILE = os.path.join(pw.RESOURCES_DIR, 'country_bounding_boxes.json')



if __name__ == '__main__':
	argparser = argparse.ArgumentParser(description="Get PowerWatch database in geospatial format.")
	argparser.add_argument('-i', '--input', type=str, default=DEFAULT_DATABASE_FILE)
	argparser.add_argument('-o', '--output', type=str, default=DEFAULT_OUTPUT_FILE)
	argparser.add_argument('-f', '--format', type=str, default=DEFAULT_VECTOR_FORMAT)
	argparser.add_argument('--country', type=str, nargs='+',
		help="ISO-3 country codes; all countries are processed by default.")
	argparser.add_argument('--checkbounds', action='store_true')
	args = argparser.parse_args()


	# prepare list of country codes
	countries = {v.iso_code: k for k, v in pw.make_country_dictionary().iteritems()}
	if args.country is None:
		args.country = sorted(countries.keys(), key=lambda k: countries[k])
	else:
		args.country = [country_name.upper() for country_name in args.country]
		for iso_code in args.country:
			if iso_code not in countries:
				raise ValueError('iso code <{0}> is invalid'.format(iso_code))

	crs = {'init': 'epsg:4326'}
	schema = {
		'geometry': 'Point',
		'properties': {
			'gppd_idnr': 'str',
			'name': 'str',
			'capacity_mw': 'float:12.4',
			'year_of_capacity_data': 'int',
			'country': 'str',
			'country_long': 'str',
			'owner': 'str',
			'source': 'str',
			'url': 'str',
			'geolocation_source': 'str',
			'commissioning_year': 'float:12.4',
			'fuel1': 'str',
			'fuel2': 'str',
			'fuel3': 'str',
			'fuel4': 'str',
			'generation_gwh_2013': 'float:12.4',
			'generation_gwh_2014': 'float:12.4',
			'generation_gwh_2015': 'float:12.4',
			'generation_gwh_2016': 'float:12.4',
			'estimated_generation_gwh': 'float:12.4'
		}
	}

	driver = args.format

	pwdb = pw.read_csv_file_to_dict(args.input)

	country_dict = pw.make_country_dictionary()

	if args.checkbounds:
		bounds_dict = json.load(open(DEFAULT_COUNTRY_BOUNDS_FILE, 'rU'))

	with fiona.open(args.output, 'w', encoding='utf-8', crs=crs, schema=schema, driver=driver) as shp:
		for idnr, pdb in pwdb.iteritems():
			lon = pdb['longitude']
			lat = pdb['latitude']
			outdict = {
				'geometry': {
					'type': 'Point',
					'coordinates': [lon, lat]
				},
				'properties': pdb.copy()
			}

			del outdict['properties']['longitude']
			del outdict['properties']['latitude']

			if args.checkbounds:
				a3 = country_dict[pdb['country']].iso_code
				bounds = bounds_dict[a3]['bounds']
				xmin = bounds['xmin']
				xmax = bounds['xmax']
				ymin = bounds['ymin']
				ymax = bounds['ymax']

				if lon >= xmin and lon <= xmax and lat >= ymin and lat <= ymax:
					continue
			shp.write(outdict)



