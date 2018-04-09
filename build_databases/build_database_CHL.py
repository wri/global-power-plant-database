# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_chile.py
Get power plant data from Chile and convert to the Global Power Plant Database format.
Data Source: Comision Nacional de Energia, Chile
Additional information: http://energiaabierta.cl/electricidad/
Location data derived from http://datos.energiaabierta.cl/rest/datastreams/107253/data.csv
Issues:
- Data includes "estado" (status); should eventually filter on operational plants, 
although currently all listed plants are operational
- Biomass plant file does not include names. Use names from location file.
- Data files use numeric format inconsistently (i.e. use of "." or ","). Treat specially.
- Data files use different column headings, including inconsistent white space and spelling.
- Solar data file does not include owner.
"""

import csv
import sys, os

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Chile"
SOURCE_NAME = u"Energía Abierta"
SOURCE_URL = u"http://energiaabierta.cl/electricidad/"
SAVE_CODE = u"CHL"
YEAR_POSTED = 2016  # Year of data posted on line
RAW_FILE_NAME = pw.make_file_path(fileType='raw', subFolder=SAVE_CODE, filename="FILENAME")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_{0}.csv".format(SAVE_CODE))
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
LOCATION_FILE_NAME = pw.make_file_path(fileType="resource",subFolder=SAVE_CODE,filename="locations_{0}.csv".format(SAVE_CODE))

# other parameters
URL_BASE = "http://datos.energiaabierta.cl/rest/datastreams/NUMBER/data.csv"

DATASETS = [{"number": "215392", "fuel": "Thermal", "filename": "chile_power_plants_thermal.csv", "idstart": 0},
            {"number":"215386", "fuel": "Hydro", "filename": "chile_power_plants_hydro.csv", "idstart": 1000},
            {"number":"215384", "fuel": "Wind", "filename": "chile_power_plants_wind.csv", "idstart": 2000},
            {"number":"215381", "fuel": "Biomass", "filename": "chile_power_plants_biomass.csv", "idstart": 3000},
            {"number":"215391", "fuel": "Solar", "filename": "chile_power_plants_solar.csv", "idstart": 4000}]

def lookup_location(fuel, idval, plant_locations):

    fuel_name = next(iter(fuel))
    if fuel_name in ["Coal", "Gas", "Oil"]:
        fuel_name = "Thermal"

    if idval in plant_locations[fuel_name].keys():
        return plant_locations[fuel_name][idval][0:2]
    else:
        return pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC

# download if specified
FILES = {}
for dataset in DATASETS:
     RAW_FILE_NAME_this = RAW_FILE_NAME.replace("FILENAME", dataset["filename"])
     URL = URL_BASE.replace("NUMBER", dataset["number"])
     FILES[RAW_FILE_NAME_this] = URL
DOWNLOAD_FILES = pw.download("Chile power plant data", FILES)

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# set up country name thesaurus
country_thesaurus = pw.make_country_names_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")

# read static location file [fuel,id,name,latitude,longitude]
plant_locations = {"Thermal": {}, "Hydro": {}, "Wind": {}, "Solar": {}, "Biomass": {}, "Unknown": {}}
with open(LOCATION_FILE_NAME, 'rbu') as f:
    datareader = csv.reader(f)
    headers = [x.lower() for x in datareader.next()]
    for row in datareader:
        fuel = row[0]
        idval = int(row[1])
        name = row[2]
        latitude = float(row[3])
        longitude = float(row[4])
        plant_locations[fuel][idval] = [latitude, longitude, name]

# read plant files
for dataset in DATASETS:
    dataset_filename = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename=dataset["filename"])

    with open(dataset_filename, "rbU") as f:
        datareader = csv.DictReader(f)
        for row in datareader:

            try:
                idval = int(row["gid"])
            except:
                print("-Error: Can't read ID for line {0}, skipping.".format(count))
                continue

            try:
                name = pw.format_string(row.get("nombre", row.get("comuna", pw.NO_DATA_UNICODE)))
            except:
                print(u"-Error: Can't read name for ID {0}, skipping.".format(idval))
                continue              

            try:
                fuel_string = row.get("Tipo", row.get("Tipo ", row.get("Combustible", pw.NO_DATA_UNICODE)))
                fuel = pw.standardize_fuel(fuel_string, fuel_thesaurus)
            except:
                print(u"-Error: Can't read fuel for plant {0}.".format(name))
                fuel = pw.NO_DATA_SET

            # special treament of name for biomass plants (not included in data file)
            if fuel == set(["Biomass"]):
                try:
                    name = pw.format_string(plant_locations["Biomass"][idval][2])
                except:
                    print("-Error: Can't read name for ID {0} (Biomass).".format(idval))
                    continue

            try:
                latitude,longitude = lookup_location(fuel, idval, plant_locations)
                geolocation_source = SOURCE_NAME
            except:
                print(u"-Error: Can't find location for plant {0}.".format(name))
                latitude,longitude = pw.NO_DATA_NUMERIC, pw.NO_DATA_NUMERIC
                geolocation_source = pw.NO_DATA_UNICODE

            try:
                capacity = float(row.get("Potencia MW", pw.NO_DATA_NUMERIC))
            except:
                try:
                    capacity_string = row.get("Potencia MW", pw.NO_DATA_NUMERIC)
                    capacity = float(capacity_string.replace(",", "."))
                except:
                    print(u"-Error: Can't read capacity for plant {0}.".format(name))
                    capacity = pw.NO_DATA_NUMERIC

            try:
                owner_string = row.get("Propietario",row.get("Propiedad", pw.NO_DATA_UNICODE))
                if not owner_string:
                    print("-Error: No owner string for plant {0}".format(name))
                    owner = pw.NO_DATA_UNICODE
                owner = pw.format_string(owner_string)
            except:
                print(u"-Error: Can't read owner for plant {0}.".format(name))
                owner = pw.NO_DATA_UNICODE

            # assign ID number, make PowerPlant object, add to dictionary
            idnr = pw.make_id(SAVE_CODE,dataset["idstart"] + idval)
            new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
            new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=COUNTRY_NAME,
                plant_owner=owner,
                plant_location=new_location, plant_coord_source=geolocation_source,
                plant_fuel=fuel,
                plant_capacity=capacity, plant_cap_year=YEAR_POSTED,
                plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL)
            plants_dictionary[idnr] = new_plant


# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
