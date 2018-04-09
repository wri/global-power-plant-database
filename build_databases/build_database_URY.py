# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
built_database_URY.py
Get power plant data from Uruguay and convert to the Global Power Plant Database format.
Data source: La Administración Nacional de Usinas y Trasmisiones Eléctricas (UTE)
URL: https://www.ute.com.uy/SgePublico/mapa.aspx
Alternative visualization: http://www.ute.com.uy/pags/kml_view/Mapafgen.html
Additional source for possible future use: http://adme.com.uy/mme_admin/participantes/generadores.php
TODO: Extract geolocation data separately and store as resource file.
Notes:
- Four plants cannot be parsed due to un-escaped quote marks or other errors in the text.
- "Thermal fuel" category is as follows:
    - Central Termica del Respaldo (CTR) (819) - Gas (see http://www.se4all.org/sites/default/files/Uruguay_RAGA_ES_Released.pdf)
    - Motores Central Batlle (820) - Diesel (see http://www.se4all.org/sites/default/files/Uruguay_RAGA_ES_Released.pdf)
    - Punta del Tigre A (824) - Gas, Diesel (see see https://publications.iadb.org/handle/11319/7980)
    - Punta del Tigre B (825) - Gas (see https://publications.iadb.org/handle/11319/7980)
    - Punta del Tigre 7 y 8 (826) - Gas (see https://publications.iadb.org/handle/11319/7980)
    - Zendaleather (837) - Gas (see http://adme.com.uy/mme_admin/participantes/generadores.php)
    - Turbina Rio Branco (891) - Fuel Oil/Diesel (visual inspection on Google Maps)
    - San Gregorio (894) - Fuel Oil (see http://www.orosur.ca/images/October-2010-San-Gregorio-Technical-Report.compressed.pdf)
"""

from lxml import etree, html
import ast
import csv
import sys, os

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Uruguay"
SOURCE_NAME = u"La Administración Nacional de Usinas y Trasmisiones Eléctricas (Uruguay)"
SOURCE_URL = u"https://www.ute.com.uy/SgePublico/mapa.aspx"
SOURCE_YEAR = 2018
SAVE_CODE = u"URY"
RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="mapa.aspx.html")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_{0}.csv".format(SAVE_CODE))
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
#COORDINATE_FILE = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE, filename="coordinates_{0}.csv".format(SAVE_CODE))
ENCODING = "UTF-8"

# download files if requested
DOWNLOAD_FILES = pw.download('UTE data', {RAW_FILE_NAME: SOURCE_URL})

# make URY-specific fuel parser
def parse_fuel_URY(fuel_string, id_val):

    fuel_synonyms = {   "fot": "Solar",
                        "eol": "Wind",
                        #"ter": "Thermal",
                        "bio": "Biomass",
                        "hid": "Hydro"}

    special_fuel_corrections = {819: "Gas",
                                824: "Gas and Oil",
                                825: "Gas",
                                826: "Gas",
                                837: "Gas",
                                891: "Oil",
                                894: "Oil"}

    if id_val in special_fuel_corrections.keys():
        return special_fuel_corrections[id_val]

    elif fuel_string in fuel_synonyms.keys():
        return fuel_synonyms[fuel_string]

    else:   # case of very small "thermal" plants - must be diesel generators
        return "Oil"

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# parse HTML to XML
parser = etree.HTMLParser(encoding=ENCODING)
tree = etree.parse(RAW_FILE_NAME, parser)
root = tree.getroot()

# get list of plant markers
plant_markers = tree.findall("body/form/ul/li")

# parse plant markers
for p in plant_markers:

    dict_string = p.attrib['data-gmapping']
    try:
        p_dict = ast.literal_eval(dict_string)      # safer than eval()
    except:
        print(u"- Error: Can't evaluate string to dictionary:")
        print(dict_string)
        continue

    # only read operational plants
    try:
        operational_status = p_dict['operativo']
        if operational_status != "En Servicio":
            continue
    except:
        print(u"- Error: Can't evaluate operational status.")
        continue

    # get id
    try:
        id_val = int(p_dict['id'])
    except:
        print(u"- Error: Can't get ID.")
        continue

    # get name
    try:
        name = pw.format_string(p_dict['generador'], ENCODING)
    except:
        print(u"- Error: Can't get name for plant {0}".format(id_val))
        continue

    # get capacity in MW
    try:
        capacity = float(p_dict['potenciaInstalada'].strip(" MW"))
    except:
        print(u"- Error: Can't get capacity for plant {0}".format(id_val))
        continue

    # get fuel type
    try:
        fuel_string_raw = p_dict['icon']
        fuel_type = parse_fuel_URY(fuel_string_raw[9:12], id_val)         # extract fuel name from icon name
        fuel = pw.standardize_fuel(fuel_type, fuel_thesaurus)
    except:
        print(u"- Error: Can't read fuel type for plant {0}".format(id_val))
        continue

    # get coordinates
    try:
        c_dict = p_dict['latlng']
        latitude = float(p_dict['latlng']['lat'])
        longitude = float(p_dict['latlng']['lng'])
        geolocation_source = SOURCE_NAME
    except:
        print(u"- Error: Can't read coordinates for plant {0}".format(id_val))
        geolocation_source = pw.NO_DATA_UNICODE

    # get owner
    try:
        owner = pw.format_string(p_dict['empresa'])
    except:
        print(u"- Error: Can't read owner for plant {0}".format(id_val))
        owner = pw.NO_DATA_UNICODE
        
    # assign ID number
    idnr = pw.make_id(SAVE_CODE,id_val)

    # special coordinate corrections
    #if idnr in special_coordinate_corrections.keys():
    #    latitude,longitude = special_coordinate_corrections[idnr]
    #    print(u"Special lat/long correction for plant {0}".format(idnr))

    new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
    new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=COUNTRY_NAME,
        plant_location=new_location, plant_coord_source=geolocation_source,
        plant_fuel=fuel, plant_capacity=capacity,
        plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL, plant_cap_year=SOURCE_YEAR)
    plants_dictionary[idnr] = new_plant

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
