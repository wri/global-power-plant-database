# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
build_database_BRA.py
Get power plant data from Brazil and convert to the Global Power Plant Database format.
Data source: Agência Nacional de Energia Elétrica, Brazil
Geolocation data source:http://sigel.aneel.gov.br/kmz.html
Geolocation data is extracted separately and store as resource file.
Fuel code information is here: http://www2.aneel.gov.br/scg/formacao_CEG.asp
Notes:
- ANEEL server initially provides KML with network links. To retriev all data, must
provide bbox of entire country with HTTP GET request.
Issues:
- Clarify fuel types for Termoelectrica
- Clarify status terms
- Known lat/long errors:
    - BRA0029736 (Jirau) - should be -9.2664,-64.6478
    - BRA0026730 (Santa Rosa) - should be -27.7298,-54.4082 (probably)
"""

# not the best way to do this, but use for now
special_coordinate_corrections =   {'BRA0029736':[-9.2664,-64.6478],
                                    'BRA0026730':[-27.7298,-54.4082]}


from lxml import etree, html
from dateutil.parser import parse as parse_date
import csv
import locale
import sys, os

sys.path.insert(0, os.pardir)
import powerplant_database as pw

# params
COUNTRY_NAME = u"Brazil"
SOURCE_NAME = u"Agência Nacional de Energia Elétrica (Brazil)"
SOURCE_URL = u"http://www2.aneel.gov.br/aplicacoes/capacidadebrasil/capacidadebrasil.cfm"
SOURCE_YEAR = 2017
SAVE_CODE = u"BRA"
RAW_FILE_NAME = pw.make_file_path(fileType="raw", subFolder=SAVE_CODE, filename="BRA_data.html")
CSV_FILE_NAME = pw.make_file_path(fileType="src_csv", filename="database_BRA.csv")
SAVE_DIRECTORY = pw.make_file_path(fileType="src_bin")
COORDINATE_FILE = pw.make_file_path(fileType="resource", subFolder=SAVE_CODE, filename="coordinates_BRA.csv")
CAPACITY_CONVERSION_TO_MW = 0.001       # capacity values are given in kW in the raw data
ENCODING = "ISO-8859-1"

# set locale to Portuguese/Brazil
locale.setlocale(locale.LC_ALL,'pt_BR')

# download files if requested (large file; slow)
DOWNLOAD_URL = u"http://www2.aneel.gov.br/aplicacoes/capacidadebrasil/GeracaoTipoFase.asp"
POST_DATA = {'tipo': 0,'fase': 3}
DOWNLOAD_FILES = pw.download('ANEEL B.I.G.', {RAW_FILE_NAME: DOWNLOAD_URL}, POST_DATA)

# define specialized fuel type interpreter
generator_types = {u'CGH':u'Hydro',
                u'CGU':u'Wave and Tidal',
                u'EOL':u'Wind',
                u'PCH':u'Hydro',
                u'UFV':u'Solar',
                u'UHE':u'Hydro',
                u'UTE':u'Thermal',
                u'UTN':u'Nuclear'}

fuel_types = {  u'FL':u'Biomass',
                u'RU':u'Waste',
                u'RA':u'Waste',
                u'BL':u'Biomass',
                u'AI':u'Biomass',
                u'CV':u'Wind',
                u'PE':u'Oil',
                u'CM':u'Coal',
                u'GN':u'Gas',
                u'OF':u'Other',
                u'PH':u'Hydro',
                u'UR':u'Nuclear',
                u'RS':u'Solar',
                u'CA':u'Wave and Tidal'}

def standardize_fuel_BRA(ceg_code):

    fuel_code = ceg_code[4:6]
    return fuel_types[fuel_code]

# set up fuel type thesaurus
fuel_thesaurus = pw.make_fuel_thesaurus()

# create dictionary for power plant objects
plants_dictionary = {}

# read in geolocation data
plant_coordinates = {}
with open(COORDINATE_FILE, 'rU') as f:
    datareader = csv.reader(f)
    header = datareader.next()
    for row in datareader:
        ceg_id = row[0]
        latitude = float(row[1])
        longitude = float(row[2])
        plant_coordinates[ceg_id] = {'latitude': latitude, 'longitude': longitude}

plant_coordinates_keys = plant_coordinates.keys()

# extract powerplant information from file(s)
print(u"Reading in plants...")

# parse HTML to extract tables
parser = etree.HTMLParser(encoding=ENCODING)
tree = etree.parse(RAW_FILE_NAME, parser)
root = tree.getroot()

# get relevant table (second of three)
plant_table = tree.findall("body/table")[1]

# parse rows (skip two header lines and one footer line)
found_coordinates_count = 0
found_operational_year_count = 0
for row in plant_table.findall("tr")[2:-1]:
    cells = row.findall("td")
    
    # get plant code
    # TODO: handle only known edge case: Roca Grande (2535)
    ceg_code = cells[0].findall("font/a")[0].text.strip()
    plant_id = int(ceg_code[-11:-5])
    fuel = standardize_fuel_BRA(ceg_code)

    # use CEG code to lookup coordinates
    ceg_code_short = ceg_code[0:16]
    if ceg_code_short in plant_coordinates_keys:
        latitude = plant_coordinates[ceg_code_short]['latitude']
        longitude = plant_coordinates[ceg_code_short]['longitude']
        found_coordinates_count += 1
        geolocation_source = SOURCE_NAME
    else:
        #print(u"-Error: No coordinates for CEG ID: {0}".format(ceg_code))
        latitude = pw.NO_DATA_NUMERIC
        longitude = pw.NO_DATA_NUMERIC
        geolocation_source = pw.NO_DATA_UNICODE

    # get plant name
    name = pw.format_string(cells[1].findall("font/a")[0].text.strip(),None)

    # get operational date
    op_date = cells[2].findall("font")[0].text.strip()
    if op_date:
        try:
            d = parse_date(op_date)
            op_year = d.year
            found_operational_year_count += 1
        except:
            op_year = pw.NO_DATA_NUMERIC
    else:
        op_year = pw.NO_DATA_NUMERIC

    # get plant capacity
    capacity = CAPACITY_CONVERSION_TO_MW * locale.atof(cells[4].findall("font")[0].text)

    # get owner
    owner = u""
    owner_description = pw.format_string(cells[6].findall("font")[0].text.strip(), ENCODING)
    if u"não identificado" in owner_description:
        owner = pw.NO_DATA_UNICODE
        break

    # TODO: Read owner correctly
    owner = owner_description
 
    """
    for owner in owner_list:
        if u"não identificado" in owner.text:
            owner_full = pw.NO_DATA_UNICODE
            break
        owner_share = owner.text.strip().replace(u"para", u"by") + u" "
        owner_name = owner.findall("a")[0].text.strip()
        owner_conjunction = u"; " if owner_full else u""
        owner_full = owner_full + owner_conjunction + owner_share + owner_name
        #owner_full.append(u" " + owner_name.text)        

    if u"50" in owner_full:
        print(u"Plant: {0}; Owner(s): {1}".format(name, owner_full))    

    #print(u"Plant: {0}; Owner(s): {1}".format(name, owner_full))
    """

    # assign ID number
    idnr = pw.make_id(SAVE_CODE, plant_id)

    # special coordinate corrections
    if idnr in special_coordinate_corrections.keys():
        latitude,longitude = special_coordinate_corrections[idnr]
        print(u"Special lat/long correction for plant {0}".format(idnr))
        geolocation_source = u"WRI"

    new_location = pw.LocationObject(pw.NO_DATA_UNICODE, latitude, longitude)
    new_plant = pw.PowerPlant(plant_idnr=idnr, plant_name=name, plant_country=COUNTRY_NAME,
        plant_location=new_location, plant_coord_source=geolocation_source,
        plant_fuel=fuel, plant_capacity=capacity,
        plant_source=SOURCE_NAME, plant_source_url=SOURCE_URL, plant_cap_year=SOURCE_YEAR,
        plant_commissioning_year=op_year)
    plants_dictionary[idnr] = new_plant

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))
print(u"Found coordinates for {0} plants.".format(found_coordinates_count))
print(u"Found operational year for {0} plants.".format(found_operational_year_count))

# write database to csv format
pw.write_csv_file(plants_dictionary, CSV_FILE_NAME)

# save database
pw.save_database(plants_dictionary, SAVE_CODE, SAVE_DIRECTORY)
print(u"Pickled database to {0}".format(SAVE_DIRECTORY))
