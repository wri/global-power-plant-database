# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
make_coordinate_file_BRA.py
Get coordinates for Brazil power plants.
Generate static resource file for use with build_database_BRA.py.
NOTE: File is generated in local directory. Must be moved to resources/BRA folder manually.
Data Source: Agência Nacional de Energia Elétrica, Brazil
URL: http://sigel.aneel.gov.br/kmz.html
Notes:
- ANEEL server initially provides KML with network links. To retriev all data, must
provide bbox of entire country with HTTP GET request.
- ANEEL data includes a large number of records with the same CEG ID number. Need 
to determine why this is and cross-check coordinate accuracy.
"""

from zipfile import ZipFile
from lxml import etree, html
import sys, os

sys.path.insert(0, os.pardir)
sys.path.insert(0, os.path.join(os.pardir, os.pardir, os.pardir))
import powerplant_database as pw

# params
COUNTRY_NAME = u"Brazil"
SOURCE_NAME = u"Agência Nacional de Energia Elétrica (Brazil)"
SOURCE_URL = u"http://sigel.aneel.gov.br/kmz.html"
SAVE_CODE = u"BRA"
RAW_FILE_NAME = pw.make_file_path(fileType="raw",subFolder=SAVE_CODE, filename="FILENAME.zip")
CSV_FILE_NAME = "coordinates_BRA.csv"
ENCODING = "UTF-8"

# other parameters
DATASETS = {0: {"name":"UHE","fuel":"Hydro"}, 1: {"name":"PCH","fuel":"Hydro"},
            2: {"name":"CGH","fuel":"Hydro"}, 3: {"name":"EOL","fuel":"Wind"},
            4: {"name":"UTE","fuel":"Coal"}, 5: {"name":"UTN","fuel":"Nuclear"},
            6: {"name":"CGU","fuel":"Other"}, 7: {"name":"UFV","fuel":"Solar"} }

URL_BASE = u"http://sigel.aneel.gov.br/arcgis/services/SIGEL/ExportKMZ/MapServer/KmlServer?Composite=false&LayerIDs=ID_HERE&BBOX=-75.0,-34.0,-30.0,6.0"

# optional raw file(s) download
FILES = {}
for fuel_code,dataset in DATASETS.iteritems():
    RAW_FILE_NAME_this = RAW_FILE_NAME.replace("FILENAME", dataset["name"])
    URL = URL_BASE.replace("ID_HERE",str(fuel_code))
    FILES[RAW_FILE_NAME_this] = URL
DOWNLOAD_FILES = pw.download(COUNTRY_NAME, FILES)

# utility function to compare coordinates
coordinate_tolerance = 1e-2
def is_close(a,b,rel_tol=coordinate_tolerance):
    return abs(a-b) <= rel_tol*max(a,b)

# create dictionary for power plant objects
plants_dictionary = {}

# extract powerplant information from file(s)
print(u"Reading in plants...")
inconsistent_coordinates = 0

# read and parse KMZ files
ns = {"kml":"http://www.opengis.net/kml/2.2"}   # namespace
parser = etree.XMLParser(ns_clean=True, recover=True, encoding=ENCODING)
for fuel_code,dataset in DATASETS.iteritems():
    zipfile = pw.make_file_path(fileType="raw",subFolder=SAVE_CODE,filename=dataset["name"]+".zip")
    kmz_file = ZipFile(zipfile,"r")
    kml_file = kmz_file.open("doc.kml","rU")

    tree = etree.parse(kml_file, parser)
    root = tree.getroot()
    for child in root[0]:
        if u"Folder" in child.tag:
            idval = int(child.attrib[u"id"].strip(u"FeatureLayer"))
            if idval in DATASETS.keys():
                shift = 0
                if idval in [0,3]:
                    shift = 1
                placemarks = child.findall(u"kml:Placemark", ns)
                for pm in placemarks:
                    description = pm.find("kml:description", ns).text # html content
                    content = html.fromstring(description)
                    rows = content.findall("body/table")[1+shift].findall("tr")[1].find("td").find("table").findall("tr")
                    status = u"N/A"
                    plant_id = u""
                    for row in rows:
                        left = row.findall("td")[0].text
                        right = row.findall("td")[1].text

                        # find CEG ID
                        if left == u"CEG":
                            plant_id = pw.format_string(right.strip(),None)

                        # make ID string formatting consistent (is not consistent in raw data)
                        # use only leading alpha chars and 6-digit number; drop trailing digits after "-"
                        if plant_id and u'Null' not in plant_id:

                            if u'.' not in plant_id:
                                plant_id = plant_id[0:3] + u'.' + plant_id[3:5] + u'.' + plant_id[5:7] + u'.' + plant_id[7:13]

                            elif u'-' in plant_id:
                                plant_id = plant_id[0:16]

                    # remove non-operating plants
                    #if status != u"Operação":
                    #    if status == u"Construção não iniciada" and year_updated == None:
                    #        pass
                    #    else:
                    #        continue

                    coordinates = pm.find("kml:Point/kml:coordinates", ns).text.split(",") #[lng, lat, height]
                    try:
                        longitude = float(coordinates[0])
                        latitude = float(coordinates[1])
                    except:
                        latitude,longitude = pw.NO_DATA_NUMERIC,pw.NO_DATA_NUMERIC

                    if plant_id and u'Null' not in plant_id:

                        if plant_id in plants_dictionary.keys():

                            # test if coords are consistent
                            old_latitude = plants_dictionary[plant_id]['latitude']
                            old_longitude = plants_dictionary[plant_id]['longitude']

                            if not is_close(latitude,old_latitude) or not is_close(longitude,old_longitude):
                                print(u"-Error: Inconsistent coordinates for duplicate ID: {0}".format(plant_id))
                                inconsistent_coordinates += 1

                        else:
                            plants_dictionary[plant_id] = {'latitude':latitude,'longitude':longitude}

# report on plants read from file
print(u"...read {0} plants.".format(len(plants_dictionary)))
print(u"CEG ID duplicates with inconsistent coordinates: {0} (fractional tolerance: {1})".format(inconsistent_coordinates,coordinate_tolerance))

# save to CSV file
with open(CSV_FILE_NAME,'w') as f:
    f.write(u'ceg_id,name,latitude,longitude\n')
    for ceg_id,p in plants_dictionary.iteritems():
        f.write(u'{0},{1},{2}\n'.format(ceg_id,p['latitude'],p['longitude']))

