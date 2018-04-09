# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
fix_coordinates_VNM.py
Convert coordinates in VNM data from minutes/seconds to decimal.
"""

import csv
import re
import sys, os

sys.path.insert(0, os.pardir)
sys.path.insert(0, os.path.join(os.pardir, os.pardir))
import powerplant_database as pw

def dms2decimal(degrees,minutes,seconds):
    return float(degrees) + float(minutes)/60 + float(seconds)/(60*60);

# params
ORIGINAL_FILE = pw.make_file_path(fileType = "raw", subFolder = "WRI", filename = "Vietnam.csv")
UPDATED_FILE = "Vietnam_coordinates_fixed.csv"
LAT_COL = 19
LONG_COL = 20

# read original file
with open(ORIGINAL_FILE,'rU') as f_in:
    with open(UPDATED_FILE, 'w') as f_out:

        datareader = csv.reader(f_in)
        datawriter = csv.writer(f_out)
        datawriter.writerow(next(datareader))

        for row in datareader:

            raw_latitude = row[LAT_COL]
            raw_longitude = row[LONG_COL]

            if not raw_latitude or not raw_longitude:       # no data
                datawriter.writerow(row)

            elif '\xc2\xb0' not in raw_latitude:            # decimal coordinates; don't convert
                datawriter.writerow(row) 

            else:                                           # min/sec coords; do conversion
                lat_parts = re.split('(\xc2\xb0)|\'|(\"N)', raw_latitude)
                d_lat = dms2decimal(lat_parts[0],lat_parts[3],lat_parts[6])
                long_parts = re.split('(\xc2\xb0)|\'|(\"E)', raw_longitude)
                d_long = dms2decimal(long_parts[0],long_parts[3],long_parts[6])
                #print("{0},{1} -> {2},{3}".format(raw_latitude,raw_longitude,d_lat,d_long))
                row[LAT_COL] = d_lat
                row[LONG_COL] = d_long
                datawriter.writerow(row)

print("finished")
