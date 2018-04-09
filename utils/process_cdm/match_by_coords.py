# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
match_by_coords.py
Find best match between two lists of plants with coordinates.
Attempts to find closest match by distance.
Input files must have cols in this order: (NAME, LATITUDE, LONGITUDE)
Haversine formula courtesy of https://stackoverflow.com/questions/15736995/how-can-i-quickly-estimate-the-distance-between-two-latitude-longitude-points
"""

import sys, os
import csv
import argparse
from math import radians, cos, sin, asin, sqrt

# params
MATCH_DISTANCE_KM = 1;


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km

# parse filenames
parser = argparse.ArgumentParser()
parser.add_argument("filename1",type=str)
parser.add_argument("filename2",type=str)
args = parser.parse_args()

# open files and read in plants
plants1 = {}
with open(args.filename1,'rU') as f:
    datareader = csv.reader(f)
    header = datareader.next()
    for row in datareader:
        plant_name = row[0]
        if not row[1] or not row[2]:
            continue
        latitude = float(row[1])
        longitude = float(row[2])
        plants1[plant_name] = {'latitude':latitude, 'longitude':longitude}

plants2 = {}
with open(args.filename2,'rU') as f:
    datareader = csv.reader(f)
    header = datareader.next()
    for row in datareader:
        plant_name = row[0]
        if not row[1] or not row[2]:
            continue
        latitude = float(row[1])
        longitude = float(row[2])
        plants2[plant_name] = {'latitude':latitude, 'longitude':longitude}
      
print("Read {0} plants from file 1, {1} plants from file 2.".format(len(plants1),len(plants2)))

total_possible_matches = 0
for plant_nameA,plantA in plants1.iteritems():
    lonA = plantA['longitude']
    latA = plantA['latitude']
    shortest = 9999
    for plant_nameB, plantB in plants2.iteritems():
        lonB = plantB['longitude']
        latB = plantB['latitude']
        dist = haversine(lonA,latA,lonB,latB)
        if dist < shortest:
            shortest = dist
    if shortest < MATCH_DISTANCE_KM:
        print("- Possible match: {0} with {1} (distance {2} km).".format(plant_nameA,plant_nameB,int(shortest)))
        total_possible_matches += 1

print("Found {0} possible matches with distance range {1}.".format(total_possible_matches,MATCH_DISTANCE_KM))
