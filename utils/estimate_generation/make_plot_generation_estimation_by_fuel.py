# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
make_plot_generation_estimation_by_fuel.py
Generate set of plots of estimated generation by fuel type.
"""

import numpy as np
import csv
import matplotlib.pyplot as plt
import pickle

# set parameters
predictions_filename = "generation_estimation_predictions_by_fuel.pickle"

fuel_type_r2 = {'Oil':0.02, 'Nuclear':0.34, 'Gas':0.15, 
				'Biomass':-0.52, 'Coal':0.12, 'Geothermal':-0.16,
				'Other':-0.26, 'Solar':0.28, 'Waste':-0.03,
				'Wind':0.21, 'Hydro':-0.08}

# colors from colorbrewer2: http://colorbrewer2.org/#type=qualitative&scheme=Paired&n=12
fuel_color = { 
			'Biomass':'#33a02c',
			'Coal':'sienna',
			'Cogeneration':'#e31a1c',
			'Gas':'#a6cee3',
			'Geothermal':'#b2df8a',
			'Hydro':'#1f78b4',
			'Nuclear':'#6a3d9a',
			'Oil':'black',
			'Other':'gray',
			'Petcoke':'#fb9a99',
			'Solar':'#ffff99',
			'Waste':'#fdbf6f',
			'Wave_and_Tidal':'#b15928',
			'Wind':'#ff7f00'
			}

# load from pickle file
prediction_data = pickle.load(open(predictions_filename,'rb'))
y_data_by_fuel = prediction_data['y_data_by_fuel']
predictions_by_fuel = prediction_data['predictions_by_fuel']

# build fuel-specific plot
fig = plt.figure(figsize=(8,6))
fig.subplots_adjust(top=0.90)
fig.subplots_adjust(bottom=0.05)
fig.subplots_adjust(left=0.05)
fig.subplots_adjust(right=0.95)
fig.subplots_adjust(wspace=0.2)
fig.subplots_adjust(hspace=0.4)
i = 1
for fuel_name,r2 in fuel_type_r2.iteritems():
	plt.subplot(3,4,i)
	i += 1
	plt.title(u"{:>10}: R2 = {:3.2f}".format(fuel_name,r2),fontsize=8, loc='left')
	plt.gca().set_aspect('equal')
	plt.scatter(y_data_by_fuel[fuel_name], predictions_by_fuel[fuel_name], marker='.', c=fuel_color[fuel_name])
	plt.ylim([0,1])
	plt.xlim([0,1])
	plt.plot([0,1],[0,1],'r-', lw=1)	# draw x=y line
	plt.tick_params(axis='both', which='major', labelsize=8)

#plt.show()

# save graphic as PDF
plt.savefig('generation_estimation_by_fuel.pdf')

