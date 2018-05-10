# This Python file uses the following encoding: utf-8
"""
Global Power Plant Database
train_generation_estimator.py
Train a Gradient Boosted Regression Tree (GBRT) model to estimate power plant electricity 
generation.

Follows methods described on these sites: 
https://www.datarobot.com/blog/gradient-boosted-regression-trees/
http://machinelearningmastery.com/configure-gradient-boosting-algorithm/
http://machinelearningmastery.com/evaluate-gradient-boosting-models-xgboost-python/
https://medium.com/towards-data-science/train-test-split-and-cross-validation-in-python-80b61beca4b6
"""

import numpy as np
import csv
import matplotlib.pyplot as plt
import pickle
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import GridSearchCV, KFold, cross_val_score, cross_val_predict
from sklearn import metrics


# set parameters
data_filename = "generation_data_ARG_EGY_IND_USA.csv"
model_filename = "generation_estimation_model_v01.pickle"
predictions_filename = "generation_estimation_predictions_by_fuel.pickle"

num_folds = 10						# number of cross-validation folds; usual = 10

params = {							# parameters for training estimator
	'n_estimators': 1500,           # AKA number of trees; optimum = 1500 (?)
	'max_depth': 6,					# number of leaves is 2^max_depth; optimum = 6
	'learning_rate': 0.003,			# AKA shrinkage; optimum = 0.003 
	'subsample': 0.5,				# unclear: good choice is 0.5
	'loss':'huber'					# unclear: good choice is 'huber'
}

# main
countries = []						# will hold list of countries
fuel_types = []						# will hold list of fuel types
capacity_factors = {}   			# will hold plant-level cfs by country and fuel
fuel_capacity_factors = {}			# will hold average cfs by country and fuel
capacity_totals = {}				# will hold total capacity by country and fuel
capacity_totals_by_fuel = {}		# will hold total capacity by country

feature_name_list = ['Fuel type','Capacity (MW)','Commissioning year','Average capacity factor (fuel)','Share of national capacity','Share of national capacity by fuel']

# load data
X_data = []
y_data = []

# read in data
with open(data_filename,'rU') as f:

	datareader = csv.reader(f)
	header = datareader.next()
	
	for row in datareader:

		# read in (raw) independent variables (X)

		# handle country
		country_name = row[1]
		if country_name not in countries:
			countries.append(country_name)
		country = countries.index(country_name)

		# handle fuel type
		fuel_name = row[6]
		if fuel_name not in fuel_types:
			fuel_types.append(fuel_name)
		fuel = fuel_types.index(fuel_name)

		# handle plant capacity
		capacity_mw = float(row[7])
		if not capacity_mw:
			print("No capacity for plant in country {0}".format(country_name))
			continue
		if country not in capacity_totals:
			capacity_totals[country] = 0
		if country not in capacity_totals_by_fuel:
			capacity_totals_by_fuel[country] = {}
		if fuel not in capacity_totals_by_fuel[country]:
			capacity_totals_by_fuel[country][fuel] = 0
		capacity_totals[country] += capacity_mw
		capacity_totals_by_fuel[country][fuel] += capacity_mw

		# handle year
		year = int(row[9].split('.')[0])

		# check if generation data exists before adding independent variables to observation list
		if not row[8]:
			print("No generation for {0} plant in country {1}".format(fuel_name,country_name))
			continue

		# calculate capacity factor (dependent variable)
		generation_mwh = float(row[8])
		capacity_factor = generation_mwh / (24.0 * 365 * capacity_mw)
		if capacity_factor <= 0.0 or capacity_factor > 1.0:
			continue    # reject all plants with capacity factors out of range OR zero-generation plants

		# add data to training set
		X_data.append([country,fuel,capacity_mw,year])   	# order must match feature_name_list (others added later)
		y_data.append(capacity_factor)

		# add capacity factor to list for averaging by country and fuel
		if country not in capacity_factors.keys():
			capacity_factors[country] = {}
		if fuel not in capacity_factors[country].keys():
			capacity_factors[country][fuel] = []
		capacity_factors[country][fuel].append(capacity_factor)

print(u'Read in {0} observations.'.format(len(X_data)))

# calculate fuel-specific capacity factors for each country
for country,fuel_list in capacity_factors.iteritems():
	if country not in fuel_capacity_factors.keys():
		fuel_capacity_factors[country] = {}
	for fuel,cfs in fuel_list.iteritems():
		avg_cf = 0.0 if not len(cfs) else float(sum(cfs)) / len(cfs)
		fuel_capacity_factors[country][fuel] = avg_cf
		print(u'{:12}; {:10}: av c.f.: {:1.2f}'.format(countries[country],fuel_types[fuel],avg_cf))

# add fuel-specific capacity factors to X data
for observation in X_data:
	country = observation[0]
	fuel = observation[1]
	try:
		observation.append(fuel_capacity_factors[country][fuel])
	except:
		print("Error with fuel_cap_factors; country: {0}, fuel: {1}".format(country,fuel))
		X_data.remove(observation)

# add plant-specific share of total (country) capacity and total (country,fuel-type) capacity
for observation in X_data:
	country = observation[0]
	if country > len(countries):
		print("Error with country.")
	fuel = observation[1]
	if fuel > len(fuel_types):
		print("Error with fuel type.")
	capacity = observation[2]
	capacity_country_share = capacity / capacity_totals[country]
	observation.append(capacity_country_share)
	capacity_country_fuel_share = capacity / capacity_totals_by_fuel[country][fuel]
	observation.append(capacity_country_fuel_share)

# convert X_data, y_data to numpy arrays
X_data_np = np.array(X_data)
y_data_np = np.array(y_data)

# now that we've used country we can remove before the fit
X_data_np = np.delete(X_data_np,0,1)

# now report on total capacity being used in training data; capacity is now index 1
for country,total_capacity in capacity_totals.iteritems():
	print("{0} capacity total: {1} MW".format(country,total_capacity))
print("Total capacity in training data: {0} MW".format(np.sum(X_data_np[:,1])))

# set up k-fold object for cross-validation
kfold = KFold(n_splits = num_folds)

# create fit estimator
est = GradientBoostingRegressor()
est.set_params(**params)

# do fit
print("Fitting model...")
est.fit(X_data_np, y_data_np)
print("...finished fit.")

# cross-validation
print("Doing cross-validation...")
results = cross_val_score(est, X_data_np, y_data_np, cv=kfold)
acc = results.mean()
dev = results.std()
print("Cross val: {:4.3f} (+/-{:4.3f})".format(acc,dev))

# make prediction plot
fig = plt.figure(figsize=(14,5))
fig.subplots_adjust(left=0.05)
fig.subplots_adjust(right=0.95)
fig.subplots_adjust(wspace=0.35)
#plt.subplot(1,2,1)
#plt.xlabel('Capacity factor (data)')
#plt.ylabel('Capacity factor (model prediction)')

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

capacity_color = {
			'0-0.99MW': 'yellow',
			'1-10MW': 'red',
			'11-100MW': 'green',
			'101-300MW': 'blue',
			'301+MW': 'black'
}

combined_data = zip(X_data_np,y_data_np)

# check fit by fuel type
"""
X_data_by_fuel = {}
y_data_by_fuel = {}
predictions_by_fuel = {}
for dp in combined_data:
	# NOTE: after deleting country, fuel type is a different index
	fuel_type_val = int(dp[0][0])
	fuel_type_name = fuel_types[fuel_type_val]
	if fuel_type_name not in X_data_by_fuel.keys():
		X_data_by_fuel[fuel_type_name] = []
	if fuel_type_name not in y_data_by_fuel.keys():
		y_data_by_fuel[fuel_type_name] = []
	if fuel_type_name not in predictions_by_fuel.keys():
		predictions_by_fuel[fuel_type_name] = []
	X_data_by_fuel[fuel_type_name].append(dp[0])
	y_data_by_fuel[fuel_type_name].append(dp[1])

fuel_type_r2 = {}
for fuel_type in fuel_types:
	fuel_count = len(X_data_by_fuel[fuel_type])
	if fuel_count <= num_folds:
		print(u"Too few samples of fuel type {0} for prediction; skipping.".format(fuel_type))
		continue
	else:
		print(u"Calculating predictions for {0} ({1} plants).".format(fuel_type,fuel_count))
	predictions = cross_val_predict(est, X_data_by_fuel[fuel_type], y_data_by_fuel[fuel_type], cv=kfold)
	predictions_by_fuel[fuel_type] = predictions
	plt.scatter(y_data_by_fuel[fuel_type], predictions, marker='.', c=fuel_color[fuel_type], label=fuel_type)
	r2_score = metrics.r2_score(y_data_by_fuel[fuel_type],predictions)
	fuel_type_r2[fuel_type] = r2_score
	print("Fuel: {:>10}; R2: {:4.3f}".format(fuel_type,r2_score))

# save predictions to file
with open(predictions_filename,'w') as f:
	prediction_data = {'X_data_by_fuel':X_data_by_fuel,'y_data_by_fuel':y_data_by_fuel,'predictions_by_fuel':predictions_by_fuel}
	pickle.dump(prediction_data,f)
print("Saved prediction data to {0}.".format(predictions_filename))
"""

"""
# check fit by capacity
def find_capacity_class(capacity_mw):
	if capacity_mw < 1.0:
		return '0-0.99MW'
	elif capacity_mw < 10:
		return '1-10MW'
	elif capacity_mw < 100:
		return '11-100MW'
	elif capacity_mw < 300:
		return '101-300MW'
	else:
		return '301+MW'

X_data_by_capacity = {'0-0.99MW':[], '1-10MW':[], '11-100MW':[], '101-300MW':[], '301+MW':[]}
y_data_by_capacity = {'0-0.99MW':[], '1-10MW':[], '11-100MW':[], '101-300MW':[], '301+MW':[]}
for dp in combined_data:
	# NOTE: after deleting country, capacity is a different index
	capacity_val = float(dp[0][1])
	capacity_class = find_capacity_class(capacity_val)
	X_data_by_capacity[capacity_class].append(dp[0])
	y_data_by_capacity[capacity_class].append(dp[1])

for cap_class in X_data_by_capacity.keys():
	if len(X_data_by_capacity[cap_class]) <= num_folds:
		print(u"Too few samples of capacity class {0} for prediction; skipping.".format(cap_class))
		continue
	print(u"Calculating predictions for {0}".format(cap_class))
	predictions = cross_val_predict(est, X_data_by_capacity[cap_class], y_data_by_capacity[cap_class], cv=kfold)
	plt.scatter(y_data_by_capacity[cap_class], predictions, marker='.', c=capacity_color[cap_class], label=cap_class)

"""

# do overall prediction and calculate r2
print(u"Calculating overall predictions...")
predictions_all_fuels = cross_val_predict(est,X_data_np,y_data_np,cv=kfold)
r2_score = metrics.r2_score(y_data_np,predictions_all_fuels)
print("R2: {:4.3f}".format(r2_score))

# make legend
ax = plt.gca()
ax.legend(loc='upper center', bbox_to_anchor=(0.5,1.15),ncol=4)

# make feature importance subplot
feature_importance = est.feature_importances_
feature_importance = feature_importance / feature_importance.sum()
sorted_idx = np.argsort(feature_importance)
pos = np.arange(sorted_idx.shape[0]) + 0.5
plt.subplot(1, 2, 2)
plt.barh(pos, feature_importance[sorted_idx], align='center')
feature_names = [feature_name_list[i] for i in sorted_idx]
plt.yticks(pos, feature_names)
plt.xlabel('Relative importance')
plt.title('Variable importance')

# display score
h = 0.60;
ax = plt.gca()
#plt.text(h,0.36,"{0}".format(', '.join(countries)),transform=ax.transAxes,fontsize=10,color='r')
#plt.text(h,0.32,"Total observations: {:,}".format(len(X_data_np)),transform=ax.transAxes,fontsize=10,color='r')	
#plt.text(h,0.28,"N_estimators: {0}".format(params['n_estimators']),transform=ax.transAxes,fontsize=10,color='r')
#plt.text(h,0.24,"Max depth: {0}".format(params['max_depth']),transform=ax.transAxes,fontsize=10,color='r')
#plt.text(h,0.20,"Number of folds: {0}".format(num_folds),transform=ax.transAxes,fontsize=10,color='r')
#plt.text(h,0.16,"Learning rate: {0}".format(params['learning_rate']),transform=ax.transAxes,fontsize=10,color='r')
#plt.text(h,0.12,"Sub-sample: {0}".format(params['subsample']),transform=ax.transAxes,fontsize=10,color='r')
#plt.text(0.65,0.08,"Loss: {0}".format(params['loss']),transform=ax.transAxes,fontsize=10,color='r')
#plt.text(h,0.08,"Loss: Huber",transform=ax.transAxes,fontsize=10,color='r')
#plt.text(h,0.04,"R2: {:4.3f}".format(r2_score),transform=ax.transAxes,fontsize=10,color='r')

# show
plt.show()

# save model
with open(model_filename,'w') as f:
	model_data = {'model': est, 'params': params, 'num_folds': num_folds, 'fuel_types': fuel_types}
	pickle.dump(model_data,f)
print("Saved trained model to {0}.".format(model_filename))

"""
# build fuel-specific plot
fig = plt.figure(figsize=(10,10))
fig.subplots_adjust(top=0.90)
fig.subplots_adjust(bottom=0.05)
fig.subplots_adjust(left=0.05)
fig.subplots_adjust(right=0.95)
fig.subplots_adjust(wspace=0.2)
fig.subplots_adjust(hspace=0.4)
i = 1
for fuel_name,r2 in fuel_type_r2.iteritems():
	plt.subplot(4,3,i)
	i += 1
	plt.title(u"{:>10}: R2 = {:3.2f}".format(fuel_name,r2))
	#plt.xlabel('Capacity factor (data)')
	#plt.ylabel('Capacity factor (model prediction)')
	plt.scatter(y_data_by_fuel[fuel_name], predictions_by_fuel[fuel_name], marker='.', c=fuel_color[fuel_name])
	plt.ylim([0,1])
	plt.xlim([0,1])
	plt.plot([0,0],[1,1],'k-', lw=2)	# draw x=y line
plt.show()
"""
