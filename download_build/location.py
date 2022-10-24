from requirements import *
def process_locations(cleanDataDir, filePath_location):
	locations = pd.read_csv(filePath_location, sep='\t', dtype = str)
	# The following function was needed to process rawlocation, but not the location table
	#locations = clean_locations(locations)
	# Fill city, state, and country with NULL when NA so that unique IDs can be assigned correctly
	#locations[['city', 'state', 'country']] = locations[['city', 'state', 'country']].fillna('-')
	# Rename id column for use in locationMap
	locations.rename(columns = {'id': 'uuid'}, inplace=True)
	locations[['lat', 'long']] = locations['latlong'].str.split('|', n=2, expand=True)
	locations.drop('latlong', axis=1, inplace=True)
	locations.replace({"": None}, inplace=True)
	locations.to_csv(cleanDataDir + 'location.tsv', sep = '\t', index=False)

	#location(cleanDataDir, locations)
	#locationMap(cleanDataDir, locations)
	#disLocation(cleanDataDir, locations)

####### FUNCTIONS BELOW THIS LINE NOT CURRENTLY USED #######
def locationMap(cleanDataDir, locations):
	temp = locations[['locationId', 'location_id']]
	temp.to_csv(cleanDataDir + 'locationMap.tsv', sep = '\t', index=False)

def location(cleanDataDir, locations):
	temp = locations[['locationId', 'city', 'state', 'country', 'latitude',	'longitude', 'county', 'state_fips', 'county_fips']]
	temp = temp.rename(columns = {'latitude': 'lat', 'longitude': 'long', 'state_fips': 'stateFips', 'county_fips': 'countyFips'})

	temp.to_csv(cleanDataDir + 'location.tsv', sep = '\t', index=False)

def disLocation(cleanDataDir, locations):
	# Create lat/long fields
	# Split on pipe char, n is max expansions (only one pipe char), expand to two new columns
	latlong = locations['latlong'].str.split('|', n = 1, expand = True)
	locations['lat'] = latlong[0]
	locations['long'] = latlong[1]

	temp = locations[['locationDid', 'lat', 'long', 'city', 'state', 'country']]
	# The only locationDid duplicates should be null values
	temp = temp.drop_duplicates()
	# Fill na values. Lat/Long values can't exceed +/- 90 (lat), +/- 180 (long)
	temp[['lat', 'long']] = temp[['lat', 'long']].fillna(200)
	temp.to_csv(cleanDataDir + 'disLocation.tsv', sep = '\t', index=False)
	
# This function is needed to clean the locations dataframe
'''
In two random spots in the data, there are file declarations that look like this: """""""R-Ploie<CUSTOM-CHARACTER FILE=""""""""US06710218-20040323-P00701.TIF"""""""" ALT=""""""""custom character"""""""" HE=""""
This causes thousands of entries to get read in a single column which throws off the computation
These rows are extracted, parsed, and combined with the rest of the data
This may not be needed in the future if the source corrects this issue
'''
def clean_locations(locations):
	# These reference the location IDs where the bad data is
	badId_1 = '4z0l6e7357j3r5n3pf18xotos'
	badId_2 = 'euqnnvn9e83qvc6fde73c1t8u'
	# Collect bad data for use later
	badRows = []
	badRows.append(locations[locations['id'] == badId_1])
	badRows.append(locations[locations['id'] == badId_2])
	# Collect good data (by getting rid of bad data)
	goodData = locations[locations['id'] != badId_1]
	goodData = goodData[goodData['id'] != badId_2]

	# Parse and clean bad data
	# Note: each entry of the "badRows" list is a dataframe with one row so I just refer to it as a row
	# Note: the data we need is in the "city" column
	parsedLines = []
	for row in badRows:
		data = row.iloc[0]['city']
		# The data is tab-separated for fields and newline-separated for lines
		# So we can first split by newline to separate rows, then split by tabs to separate columns 
		# Each row of the data (besides the first) looks like this: '4z0l94azyypjy87qpslnm3my7\tx2rkgupfbd4e\tNagoya\tNULL\tJP\tundisambiguated'
		lines = data.split('\n')
		# First line is the most messy and needs the most tlc
		firstLine = lines[0]
		# First element (in first line) is the quoted text from the description above this function, so remove it
		firstLine = firstLine.split('\t')[1:]
		# Append data from other columns
		firstLine.insert(0, row.iloc[0]['location_id'])
		firstLine.insert(0, row.iloc[0]['id'])
		# Save this row
		parsedLines.append(firstLine)
		# Process other lines
		lines = lines[1:]
		for line in lines:
			line = line.split('\t')
			parsedLines.append(line)

	cleanedLines = pd.DataFrame(data=parsedLines, columns = ['id', 'location_id', 'city', 'state', 'country', 'latlong'])	

	goodData = goodData.append(cleanedLines)
	return (goodData)










