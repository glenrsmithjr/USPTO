from requirements import *
def process_inventors(cleanDataDir, filePath_inventor, filePath_patent_inventor_map, filePath_location_inventor_map):
	# Table that maps raw patent IDs to in-house generated patent IDs
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)
	# Table that maps raw location IDs to in-house generated location IDs
	locationMap = pd.read_csv(cleanDataDir + 'locationMap.tsv', sep = '\t', dtype = str)
	# Table that maps patent IDs to inventor IDs
	patentInventorMap = pd.read_csv(filePath_patent_inventor_map, sep = '\t', dtype = str)
	# Table that maps location IDs to inventor IDs
	locInventorMap = pd.read_csv(filePath_location_inventor_map, sep = '\t', dtype = str)
	# Locations
	locations = pd.read_csv(cleanDataDir + 'location.tsv', sep = '\t', dtype = str)
	# Read in inventors
	inventors = pd.read_csv(filePath_inventor, sep = '\t',  dtype = str, engine = 'python', error_bad_lines =False)
	# Fill NAs with NULL so that unique IDs can be created correctly
	inventors[['name_first', 'name_last']] = inventors[['name_first', 'name_last']].fillna('-')
	inventors['inventorId'] = inventors.groupby(['name_first', 'name_last']).ngroup()
	inventors = inventors.rename(columns = {'name_first': 'firstName', 'name_last': 'lastName'})

	#disInventor(cleanDataDir, inventors)
	#temp = inventor(cleanDataDir, inventors, patentMap, patentInventorMap, locations, locationMap, locInventorMap)
	inventor(cleanDataDir, inventors, patentInventorMap, locations, locationMap, locInventorMap)
	#patentInventor(cleanDataDir, temp)


def inventor(cleanDataDir, inventors, patentInventorMap, locations, locationMap, locInventorMap):
	###################### 
	# JOIN IN PATENT IDs #
	######################
	# First join in raw patent IDs...
	inventors = inventors.merge(patentInventorMap, left_on = 'id', right_on = 'inventor_id', how = 'left')
	# Rename patent_id column (patent numbers) to patentId
	inventors = inventors.rename(columns = {'patent_id': 'patentId'})
	# Then collect in-house generated patent IDs...
	#inventors = inventors.merge(patentMap, on = 'patent_id', how = 'left')

	##################### 
	# JOIN IN LOCATIONS #
	#####################
	# First join in raw location IDs...
	inventors = inventors.merge(locInventorMap, left_on = 'id', right_on = 'inventor_id', how = 'left')
	# Then collect in-house generated location IDs...
	inventors = inventors.merge(locationMap, on = 'location_id', how = 'left')
	# Finally, join in location information...
	inventors = inventors.merge(locations, on ='locationId', how = 'left') 

	# Save this to use for patentInventor
	#toReturn = temp
	temp = inventors[['inventorId', 'patentId', 'firstName', 'lastName', 'lat', 'long', 'city', 'state', 'country', 'county', 'stateFips', 'countyFips']]
	temp.to_csv(cleanDataDir + 'inventor.tsv', sep = '\t', index=False)
	#return (toReturn)

### FUNCTIONS NOT CURRENTLY USED ###

def disInventor(cleanDataDir, inventors):
	temp = inventors[['inventorDid', 'firstName', 'lastName']]
	temp = temp.drop_duplicates()
	temp.to_csv(cleanDataDir + 'disInventor.tsv', sep = '\t', index=False)

def patentInventor(cleanDataDir, temp):
	temp = temp[['patentId', 'inventorId', 'sequence', 'firstName', 'lastName', 'lat', 'long', 'city', 'state', 'country', 'state_fips', 'county_fips']]
	temp = temp.drop_duplicates()
	# Fill na values
	temp['sequence'] = temp['sequence'].fillna(-1)
	temp.to_csv(cleanDataDir + 'patentInventor.tsv', sep = '\t', index=False) 

