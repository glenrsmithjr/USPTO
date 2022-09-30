from requirements import *
# Main function that processes tables: patents, patentAbstracts, and patentTypes
def process_patents(cleanDataDir, filePath_patent, filePath_application):
	# Read in patents and rename columns
	patent_fields = ['id', 'type', 'number', 'date', 'abstract', 'title', 'kind', 'num_claims']
	patent = pd.read_csv(filePath_patent, sep = '\t', usecols = patent_fields, dtype = str)
	patent = patent.rename(columns = {'number': 'patentId', 'type': 'patentType', 'date': 'grantDate', 'kind': 'patentKind', 'num_claims': 'numClaims'})
	# Create new unique ID for each patent uuid
	#patent['patentId'] = patent.groupby(['patent_id']).ngroup()
	# I CURRENTLY DO NOT FILTER PATENT NUMBER LIKE THE R VERSION: filter(nchar(number) > 3, nchar(number) <= 10)
	# Patent types are converted to numbers so must pass patents to patentTypes first
	patent = patentTypes(cleanDataDir, patent)
	#patentMap(cleanDataDir, patent)
	abstracts = patents(cleanDataDir, filePath_application, patent)
	patentAbstracts(cleanDataDir, abstracts)

def patentMap(cleanDataDir, patents):
	temp = patents[['patentId', 'patent_id']]
	temp.to_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', index=False)

# Collects information from the patent application data and adds it to the patent table 
def patents(cleanDataDir, filePath_application, patents):
	app_fields = ['patent_id', 'date']
	apps = pd.read_csv(filePath_application, sep = '\t', usecols = app_fields, dtype = str)
	apps.columns = ['patentId', 'appFileDate']
	apps = apps.drop_duplicates()

	'''
		patentNumber (from patent) and patent_id (from application) are the same values, 
		so do a join on patentNumber (NOT patentId) because patentId was uniquely generated 
		for the 'patent' table. patentNumber in the 'patent' table corresponds to the raw
		patent_id field in the 'application' table (that's why it's renamed to patentNumber
		above)
	'''
	patents = patents.merge(apps, on = 'patentId', how = 'left')
	#patents = patents[['patentId', 'patentNumber', 'grantDate', 'appFileDate', 'title', 'patentKind', 'numClaims', 'patentTypeId', 'abstract']]
	patents = patents[['patentId', 'grantDate', 'appFileDate', 'title', 'patentKind', 'numClaims', 'patentTypeId', 'abstract']]
	patents = patents.drop_duplicates()
	# Join in patent types
	# This will be used to create the patentAbstracts table
	abstracts = patents[['patentId', 'abstract']]
	# Remove abstract field from this column
	patents = patents.drop(columns = ['abstract'])
	# Rename title column
	patents = patents.rename(columns = {'title': 'patentTitle'})
	# Fill na values
	patents['numClaims'] = patents['numClaims'].fillna(-1)
	patents[['grantDate', 'appFileDate']] = patents[['grantDate', 'appFileDate']].fillna('1000-01-01')
	patents[['patentTitle', 'patentKind']] = patents[['patentTitle', 'patentKind']].fillna('-')
	# CURRENTLY DO NOT FILTER PATENT LIKE R VERSION: filter(patentNumber != "b1", patentId != 0)
	# DO NOT NEED TO REMOVE PATENTID = 0
	# Create grant year
	patents['grantYear'] = pd.to_datetime(patents['grantDate'], yearfirst=True, errors='coerce').dt.year
	patents.to_csv(cleanDataDir + 'patent.tsv', sep = '\t', index=False)
	return (abstracts)

# Extracts abstracts from patent table and puts them in their own table
def patentAbstracts(cleanDataDir, abstracts):
	# Rename abstract field
	abstracts = abstracts.rename(columns = {'abstract': 'patentAbstract'})
	# Fill na values
	abstracts['patentAbstract'] = abstracts['patentAbstract'].fillna('-') 
	# Write the patentId and Abstract to create the patentAbstract table
	abstracts.to_csv(cleanDataDir + 'patentAbstract.tsv', sep = '\t', index=False)

# Defines number codes for all known patent types
def patentTypes(cleanDataDir, patents):
	# Types are known and hardcoded. Would have to be updated if these change.
	types = {'defensive publication' : 1, 'design': 2, 'plant': 3, 'reissue': 4, 
			'statutory invention registration': 5, 'tvpp': 6, 'utility': 7, 'other': 8}
	# Replace types with number codes and return 
	patents['patentTypeId'] = patents['patentType'].replace(types)
	# For all other types, replace with number 8
	codes = types.values()
	patents['patentTypeId'] = [8 if val not in codes else val for val in patents['patentTypeId']]
	# Create temp dataframe before saving to a file
	df = pd.DataFrame(list(types.items()), columns = ['patentTypeDescription', 'patentTypeId'])
	df.to_csv(cleanDataDir + 'patentType.tsv', sep = '\t', index=False)
	return (patents)