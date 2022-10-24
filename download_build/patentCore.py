from requirements import *
# Main function that processes tables: patents, patentAbstracts, and patentTypes
def process_patents(cleanDataDir, filePath_patent):
	# Read in patents and rename columns
	patent_fields = ['id', 'type', 'number', 'date', 'abstract', 'title', 'kind', 'num_claims']
	patent = pd.read_csv(filePath_patent, sep = '\t', usecols = patent_fields, dtype = str)
	patent = patent.rename(columns = {'id': 'patent_id', 'number': 'patent_number', 'type': 'patent_type',
									  'date': 'grant_date', 'kind': 'patent_kind', 'title': 'patent_title'})

	# Patent types are converted to numbers so must pass patents to patentTypes first
	patent = patentTypes(cleanDataDir, patent)
	abstracts = patents(cleanDataDir, patent)
	patentAbstracts(cleanDataDir, abstracts)


# Collects information from the patent application data and adds it to the patent table 
def patents(cleanDataDir, patents):
	app_fields = ['patent_id', 'app_file_date']
	apps = pd.read_csv(cleanDataDir + 'application.tsv', sep = '\t', usecols = app_fields, dtype = str)
	apps.drop_duplicates(inplace=True)

	'''
		patentNumber (from patent) and patent_id (from application) are the same values, 
		so do a join on patentNumber (NOT patentId) because patentId was uniquely generated 
		for the 'patent' table. patentNumber in the 'patent' table corresponds to the raw
		patent_id field in the 'application' table (that's why it's renamed to patentNumber
		above)
	'''
	patents = patents.merge(apps, on = 'patent_id', how = 'left')
	# Join in patent types
	# This will be used to create the patentAbstracts table
	abstracts = patents[['patent_id', 'abstract']]
	# Remove abstract field from this column
	patents = patents.drop(columns = ['abstract'])
	patents['grant_year'] = pd.to_datetime(patents['grant_date'], yearfirst=True, errors='coerce').dt.year
	patents.to_csv(cleanDataDir + 'patent.tsv', sep = '\t', index=False)
	return abstracts

# Extracts abstracts from patent table and puts them in their own table
def patentAbstracts(cleanDataDir, abstracts):
	# Write the patentId and Abstract to create the patentAbstract table
	abstracts.to_csv(cleanDataDir + 'patentAbstract.tsv', sep = '\t', index=False)

# Defines number codes for all known patent types
def patentTypes(cleanDataDir, patents):
	# Types are known and hardcoded. Would have to be updated if these change.
	types = {'defensive publication' : 1, 'design': 2, 'plant': 3, 'reissue': 4, 
			'statutory invention registration': 5, 'tvpp': 6, 'utility': 7, 'other': 8}
	# Replace types with number codes and return 
	patents['patent_type_id'] = patents['patent_type'].replace(types)
	# For all other types, replace with number 8
	codes = types.values()
	patents['patent_type_id'] = [8 if val not in codes else val for val in patents['patent_type_id']]
	# Create temp dataframe before saving to a file
	df = pd.DataFrame(list(types.items()), columns = ['patent_type_description', 'patent_type_id'])
	df.to_csv(cleanDataDir + 'patentType.tsv', sep = '\t', index=False)
	return patents