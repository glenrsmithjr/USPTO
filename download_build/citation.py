from requirements import *
# NOTE: Patents table will be read in twice in the below code, but this is to use the least amount of memory
def process_citations(cleanDataDir, filePath_uspatentcitation, filePath_foreigncitation):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype={'patent_id':str, 'patentId':int})

	'''
		Because the following code will be executed multiple times with different parameters, the following lists were created
		to iterate over. Each internal list has the following parameter values:
		1. the filepath to the raw data file to read in
		2. The list of keep when reading in the file
		3. A dictionary mapping of provided field names to desired field names
		4. The name of the output file containing the processed data
		5. A marker to decide whether to process US citations or foreign citations
	'''
	toChange = [
		[filePath_uspatentcitation, ['patent_id', 'citation_id', 'category', 'sequence', 'name', 'kind'], {'patent_id': 'outgoingCitationId', 'citation_id': 'incomingCitationId'}, 'usCitations.tsv', 'us'],
		[filePath_foreigncitation, ['patent_id', 'number', 'category', 'sequence', 'country'], {'patent_id': 'outgoingCitationId', 'number': 'incomingCitationId'}, 'foreignCitations.tsv', 'foreign']
	]

	for params in toChange:
		# Read in citations table and rename columns
		table = pd.read_csv(params[0], sep = '\t', usecols = params[1])
		table = table.rename(columns = params[2])
		# Fill na values
		table['sequence'] = table['sequence'].fillna(-1)
		# The US and foreign citation tables have slightly different processing from this point on
		if params[-1] == 'us':
			us = createCitationTable(cleanDataDir, table, params[3], 'us')
			getNumCitationsUS(cleanDataDir, us)
		else:
			foreign = createCitationTable(cleanDataDir, table, params[3], 'foreign')
			getNumCitationsForeign(cleanDataDir, foreign)

	patentCitationCategory(cleanDataDir)
	
# Replaces source-generated patent IDs with in-house-generated patent IDs
def createCitationTable(cleanDataDir, citations, tableName, usForeignMarker):
	citations = citations.drop_duplicates()
	# Rename these columns depending on which table is present
	if usForeignMarker == 'us':
		citations = citations.rename(columns = {'name': 'incomingCitationName', 'kind': 'incomingCitationKind'})
		# Select final set of fields
		citations = citations[['outgoingCitationId', 'sequence', 'incomingCitationId', 'category', 'incomingCitationName', 'incomingCitationKind']]
	else:
		citations = citations[['outgoingCitationId', 'sequence', 'incomingCitationId', 'category', 'country']]
	# Merge and rename patentId to not confuse it with next merge
	#temp = pd.merge(temp, patentMap, left_on = 'citingPatentId', right_on = 'patent_id', how = 'left')
	#temp = temp.rename(columns = {'patentId': 'outgoingCitationId'})
	#temp = pd.merge(temp, patentMap, left_on = 'citedPatentId', right_on = 'patent_id', how = 'left')
	#temp = temp.rename(columns = {'patentId': 'incomingCitationId'})
	# Sort patentMap by citing patent ID
	#patentMap = patentMap.sort_values('patentId')
	#temp['outgoingCitationId'] = temp['citingPatentId'].apply(lambda x: patentMap['patentId'][patentMap['patent_id'] == x].values[0])
	#temp['incomingCitationId'] = temp['citedPatentId'].apply(lambda x: patentMap['patentId'][patentMap['patent_id'] == x].values[0])
	# Create category IDs based on words in categories
	# findRepCats (find replace categories) is used to find the categories and replace categories by 
	# words they contain
	#findRepCats = {'other': 1, 'exam': 2, 'applicant': 3, 'third': 4}

	# Solution for the following found here: https://stackoverflow.com/questions/43905930/conditional-if-statement-if-value-in-row-contains-string-set-another-column
	citations['category'] = citations['category'].fillna('0')
	citations['categoryId'] = pd.np.where(citations.category.str.contains('0'), 0,
						 pd.np.where(citations.category.str.contains('other'), 1,
						 pd.np.where(citations.category.str.contains('exam'), 2,
						 pd.np.where(citations.category.str.contains('applicant'), 3,
						 pd.np.where(citations.category.str.contains('third'), 4, 0)))))
	citations = citations.drop(columns = ['category'])
	
	
	citations.to_csv(cleanDataDir + tableName, sep = '\t', index=False)
	# Only need to return these two columns for use later
	return (citations[['outgoingCitationId', 'incomingCitationId']])

def getNumCitationsUS(cleanDataDir, us_citations):
	# Calculate # of outgoing citations to US patents
	us_us_outgoing_cits = us_citations.groupby('outgoingCitationId').size().reset_index(name='outgoingCitationsUS')
	us_us_outgoing_cits['outgoingCitationsUS'] = us_us_outgoing_cits['outgoingCitationsUS'].fillna(0) 
	#print ('us_us_outgoing_cits SIZE: ' + str(us_us_outgoing_cits.shape[0]))
	#print ('unique patents: ' + str(len(list(us_us_outgoing_cits['outgoingCitationId'].unique()))))
	# Calculate # of incoming citations by US patents
	us_us_incoming_cits = us_citations.groupby('incomingCitationId').size().reset_index(name='incomingCitationsUS')
	us_us_incoming_cits['incomingCitationsUS'] = us_us_incoming_cits['incomingCitationsUS'].fillna(0)
	#print ('us_us_incoming_cits SIZE: ' + str(us_us_incoming_cits.shape[0]))
	#print ('unique patents: ' + str(len(list(us_us_outgoing_cits['outgoingCitationId'].unique()))))
	patents = pd.read_csv(cleanDataDir + 'patent.tsv', sep = '\t')
	# Join all data into patents table
	patents = patents.merge(us_us_outgoing_cits, left_on = 'patentId', right_on = 'outgoingCitationId', how = 'left')
	patents = patents.merge(us_us_incoming_cits, left_on = 'patentId', right_on = 'incomingCitationId', how = 'left')
	# Remove irrelevant columns
	patents = patents.drop(columns = ['outgoingCitationId', 'incomingCitationId'])
	# Save patents table
	patents.to_csv(cleanDataDir + 'patent.tsv', sep = '\t', index=False)

def getNumCitationsForeign(cleanDataDir, foreign_citations):
	# Calculate # of outgoing citations to foreign patents
	us_foreign_outgoing_cits = foreign_citations.groupby('outgoingCitationId').size().reset_index(name='outgoingCitationsForeign')

	patents = pd.read_csv(cleanDataDir + 'patent.tsv', sep = '\t')
	# Join all data into patents table
	patents = patents.merge(us_foreign_outgoing_cits, left_on = 'patentId', right_on = 'outgoingCitationId', how = 'left')
	# Remove irrelevant columns
	patents = patents.drop(columns = ['outgoingCitationId'])
	# Save patents table
	patents.to_csv(cleanDataDir + 'patent.tsv', sep = '\t', index=False)

def patentCitationCategory(cleanDataDir):
	categories = {1: 'cited by other', 2: 'cited by examiner',
				  3: 'cited by applicant', 4: 'cited by third party'}
	df = pd.DataFrame(list(categories.items()), columns = ['citationCatId', 'citationCatDesc'])
	df.to_csv(cleanDataDir + 'patentCitationCategory.tsv', sep = '\t', index=False)