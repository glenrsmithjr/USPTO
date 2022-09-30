from requirements import *
def process_misc_tables(cleanDataDir, filePath_foreign_priorities, filePath_other_references, filePath_nber, filePath_nber_category, filePath_nber_subcategory):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)
	process_foreign_priorities(cleanDataDir, filePath_foreign_priorities)
	process_references(cleanDataDir, filePath_other_references)
	process_nber(cleanDataDir, filePath_nber, filePath_nber_category, filePath_nber_subcategory)

def process_foreign_priorities(cleanDataDir, filePath_foreign_priorities):
	# Read in data
	fp = pd.read_csv(filePath_foreign_priorities, sep = '\t', usecols = ['patent_id', 'sequence', 'kind', 'number', 'date', 'country_transformed'])
	fp = fp.rename(columns = {'patent_id': 'patentId', 'kind': 'priorityType', 'number': 'foreignAppNum', 'date': 'foreignAppDate', 'country_transformed': 'foreignCountry'})

	#fp = fp.merge(patentMap, left_on = 'patent_id', right_on = 'patent_id', how = 'left')
	# Collect relevant columns
	#fp = fp[['patentId', 'priorityType', 'sequence', 'foreignAppNum', 'foreignAppDate', 'foreignCountry']]
	# Remove US citations as those would be captured in usCitations.tsv
	fp = fp[~fp.foreignCountry.str.contains('US')]
	# Fill na values
	fp[['priorityType', 'foreignAppNum', 'foreignCountry']] = fp[['priorityType', 'foreignAppNum', 'foreignCountry']].fillna('-')
	fp['foreignAppDate'] = fp['foreignAppDate'].fillna('1000-01-01')
	
	fp.to_csv(cleanDataDir + 'foreignPriorities.tsv', sep = '\t', index=False)

def process_references(cleanDataDir, filePath_other_references):
	# Read in data
	refs = pd.read_csv(filePath_other_references, sep = '\t', usecols = ['patent_id', 'text', 'sequence'])
	# Rename patent_id field
	refs = refs.rename(columns = {'patent_id': 'patentId'})
	# Fill na values
	refs['sequence'] = refs['sequence'].fillna(-1)
	refs['text'] = refs['text'].fillna('-')
	#refs = refs.merge(patentMap, left_on = 'patent_id', right_on = 'patent_id', how = 'left')
	#refs = refs[['patentId', 'text', 'sequence']]
	refs.to_csv(cleanDataDir + 'otherReferences.tsv', sep = '\t', index=False)

def process_nber(cleanDataDir, filePath_nber, filePath_nber_category, filePath_nber_subcategory):
	nber = pd.read_csv(filePath_nber, sep = '\t', usecols = ['patent_id', 'category_id', 'subcategory_id'])
	nberCategory = pd.read_csv(filePath_nber_category, sep = '\t')
	nbersubCategory = pd.read_csv(filePath_nber_subcategory, sep = '\t')
	# Rename columns
	nber = nber.rename(columns = {'patent_id': 'patentId', 'category_id': 'nberCategoryId', 'subcategory_id': 'nberSubCategoryId'})
	# Add in categories
	nber = nber.merge(nberCategory, left_on = 'nberCategoryId', right_on = 'id', how = 'inner')
	nber = nber.rename(columns = {'title': 'nberCategory'})
	# Add in subcategories
	nber = nber.merge(nbersubCategory, left_on = 'nberSubCategoryId', right_on = 'id', how = 'inner')
	nber = nber.rename(columns = {'title': 'nberSubCategory'})
	# Select desired columns
	nber = nber[['patentId', 'nberCategory', 'nberSubCategory']]
	nber.to_csv(cleanDataDir + 'nber.tsv', sep = '\t', index=False)
