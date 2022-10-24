from requirements import *
def process_misc_tables(cleanDataDir, filePath_foreign_priorities, filePath_other_references, filePath_nber, filePath_nber_category, filePath_nber_subcategory):
	process_foreign_priorities(cleanDataDir, filePath_foreign_priorities)
	process_references(cleanDataDir, filePath_other_references)
	process_nber(cleanDataDir, filePath_nber, filePath_nber_category, filePath_nber_subcategory)

def process_foreign_priorities(cleanDataDir, filePath_foreign_priorities):
	# Read in data
	fp = pd.read_csv(filePath_foreign_priorities, sep = '\t')
	fp = fp.rename(columns = {'kind': 'priority_type', 'number': 'foreign_app_num', 'date': 'foreign_app_date',
							  'country_transformed': 'foreign_country'})

	#fp = fp.merge(patentMap, left_on = 'patent_id', right_on = 'patent_id', how = 'left')
	# Collect relevant columns
	#fp = fp[['patentId', 'priorityType', 'sequence', 'foreignAppNum', 'foreignAppDate', 'foreignCountry']]
	# Remove US citations as those would be captured in usCitations.tsv
	fp = fp[~fp.foreign_country.str.contains('US')]
	fp.to_csv(cleanDataDir + 'foreignPriorities.tsv', sep = '\t', index=False)

def process_references(cleanDataDir, filePath_other_references):
	# Read in data
	refs = pd.read_csv(filePath_other_references, sep = '\t')
	refs.to_csv(cleanDataDir + 'otherReferences.tsv', sep = '\t', index=False)

def process_nber(cleanDataDir, filePath_nber, filePath_nber_category, filePath_nber_subcategory):
	nber = pd.read_csv(filePath_nber, sep = '\t')
	nberCategory = pd.read_csv(filePath_nber_category, sep = '\t')
	nbersubCategory = pd.read_csv(filePath_nber_subcategory, sep = '\t')
	# Rename columns
	nber = nber.rename(columns = {'category_id': 'nber_category_id', 'subcategory_id': 'nber_subcategory_id'})
	# Add in categories
	nber = nber.merge(nberCategory, left_on = 'nber_category_id', right_on = 'id', how = 'left')
	nber = nber.rename(columns = {'title': 'nber_category'})
	# Add in subcategories
	nber = nber.merge(nbersubCategory, left_on = 'nber_subcategory_id', right_on = 'id', how = 'left')
	nber = nber.rename(columns = {'title': 'nber_subcategory'})
	nber.to_csv(cleanDataDir + 'nber.tsv', sep = '\t', index=False)
