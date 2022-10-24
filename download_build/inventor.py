from requirements import *
def process_inventors(cleanDataDir, filePath_inventor, filePath_patent_inventor_map):
	# Table that maps raw patent IDs to in-house generated patent IDs
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)
	# Table that maps patent IDs to inventor IDs
	patentInventorMap = pd.read_csv(filePath_patent_inventor_map, sep = '\t', dtype = str)
	# Read in inventors
	inventors = pd.read_csv(filePath_inventor, sep = '\t',  dtype = str)
	inventors.rename(columns = {'name_first': 'first_name', 'name_last': 'last_name', 'id': 'inventor_id'}, inplace=True)
	inventors['male_flag'] = inventors['male_flag'].apply(lambda x: str(x).split('.')[0])

	inventors = inventors.merge(patentInventorMap, on='inventor_id', how='left')
	inventors.to_csv(cleanDataDir + 'inventor.tsv', sep='\t', index=False)


