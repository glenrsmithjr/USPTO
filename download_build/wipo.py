from requirements import *
def process_wipo(cleanDataDir, filePath_wipo, filePath_wipo_field):
	wipo = pd.read_csv(filePath_wipo, sep = '\t', dtype = str)
	wipo = wipo.rename(columns = {'field_id': 'fieldId'})

	patentWipo(cleanDataDir, wipo)
	wipoField(cleanDataDir, filePath_wipo_field)

def patentWipo(cleanDataDir, wipo):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)
	#temp = patentMap.merge(wipo, on = 'patent_id', how = 'inner')
	#temp = temp[['patentId', 'sequence', 'fieldId']]
	wipo = wipo.rename(columns = {'patent_id': 'patentId'})
	# Fill na values
	wipo[['sequence', 'fieldId']] = wipo[['sequence', 'fieldId']].fillna(-1)
	wipo.to_csv(cleanDataDir + 'patentWipo.tsv', sep = '\t', index=False)

def wipoField(cleanDataDir, filePath_wipo_field):
	wipo_field = pd.read_csv(filePath_wipo_field, sep = '\t', dtype = str)
	wipo_field = wipo_field.rename(columns = {'id': 'fieldId', 'sector_title': 'sectorDescription', 'field_title': 'fieldDescription'})
	# Fill na values
	wipo_field[['sectorDescription', 'fieldDescription']] = wipo_field[['sectorDescription', 'fieldDescription']].fillna('-')
	wipo_field.to_csv(cleanDataDir + 'wipoField.tsv', sep ='\t', index=False)