from requirements import *
def process_wipo(cleanDataDir, filePath_wipo, filePath_wipo_field):
	wipo = pd.read_csv(filePath_wipo, sep = '\t', dtype = str)
	wipo['field_id'] = wipo['field_id'].apply(lambda x: str(x).split('.')[0])
	wipo.to_csv(cleanDataDir + 'patentWipo.tsv', sep='\t', index=False)

	wipoField(cleanDataDir, filePath_wipo_field)

def wipoField(cleanDataDir, filePath_wipo_field):
	wipo_field = pd.read_csv(filePath_wipo_field, sep = '\t', dtype = str)
	wipo_field = wipo_field.rename(columns = {'id': 'field_id', 'sector_title': 'sector_description',
											  'field_title': 'field_description'})
	wipo_field.to_csv(cleanDataDir + 'wipoField.tsv', sep ='\t', index=False)