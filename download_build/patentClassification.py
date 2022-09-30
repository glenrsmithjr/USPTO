from requirements import *
def process_classifications(cleanDataDir, filePath_cpc_current, filePath_ipcr, filePath_cpc_subsection, filePath_cpc_group, filePath_cpc_subgroup):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)

	cpcCurrent = pd.read_csv(filePath_cpc_current, sep = '\t', dtype = str)
	cpcCurrent = cpcCurrent.rename(columns = {'section_id': 'cpcSectionId', 'subsection_id': 'cpcSubSectionId',
											  'group_id': 'cpcGroupId', 'subgroup_id': 'cpcSubGroupId', 'patent_id': 'patentId'})
	# Only want the part before the '/' (ex: A63B71/146)
	cpcCurrent['subGroupPrime'] = cpcCurrent['cpcSubGroupId'].str.split('/', n = 1, expand = True)[0]

	patentCPC(cleanDataDir, cpcCurrent)
	CPCsection(cleanDataDir)
	ipc(cleanDataDir, filePath_ipcr)

	'''
		The same operations are performed on each of the following tables, so for conciseness the code will
		only be written once with the changing parameters below. The explaination of the parameters follows:
		0) This is the name of the file to read from
		1) This is what the 'id' field will be renamed to
		2) This is what the 'title' field will be renamed to
		3) This is the name of the file that is written to disk
	'''
	toChange = [
		[filePath_cpc_subsection, 'cpcSubSectionId', 'cpcSubSectionDescription', 'CPCsubSection.tsv'],
		[filePath_cpc_group, 'cpcGroupId', 'cpcGroupDescription', 'CPCgroup.tsv'],
		[filePath_cpc_subgroup, 'cpcSubGroupId', 'cpcSubGroupDescription', 'CPCsubGroup.tsv']
	]
	for params in toChange:
		createCPCTables(cleanDataDir, params)

def patentCPC(cleanDataDir, cpcCurrent):
	#temp = cpcCurrent.merge(patentMap, on = 'patent_id', how = 'inner')
	# Fill na values
	cpcCurrent['sequence'] = cpcCurrent['sequence'].fillna(-1)
	cpcCurrent[['cpcSectionId', 'cpcSubSectionId', 'cpcGroupId', 'cpcSubGroupId', 'subGroupPrime', 'category']] = cpcCurrent[['cpcSectionId', 'cpcSubSectionId', 'cpcGroupId', 'cpcSubGroupId', 'subGroupPrime', 'category']].fillna('-')
	cpcCurrent.to_csv(cleanDataDir + 'patentCPC.tsv', sep = '\t', index=False)

# Not created from any specific table
def CPCsection(cleanDataDir):
	sections =  {
		"A": "Human Necessitites", 
		"B": "Performing Operations; Transporting", 
		"C": "Chemistry; Metallurgy",
		"D": "Textiles; Paper", 
		"E": "Fixed Constructions", 
		"F": "Mechanical Engineering; Lighting; Heating; Weapons; Blasting Engines or Pumps",
		"G": "Physics", 
		"H": "Electricity", 
		"Y": "General Tagging of New Technological Developments" }

	df = pd.DataFrame(list(sections.items()), columns = ['cpcSectionId', 'cpcSectionDescription'])
	df.to_csv(cleanDataDir + 'CPCsection.tsv', sep = '\t', index=False)

def ipc(cleanDataDir, filePath_ipcr):
	ipc = pd.read_csv(filePath_ipcr, sep = '\t', dtype = str)
	ipc = ipc.rename(columns = {'section': 'ipcSectionId', 'ipc_class': 'ipcClassId',
		  'subclass': 'ipcSubClassId', 'main_group': 'ipcGroupId', 'subgroup': 'ipcSubGroupId',
		  'symbol_position': 'symbolPosition', 'classification_value': 'classValue',
		  'classification_status': 'classStatus', 'classification_data_source': 'dataSource',
		  'action_date': 'actionDate', 'ipc_version_indicator': 'ipcVersion', 'patent_id': 'patentId'})

	#temp = ipc.merge(patentMap, on = 'patent_id', how = 'inner')
	ipc = ipc[['patentId', 'sequence', 'ipcSectionId', 'ipcClassId', 'ipcSubClassId',
				 'ipcGroupId', 'ipcSubGroupId', 'symbolPosition', 'classValue', 'classStatus',
				 'dataSource', 'actionDate', 'ipcVersion']]
	# Fill na values
	ipc['sequence'] = ipc['sequence'].fillna(-1)
	ipc[['actionDate', 'ipcVersion']] = ipc[['actionDate', 'ipcVersion']].fillna('1000-01-01')
	ipc[['ipcSectionId','ipcClassId','ipcSubClassId','ipcGroupId','ipcSubGroupId','symbolPosition','classValue','classStatus','dataSource']] = ipc[['ipcSectionId','ipcClassId','ipcSubClassId','ipcGroupId','ipcSubGroupId','symbolPosition','classValue','classStatus','dataSource']].fillna('-')
	ipc.to_csv(cleanDataDir + 'patentIPC.tsv', sep = '\t', index=False)

# Created from 'cpc_subsection'
def createCPCTables(cleanDataDir, params):
	# Read in data
	temp = pd.read_csv(params[0], sep = '\t', dtype = str)
	# Lowercase all titles 
	temp['title'] = temp['title'].str.lower()
	# Rename columns
	temp = temp.rename(columns = {'id': params[1], 'title': params[2]})
	# Fill na values
	temp[params[2]] = temp[params[2]].fillna('-')
	# Save data
	temp.to_csv(cleanDataDir + params[3], sep = '\t', index=False)


