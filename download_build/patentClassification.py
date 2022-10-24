from requirements import *
def process_classifications(cleanDataDir, filePath_cpc_current, filePath_ipcr, filePath_cpc_subsection, filePath_cpc_group, filePath_cpc_subgroup):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)

	cpcCurrent = pd.read_csv(filePath_cpc_current, sep = '\t', dtype = str)
	cpcCurrent = cpcCurrent.rename(columns = {'section_id': 'cpc_section_id', 'subsection_id': 'cpc_subsection_id',
											  'group_id': 'cpc_group_id', 'subgroup_id': 'cpc_subgroup_id'})
	# Only want the part before the '/' (ex: A63B71/146)
	cpcCurrent['sub_group_prime'] = cpcCurrent['cpc_subgroup_id'].str.split('/', n = 1, expand = True)[0]
	cpcCurrent.to_csv(cleanDataDir + 'patentCPC.tsv', sep='\t', index=False)

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
		[filePath_cpc_subsection, 'cpc_subsection_id', 'cpc_subsection_description', 'CPCsubSection.tsv'],
		[filePath_cpc_group, 'cpc_group_id', 'cpc_group_description', 'CPCgroup.tsv'],
		[filePath_cpc_subgroup, 'cpc_subgroup_id', 'cpc_subgroup_description', 'CPCsubGroup.tsv']
	]
	for params in toChange:
		createCPCTables(cleanDataDir, params)

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

	df = pd.DataFrame(list(sections.items()), columns = ['cpc_section_id', 'cpc_section_description'])
	df.to_csv(cleanDataDir + 'CPCsection.tsv', sep = '\t', index=False)

def ipc(cleanDataDir, filePath_ipcr):
	ipc = pd.read_csv(filePath_ipcr, sep = '\t', dtype = str)
	ipc = ipc.rename(columns = {'section': 'ipc_section_id', 'ipc_class': 'ipc_class_id',
		  						'subclass': 'ipc_sub_class_id', 'main_group': 'ipc_group_id',
								'subgroup': 'ipc_subgroup_id', 'ipc_version_indicator': 'ipc_version'})

	ipc.to_csv(cleanDataDir + 'patentIPC.tsv', sep = '\t', index=False)

# Created from 'cpc_subsection'
def createCPCTables(cleanDataDir, params):
	# Read in data
	temp = pd.read_csv(params[0], sep = '\t', dtype = str)
	# Lowercase all titles 
	temp['title'] = temp['title'].str.lower()
	# Rename columns
	temp = temp.rename(columns = {'id': params[1], 'title': params[2]})
	# Save data
	temp.to_csv(cleanDataDir + params[3], sep = '\t', index=False)


