from requirements import *
def process_gov_data(cleanDataDir, filePath_gov_org, filePath_govintorg, filePath_gov_interest, filePath_contract_award):
	govOrg(cleanDataDir, filePath_gov_org)

	'''
		The same operations are performed on each of the following tables, so for conciseness the code will
		only be written once with the changing parameters below. The explaination of the parameters follows:
		0) This is the name of the file to read from
		1) This is the field name that will be selected along with 'patentId' when selecting a subset of the data
		2) This is what the previous field name will be renamed to for clarity
		3) This is the name of the file that is written to disk
	'''
	toChange = [
		[filePath_govintorg, 'organization_id', 'gov_org_id', 'patentGovOrg.tsv'],
		[filePath_gov_interest, 'gi_statement', 'gov_org_statement', 'patentGovOrgStatement.tsv'],
		[filePath_contract_award, 'contract_award_number', 'contract_num', 'patentGovOrgGrant.tsv']
	]
	for params in toChange:
		createOrgTables(cleanDataDir, params)

def govOrg(cleanDataDir, filePath_gov_org):
	gov = pd.read_csv(filePath_gov_org, sep = '\t', dtype = str)
	gov = gov.rename(columns = {'organization_id': 'gov_org_id', 'name': 'gov_org_name'})
	gov.to_csv(cleanDataDir + 'govOrg.tsv', sep = '\t', index=False)

def createOrgTables(cleanDataDir, params):
	table = pd.read_csv(params[0], sep = '\t', dtype = str)
	# Add in patent IDs 
	#table = table.merge(patentMap, on = 'patent_id', how = 'inner')
	# Select subset of data
	#table = table[['patentId', params[1]]]
	# Lower text column if this is govOrgStatement
	if params[1] == 'gi_statement':
		table[params[1]] = table[params[1]].str.lower()
	# Rename organization_id field and patent_id 
	table = table.rename(columns = { params[1] : params[2]} )
	table = table.drop_duplicates()
	table.to_csv(cleanDataDir + params[3], sep = '\t', index=False)

