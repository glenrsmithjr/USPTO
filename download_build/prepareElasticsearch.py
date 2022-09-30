from requirements import *
import esUtils

def _get_gov_org_data(cleanDataDir):
	# Read in gov orgs table
	govOrgs = pd.read_csv(cleanDataDir + 'govOrg.tsv', sep = '\t', dtype = {'govOrgId': str})
	# Read in table that maps patent IDs to gov org IDs
	mapTable = pd.read_csv(cleanDataDir + 'patentGovOrg.tsv', sep = '\t', dtype = {'patentId': int, 'govOrgId': str})
	# Merge tables to get one-to-many relationship b/w gov orgs and patents
	govOrgs = govOrgs.merge(mapTable, on = 'govOrgId', how = 'left')
	# Rename columns for use in elasticsearch
	govOrgs = govOrgs.rename(columns = esUtils._get_renamed_columns('govOrgs'))
	# Replace empty Patent IDs with -1 value
	# This number does not matter since a left join on the patents table will filter out those rows
	# These rows are patents that don't have a gov org recognized in the dataset
	govOrgs.patentId = govOrgs.patentId.fillna(-1).astype(int)
	# Group by patent IDs to collect all gov orgs associated with each patent
	patentGroups = govOrgs.groupby('patentId')

	return _map_ids_to_dicts(patentGroups, 'govOrgs', ['patentId', 'govInterests'])

def _get_gov_grants_data(cleanDataDir):
	grants = pd.read_csv(cleanDataDir + 'patentGovOrgGrant.tsv', sep = '\t', dtype={'contractNum': str})
	patentGroups = grants.groupby('patentId')
	return _map_ids_to_list(patentGroups, 'contractNum', ['patentId', 'contractNumbers'])	

def _get_citation_data(cleanDataDir, country_origin):
	if country_origin == 'us':
		cites = pd.read_csv(cleanDataDir + 'usCitations.tsv', sep = '\t')
	else:
		cites = pd.read_csv(cleanDataDir + 'foreignCitations.tsv', sep = '\t')
	# US PatentIds are labeled as outgoingCitationId in these citation tables
	patentGroups = cites.groupby('outgoingCitationId')
	# PatentIds that are being cited are labeled as incomingCitationId in this table
	if country_origin == 'us':
		return _map_ids_to_list(patentGroups, 'incomingCitationId', ['patentId', 'citationsUS'])
	else:
		return _map_ids_to_list(patentGroups, 'incomingCitationId', ['patentId', 'citationsForeign'])

def _get_inventor_data(cleanDataDir):
	# Read in inventors table
	#inventorNames = pd.read_csv(cleanDataDir + 'inventor.tsv', sep = '\t')
	# Read in table that maps patent IDs to inventor IDs
	inventors = pd.read_csv(cleanDataDir + 'patentInventor.tsv', sep = '\t')
	# Locations
	#locs = pd.read_csv(cleanDataDir + 'disLocation.tsv', sep = '\t')
	# Merge table to get one-to-many relationship b/w patents and inventors
	#inventors = mapTable.merge(inventorNames, on = 'inventorId', how = 'left')
	# Join in location information
	#inventors = inventors.merge(locs, on = 'locationDid', how = 'left')
	# Remove unnecessary columns
	inventors = inventors[['patentId', 'sequence', 'firstName', 'lastName', 'lat', 'long', 'city', 'state', 'country']]
	# Group by patent IDs to collect all inventors associated with each patent
	patentGroups = inventors.groupby('patentId')

	return _map_ids_to_dicts(patentGroups, 'inventors', ['patentId', 'inventors'])

def _get_wipo_data(cleanDataDir):
	patentWipo = pd.read_csv(cleanDataDir + 'patentWipo.tsv', sep = '\t')	
	patentGroups = patentWipo.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'wipo', ['patentId', 'wipoFields'])
	# MAYBE JOIN IN WIPO FIELD DESCRIPTIONS

def _get_cpc_data(cleanDataDir):
	patentCPC = pd.read_csv(cleanDataDir + 'patentCPC.tsv', sep = '\t')	
	patentGroups = patentCPC.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'patent_cpc', ['patentId', 'cpc'])

def _get_ipc_data(cleanDataDir):
	ipc = pd.read_csv(cleanDataDir + 'patentIPC.tsv', sep = '\t', parse_dates=['actionDate', 'ipcVersion'])	
	# Create string date columns
	ipc['actionDateString'] = ipc['actionDate'].astype(str)
	ipc['ipcVersionString'] = ipc['ipcVersion'].astype(str)

	patentGroups = patentCPC.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'patent_ipc', ['patentId', 'ipc'])

def _get_claim_data(cleanDataDir):
	claims = pd.read_csv(cleanDataDir + 'claims.tsv', sep = '\t', usecols = ['patentId', 'dependent', 'sequence', 'exemplary'])
	patentGroups = claims.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'claims', ['patentId', 'claims'])

def _get_foreign_priority_data(cleanDataDir):
	fp = pd.read_csv(cleanDataDir + 'foreignPriorities.tsv', sep = '\t', parse_dates = ['foreignAppDate'])
	# Create string date column
	fp['foreignAppDateString'] = fp['foreignAppDate'].astype(str)
	patentGroups = fp.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'foreign_priority', ['patentId', 'foreignPriorities'])

def _get_other_ref_data(cleanDataDir):
	of = pd.read_csv(cleanDataDir + 'otherReferences.tsv', sep = '\t')
	patentGroups = fp.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'other_references', ['patentId', 'otherReferences'])	

def _get_assignee_data(cleanDataDir):
	assignees = pd.read_csv(cleanDataDir + 'patentAssignee.tsv', sep = '\t')
	mapTable = pd.read_csv(cleanDataDir + 'disAssignee.tsv', sep = '\t')
	assignees = assignees.merge(mapTable, on ='assigneeDid', how = 'left')

	patentGroups = assignees.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'assignees', ['patentId', 'assignees'])

def _get_nber_data(cleanDataDir):
	nber = pd.read_csv(cleanDataDir + 'nber.tsv', sep = '\t')
	patentGroups = assignees.groupby('patentId')
	return _map_ids_to_dicts(patentGroups, 'nber', ['patentId', 'nber'])


######################
# ADD DATA FUNCTIONS #
######################
# Final column set: 
# patentId, patentNumber, grantDate, appFileDate, patentTitle, patentKind, numClaims, patentTypeId, incomingCitationsUS, outgoingCitationsUS, 
# outgoingCitationsForeign, abstract, appId, seriesCode, appNum,
# 
# 
##### COMBINE BELOW FUNCTIONS WHEN COLUMNS RENAMED DURING PROCESSING #####

def _add_data(patents, data, on = 'patentId', how = 'left'):
	return patents.merge(data, on = on, how = how)

# This helper function takes a dataframe that has been grouped on a list of IDs
# and maps those IDs to a dictionary of the data they relate to. This structure
# is needed to index the data into elasticsearch
def _map_ids_to_dicts(groups, desired_column_lookup, column_mappings):

	allData = []
	for uuid, data in groups:
		# Collect desired columns from data
		data = data[esUtils._get_desired_columns(desired_column_lookup)]
		# Add patent ID so records can be split into multiple tables when data is pulled
		data['patentId'] = uuid
		# Convert dataframe to dict
		xDict = data.to_dict(orient='records')
		# Save the patent ID and corresponding dictionary of orgs data
		allData.append([uuid, xDict])
	# Convert to dataframe
	allData = pd.DataFrame(allData, columns = column_mappings)
	return (allData)
# This is a special helper function that take a grouped dataframe and maps each group id
# to a list created from one column of the dataframe. The assumption is that there is only
# one column in the grouped dataframe
def _map_ids_to_list(groups, list_column, column_mappings):
	allData = []
	for uuid, data in groups:
		# Collect desired columns from data
		data = list(data[list_column])
		# Save the patent ID and corresponding list
		allData.append([uuid, data])
	# Convert to dataframe
	allData = pd.DataFrame(allData, columns = column_mappings)
	return (allData)

# Read in inventors table
	inventors = pd.read_csv('inventor.csv')
	# Read in table that maps patent IDs to inventor IDs
	mapTable = pd.read_csv('patentInventor.csv')
	# Locations
	locs = pd.read_csv('disLocation.csv')
	# Merge table to get one-to-many relationship b/w patents and inventors
	inventors = inventors.merge(mapTable, on = 'inventorId', how = 'left')
	# Join in location information
	inventors = inventors.merge(locs, on = 'locationDid', how = 'left')
	# Remove unnecessary columns
	inventors = inventors[['patentId', 'sequence', 'inventorId', 'locationDid', 'firstName', 'lastName', 'lat', 'long', 'city', 'state', 'country']]
	# Group by patent IDs to collect all inventors associated with each patent
	patentGroups = inventors.groupby('patentId')

	return _map_ids_to_dicts(patentGroups, 'inventors', ['patentId', 'inventors'])