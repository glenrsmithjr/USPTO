from requirements import *
def process_dod_patents(rawDataDir, cleanDataDir):
	patentMap = pd.read_csv(cleanDataDir + 'patentMap.csv', dtype = str)
	patAssignee = pd.read_csv(cleanDataDir + 'patentAssignee.csv', dtype = str)
	assignee = pd.read_csv(cleanDataDir + 'assignee.csv', dtype = str)

	# temp has some relevant fields
	temp = patAssignee.merge(patentMap, on = 'patentId', how = 'inner')
	temp = temp.rename(columns = {'patent_id': 'patentNumber'})
	temp = temp[['patentId', 'patentNumber', 'assigneeId']]
	dodPatents(cleanDataDir, temp, assignee)

def dodPatents(cleanDataDir, temp, assignee):
	filters = get_filters()
	# Find all rows that contain air, navy, or army in their organization fields
	milOrgs = assignee[ assignee.organization.str.contains(filters['branches'], na=False)]
	# Then remove all rows that have engineer, medical research, or llc
	milOrgs = milOrgs[ ~milOrgs.organization.str.contains(filters['cant_have'], na=False)]
	# A list of regexes to find 
	toFind = [ filters['af_string'], filters['army_string_1'], filters['army_string_2'], filters['navy_string'] ]
	# A list of values to change matches from the toFind list to
	replaceTo = ['Air Force', 'Navy', 'Army', 'Army']

	# Only collect rows that contain the regexes we want
	temp = pd.DataFrame()
	for regx in toFind:
		print (regx)
		print (milOrgs[ milOrgs.organization.str.contains(regx, na=False)])
		temp = temp.append( milOrgs[ milOrgs.organization.str.contains(regx, na=False)] )

	temp['department'] = temp['organization'].replace(to_replace = toFind, value = replaceTo, regex = True)
	print (temp['department'])
	exit(0)
	milOrgs = milOrgs.rename(columns = {'organization': 'alias'})

	dodPatents = milOrgs.merge(temp, on = 'assigneeId', how = 'inner')
	dodPatents = dodPatents[['department', 'patentId', 'patentNumber', 'assigneeId']]
	dodPatents = dodPatents.drop_duplicates()
	dodPatents.to_csv(cleanDataDir + 'dodPatents.csv', index=False)


def get_filters():
	# Dictionary of regular expressions to filter and assign patents based on air force, army, and navy
	filters = {
		'af_string'		: '(united states|secre.*).* air',
		'army_string_1'	: '(department|united states of america|sec.* of|sceretary|dept.*|research laboratory).* army',
		'army_string_2'	: 'army .* research .*laboratory',
		'navy_string'	: 'navy',
		#'branches'		: ' air | navy | army ',
		'branches'		: 'air|navy|army',
		'cant_have'		: 'engineer|medical research|llc'
	}

	return (filters)