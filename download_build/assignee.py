from requirements import *
# uuid	patent_id	assignee_id	rawlocation_id	type	
# name_first	name_last	organization	sequence
def process_assignees(cleanDataDir, filePath_assignee, filePath_patent_assignee_map, filePath_location_assignee_map):
	assignees = pd.read_csv(filePath_assignee, sep = '\t', dtype = str)
	patentAssigneeMap = pd.read_csv(filePath_patent_assignee_map, sep = '\t', dtype = str)
	locationAssigneeMap = pd.read_csv(filePath_location_assignee_map, sep = '\t', dtype = str)
	assignees = assignees.rename(columns = {'type' : 'assigneeTypeId', 'name_first': 'firstName', 'name_last': 'lastName'})
	# Fill these fields with null so that unique IDs can be assigned correctly
	assignees[['firstName', 'lastName', 'organization']] = assignees[['firstName', 'lastName', 'organization']].fillna('-')
	assignees[['firstName', 'lastName', 'organization']] = assignees[['firstName', 'lastName', 'organization']].replace({'NULL': '-'})
	# Assign unique IDs to each assignee based on first/last name and company
	assignees['assigneeId'] = assignees.groupby(['firstName', 'lastName', 'organization']).ngroup()
	# Also create unique ID (easier to work with) for each entry in the table
	assignees['assigneeDid'] = assignees.groupby(['id']).ngroup()

	#disAssignee(cleanDataDir, assignees)
	assignee(cleanDataDir, assignees)
	patentAssignee(cleanDataDir, assignees, patentAssigneeMap, locationAssigneeMap)
	assigneeTypes(cleanDataDir)


def assignee(cleanDataDir, assignees):
	temp = assignees[['assigneeId', 'firstName', 'lastName', 'organization', 'assigneeTypeId']]
	temp = temp.drop_duplicates()
	temp.to_csv(cleanDataDir + 'assignee.tsv', sep = '\t', index=False)


def patentAssignee(cleanDataDir, assignees, patentAssigneeMap, locationAssigneeMap):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)
	locationMap = pd.read_csv(cleanDataDir + 'locationMap.tsv', sep = '\t', dtype = str)
	locations = pd.read_csv(cleanDataDir + 'location.tsv', sep = '\t', dtype = str)

	# First join in raw patent IDs...
	assignees = assignees.merge(patentAssigneeMap, left_on = 'id', right_on = 'assignee_id', how = 'left')
	# Rename patent_id column (patent numbers) to patentId
	assignees = assignees.rename(columns = {'patent_id': 'patentId'})
	# Then collect in-house generated patent IDs...
	#assignees = assignees.merge(patentMap, on = 'patent_id', how = 'left')

	# First join in raw location IDs...
	assignees = assignees.merge(locationAssigneeMap, left_on = 'id', right_on = 'assignee_id', how = 'left')
	# Then collect in-house generated location IDs...
	assignees = assignees.merge(locationMap, on = 'location_id', how = 'left')
	# Finally, bring in locations...
	assignees = assignees.merge(locations, on  = 'locationId', how = 'left')

	temp = assignees[['patentId', 'firstName', 'lastName', 'assigneeTypeId', 'assigneeId', 'city', 'state', 'country', 'lat', 'long', 'county', 'stateFips', 'countyFips', 'organization']]
	# save data
	temp.to_csv(cleanDataDir + 'patentAssignee.tsv', sep = '\t', index=False)


def assigneeTypes(cleanDataDir):
	types = {
	  1: "A 1 appearing before any code signifies part interest",
	  2: "US Company or Corporation", 
	  3: "Foreign Company or Corporation", 
	  4: "US Individual", 
	  5: "Foreign Individual",
	  6: "US Government", 
	  7: "Foreign Government", 
	  8: "Country Government", 
	  9: "State Government (US)"
	}

	# Create temp dataframe before saving to a file
	df = pd.DataFrame(list(types.items()), columns = ['assigneeTypeId', 'assigneeTypeDescription'])
	df.to_csv(cleanDataDir + 'assigneeType.tsv', sep = '\t', index=False)

### FUNCTIONS NOT CURRENTLY USED ###

def disAssignee(cleanDataDir, assignees):
	temp = assignees[['assigneeDid', 'assigneeTypeId', 'firstName', 'lastName', 'organization']]
	# Remove any duplicate rows
	temp = temp.drop_duplicates()
	temp.to_csv(cleanDataDir + 'disAssignee.tsv', sep = '\t', index=False)


