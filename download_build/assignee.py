from requirements import *
# uuid	patent_id	assignee_id	rawlocation_id	type	
# name_first	name_last	organization	sequence
def process_assignees(cleanDataDir, filePath_assignee, filePath_patent_map):
	assignees = pd.read_csv(filePath_assignee, sep = '\t', dtype = str)
	patent_map = pd.read_csv(filePath_patent_map, sep = '\t', dtype = str)

	assignees = assignees.rename(columns = {'type' : 'assignee_type_id', 'name_first': 'first_name',
											'name_last': 'last_name', "rawlocation_id": "location_id"})
	# Fill these fields with null so that unique IDs can be assigned correctly
	assignees.replace({"": None}, inplace=True)
	assignees['assignee_type_id'] = assignees['assignee_type_id'].apply(lambda x: str(x).split('.')[0])
	assignees.to_csv(cleanDataDir + 'assignee.tsv', sep='\t', index=False)
	assigneeTypes(cleanDataDir)


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
	df = pd.DataFrame(list(types.items()), columns = ['assignee_type_id', 'assignee_type_description'])
	df.to_csv(cleanDataDir + 'assigneeType.tsv', sep = '\t', index=False)

### FUNCTIONS NOT CURRENTLY USED ###

def disAssignee(cleanDataDir, assignees):
	temp = assignees[['assigneeDid', 'assigneeTypeId', 'firstName', 'lastName', 'organization']]
	# Remove any duplicate rows
	temp = temp.drop_duplicates()
	temp.to_csv(cleanDataDir + 'disAssignee.tsv', sep = '\t', index=False)


