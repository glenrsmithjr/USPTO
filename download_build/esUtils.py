# Helper function to generate desired column names for gov orgs data
def _get_renamed_columns(table):
	if table == 'govOrgs':
		return {'name': 'govOrgName'}
# Helper function to select the list of columns that will be indexed into elasticsearch
def _get_desired_columns(table):
	if table == 'assignees':
		return ['firstName', 'lastName', 'assigneeTypeId', 'organization']
		
	if table == 'claims':
		return ['dependent', 'sequence', 'exemplary']

	elif table == 'foreign_priority':
		return ['priorityType', 'sequence', 'foreignAppNum', 'foreignAppDate', 'foreignCountry']

	elif table == 'govOrgs':
		return ['govOrgId', 'govOrgName', 'levelOne', 'levelTwo', 'levelThree']

	elif table == 'inventors':
		return ['sequence', 'inventorId', 'locationDid', 'firstName', 'lastName']

	elif table == 'nber':
		return ['nberCategory', 'nberSubCategory']		

	elif table == 'other_references':
		return ['text', 'sequence']

	elif table == 'patent_cpc':
		return ['sequence','cpcSectionId','cpcSubSectionId','cpcGroupId','cpcSubGroupId','subGroupPrime']

	elif table == 'patent_ipc':
		return ['sequence','ipcSectionId','ipcClassId','ipcSubClassId','ipcGroupId','ipcSubGroupId','symbolPosition','classValue','classStatus','dataSource','actionDate','ipcVersion']	

	elif table == 'wipo':
		return ['sequence', 'fieldId']