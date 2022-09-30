# Import required packages
from requirements import *
# Import elastipy module
from elastipy.elastipy import *
# Import elasticsearch vars
from indices_and_mappings import *

es = es_connect()

def index_patents(filePath_patents, filePath_patent_types):
	_setup_index()
	df = pd.read_csv(filePath_patents, sep = '\t', dtype={'grantYear':str}, usecols=['patentId','grantDate','appFileDate','patentTitle','patentKind','numClaims','patentTypeId','grantYear'])
	df = _clean_patents(df, filePath_patent_types)
	bulk_index_data(es, ES_INDEX_USPTO_PATENTS, df, use_column_ids = True, id_column = 'patentId')

def index_assignees(filePath_patent_assignee, filePath_assignee_type):
	pa = pd.read_csv(filePath_patent_assignee, sep = '\t', dtype={'assigneeTypeId':str})
	at = pd.read_csv(filePath_assignee_type, sep = '\t', dtype={'assigneeTypeId':str})
	# Combine
	pa = pa.merge(at, on = 'assigneeTypeId', how = 'left')
	# Drop uneeded columns
	pa = pa.drop('assigneeTypeId', axis=1)
	# Rename column
	pa = pa.rename(columns = {'assigneeTypeDescription': 'assigneeType'})
	index_table(pa, convert_to_dict=True, converted_column_names = ['patentId', 'assignees'], group_by_column='patentId')

def index_gov_org(filePath_gov_org, filePath_patent_gov_org):
	govOrg = pd.read_csv(filePath_gov_org, sep = '\t')
	patentGovOrg = pd.read_csv(filePath_patent_gov_org, sep = '\t')
	# Combine
	patentGovOrg = patentGovOrg.merge(govOrg, on = 'govOrgId', how = 'left')
	# Drop uneeded columns
	patentGovOrg = patentGovOrg.drop('govOrgId', axis=1)
	index_table(patentGovOrg, convert_to_dict=True, converted_column_names = ['patentId', 'govInterests'], group_by_column='patentId')

def index_patent_wipo(filePath_patent_wipo, filePath_wipo_fields):
	patentWipo = pd.read_csv(filePath_patent_wipo, sep = '\t', dtype={'fieldId': str})
	wipoField = pd.read_csv(filePath_wipo_fields, sep = '\t', dtype={'fieldId': str})
	# Combine
	patentWipo = patentWipo.merge(wipoField, on = 'fieldId', how = 'left')
	# Drop uneeded columns
	patentWipo = patentWipo.drop('fieldId', axis=1)
	index_table(patentWipo, convert_to_dict=True, converted_column_names = ['patentId', 'wipoFields'], group_by_column='patentId')

def index_table(filePath, columns_to_use=None, convert_to_dict=False, convert_to_list=False, list_column=None, converted_column_names = None, group_by_column=None):
	if type(filePath) == str:
		# Read data
		df = pd.read_csv(filePath, sep = '\t')
	else:
		df = filePath
	# Use columns specified by user
	if columns_to_use is not None:
		df = df[columns_to_use]
	# If data needs to be converted to dictionaries, do it
	if convert_to_dict:
		df = _get_data_groups_dict(df, group_by_column, converted_column_names)
	elif convert_to_list:
		df = _get_data_groups_list(df, group_by_column, list_column, converted_column_names)

	bulk_update_data(es, ES_INDEX_USPTO_PATENTS, df, 'patentId')


####################
# HELPER FUNCTIONS #
####################

def _clean_patents(df, filePath_patent_types):
	df['grantYear'] = df.grantYear.fillna(0)
	df['grantYear'] = df['grantYear'].apply(lambda x: re.sub('\.0', '', str(x)))

	# Date columns
	df['appFileDateString'] = df.appFileDate
	df['grantDateString'] = df.grantDate
	df.appFileDate = pd.to_datetime(df.appFileDate, errors = "coerce", yearfirst=True)
	df.grantDate = pd.to_datetime(df.grantDate, errors = "coerce", yearfirst=True)
	df.grantYear = df.grantYear.astype(int)

	types = pd.read_csv(filePath_patent_types, sep = '\t')
	df = df.merge(types, on = 'patentTypeId', how = 'left')
	return df

def _setup_index():
	if not index_exists(es, ES_INDEX_USPTO_PATENTS):
		# Create index
		create_index(es, ES_INDEX_USPTO_PATENTS, field_mappings = MAPPINGS_USPTO_PATENTS, num_shards = 2, ignore_malformed=True)

def _get_data_groups_dict(data, groupByColumn, dfColumns):
	# Groupby patentId and map data to dictionaries
	groups = data.groupby(groupByColumn)
	# Convert data to lists of tuples containing uuid for group and data as dictionary
	groupList = [ ( group[0], group[1].to_dict(orient='records') ) for group in groups]
	# Convert to dataframe
	return pd.DataFrame(groupList, columns = dfColumns)

def _get_data_groups_list(data, groupByColumn, list_column, dfColumns):
	# Groupby patentId and map data to list
	groups = data.groupby(groupByColumn)
	# Convert data to lists of tuples containing uuid for group and data as list
	groupList = [ ( group[0], list(group[1][list_column])) for group in groups]
	# Convert to dataframe
	return pd.DataFrame(groupList, columns = dfColumns)
