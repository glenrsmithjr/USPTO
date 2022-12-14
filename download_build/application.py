from requirements import *
def process_applications(cleanDataDir, filePath_application, filePath_rel_app_text):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)
	apps = pd.read_csv(filePath_application, sep = '\t', dtype = str)
	relApp = pd.read_csv(filePath_rel_app_text, sep = '\t', dtype = str)

	application(cleanDataDir, apps)
	relApp = relatedApplication(cleanDataDir, relApp)
	#relatedApplicationNumber(cleanDataDir, relApp)

def application(cleanDataDir, apps):
	apps = apps.rename(columns = {'id': 'uuid', 'number': 'app_number', 'date': 'app_file_date'})
	apps.to_csv(cleanDataDir + 'application.tsv', sep = '\t', index=False)

def relatedApplication(cleanDataDir, relApp):
	#temp = relApp.merge(patentMap, on = 'patent_id', how = 'inner')
	# Rename columns
	relApp = relApp.rename(columns = {'text': 'rel_app_text'})
	# Cast text to lowercase
	relApp['rel_app_text'] = relApp['rel_app_text'].str.lower()
	
	relApp.to_csv(cleanDataDir + 'relatedApplication.tsv', sep ='\t', index=False)
	# This dataframe is needed to create the relatedApplicationNumber table
	return relApp

### FUNCTIONS NOT CURRENTLY USED ###

def relatedApplicationNumber(cleanDataDir, relApp):
	# This regex will match a pattern that begins with a number ([0-9]), then at least 6 or more consecutive numbers or commas ([0-9,]{6,}), then another space ( )
	# The number 6 here is technically arbitrary but captures most/if not all relevant application numbers (ex: 5,171,125)

	regex = ' [0-9][0-9,]{6,} '
	captureGroup = ' ([0-9][0-9,]{6,}) '
	# Relevant rows
	relRows = relApp[relApp.relAppText.str.contains(regex, na=False)]
	relRows['relAppNumber'] = relRows.relAppText.str.extract(captureGroup)
	relRows['relAppNumber'] = relRows.relAppNumber.str.replace('[^0-9a-zA-Z]+', '', regex = True)

	relRows = relRows[['patentId', 'relAppNumber']]
	relRows.to_csv(cleanDataDir + 'relatedApplicationNumber.tsv', sep = '\t', index=False)

	'''
	Other options for regex:
	1. This regex matches one space ( ), then any one number ([0-9]), then at least 6 consecutive non-space characters([^\s]{6,}), then another space ( )
	regex = ' [0-9][^\s]{6,} '
	captureGroup = ' ([0-9][^\s]{6,}) '
	2. This regex matches one space ( ), then the number 6 (6), then at least 6 consecutive non-space characters ([^\s]{6,}), then another space ( )
	regex = ' 6[^\s]{6,} '
	captureGroup = ' (6[^\s]{6,}) '
	'''

