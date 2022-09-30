# Import hadoop functions
from hadoopy.hadoopy import *
# Import required packages
from requirements import *
# Import elasticsearch package
from elastipy.elastipy import *
# Import elasticsearch variables
from indices_and_mappings import *
# Import data helper functions
import prepareElasticsearch as prep
# Import utility functions
import utils
def upload_to_hadoop(cleanDataDir, hPath):
	# Make appropriate directory in hadoop
	hadoop_mkdir(hPath)
	# Get name of files to upload
	files = os.listdir(cleanDataDir)

	for file in files:
		filePath = cleanDataDir + file
		hadoop_write(hPath + '/' + file, filePath, overwrite = True)

def upload_to_elasticsearch(cleanDataDir):
	####################### 
	# SETUP ELASTICSEARCH #
	#######################

	es = connect_to_elasticsearch()

	if index_exists(es, ES_INDEX):
		delete_index(es, ES_INDEX)

	create_index(es, ES_INDEX, field_mappings = MAPPINGS)

	##############################
	# COLLECT & COMBINE ALL DATA #
	##############################

	# Load patent data
	patents = pd.read_csv(cleanDataDir + 'patent.tsv', sep = '\t', parse_dates = ['appFileDate', 'grantDate'])
	# Create string date columns
	patents['appFileDateString'] = patents['appFileDate'].astype(str)
	patents['grantDateString'] = patents['grantDate'].astype(str)
	# Add abstracts
	utils.print_status('ADDING ABSTRACTS...')
	abstracts = pd.read_csv(cleanDataDir + 'patentAbstract.tsv', sep = '\t')
	patents = prep._add_data(patents, abstracts)
	del abstracts
	gc.collect()
	# Add applications
	utils.print_status('ADDING APPLICATIONS...')
	apps = pd.read_csv(cleanDataDir + 'application.tsv', sep = '\t', usecols = ['patentId', 'appId', 'seriesCode', 'appNum'])
	patents = prep._add_data(patents, apps)
	del apps
	gc.collect()

	utils.print_status('ADDING GOV ORGS...')
	govOrgs = prep._get_gov_org_data(cleanDataDir)
	# Add govOrgs
	patents = prep._add_data(patents, govOrgs)
	del govOrgs
	gc.collect()

	utils.print_status('ADDING GOV GRANTS...')
	govGrants = prep._get_gov_grants_data(cleanDataDir)
	# Add govGrants
	patents = prep._add_data(patents, govGrants)
	del govGrants
	gc.collect()

	utils.print_status('ADDING US CITATIONS...')
	citationsUS = prep._get_citation_data(cleanDataDir, 'us')
	# Add US citations
	patents = prep._add_data(patents, citationsUS)
	del citationsUS
	gc.collect()

	utils.print_status('ADDING FOREIGN CITATIONS...')
	citationsForeign = prep._get_citation_data(cleanDataDir, 'foreign')
	# Add Foreign citations
	patents = prep._add_data(patents, citationsForeign)
	del citationsForeign
	gc.collect()

	utils.print_status('ADDING WIPO...')
	wipo = prep._get_wipo_data(cleanDataDir)
	# Add wipo
	patents = prep._add_data(patents, wipo)
	del wipo
	gc.collect()

	utils.print_status('ADDING CPC...')
	cpc = prep._get_cpc_data(cleanDataDir)
	# Add classifications
	patents = prep._add_data(patents, cpc)
	del cpc
	gc.collect()

	utils.print_status('ADDING IPC...')
	ipc = prep._get_ipc_data(cleanDataDir)
	# Add classifications
	patents = prep._add_data(patents, ipc)
	del ipc
	gc.collect()

	utils.print_status('ADDING CLAIMS...')
	claims = prep._get_claim_data(cleanDataDir)
	# Add claims
	patents = prep._add_data(patents, claims)
	del claims
	gc.collect()

	utils.print_status('ADDING FOREIGN PRIORITY...')
	fps = prep._get_foreign_priority_data(cleanDataDir)
	# Add foreign prirorities
	patents = prep._add_data(patents, fps)
	del fps
	gc.collect()

	utils.print_status('ADDING OTHER REFS...')
	otherRefs = prep._get_other_ref_data(cleanDataDir)
	# Add other references
	patents = prep._add_data(patents, otherRefs)
	del otherRefs
	gc.collect()

	utils.print_status('ADDING ASSIGNEES...')
	assignees = prep._get_assignee_data(cleanDataDir)
	# Add assignees
	patents = prep._add_data(patents, assignees)
	del assignees
	gc.collect()

	chunksize = 100000
	# Will split the data into chunks of 100,000
	splitBy = int(np.ceil(patents.shape[0] / chunksize))
	chunks = np.array_split(patents, splitBy)

	#id_counter = 0

	for chunk in chunk:
		index_data(es, ES_INDEX, chunk)
		#id_counter += chunksize
	utils.print_status('DONE UPLOADING.')



