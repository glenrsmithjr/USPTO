from hadoopy.hadoopy import *
# Import necessary modules
from requirements import *
# Import files
import application
import assignee
import citation
import claim
# import dodpatents
import download
import govInterest
import inventor
import location
import maintenance
import miscellaneous
import patentClassification
import patentCore
# import patentStats
# upload
import utils
import wipo


def main():
	#################
	# Download Data #
	#################
	# Create two data directories for raw and clean data
	data_dirs = ['raw-data/', 'raw-data/claims/', 'clean-data/', 'clean-data/claims/']
	for d in data_dirs:
		os.makedirs(d, exist_ok=True)
	# Download data into raw-data folder
	download.download_all(data_dirs[0], data_dirs[1])
	exit(0)

	################
	# Process Data #
	################

	# See function below for explaination of this function
	fileDict = get_file_locations(data_dirs[0], data_dirs[1])
	process_data(fileDict, data_dirs)

	#########################
	# Upload Data to Hadoop #
	#########################

	# See archive/hadoop.py

	################################
	# Ingest Data to Elasticsearch #
	################################

	# See archive/elastic.py


def process_data(fileDict, data_dirs):
	################
	# Process Data #
	################
	# Note: The * in front of data_dirs when passed into functions will unpack the list into multiple input parameters
	
	utils.print_processing_status ('PATENTS')
	# PatentCore.py
	patentCore.process_patents(data_dirs[2], fileDict['patent'], fileDict['application'])
	
	utils.print_processing_status ('LOCATIONS')
	# Location.py
	location.process_locations(data_dirs[2], fileDict['location'])
	
	utils.print_processing_status ('ASSIGNEES')
	# Assignees.py
	assignee.process_assignees(data_dirs[2], fileDict['assignee'], fileDict['patent_assignee'],
							   fileDict['location_assignee'])
	
	utils.print_processing_status ('INVENTORS')
	# Inventor.py
	inventor.process_inventors(data_dirs[2], fileDict['inventor'], fileDict['patent_inventor'],
							   fileDict['location_inventor'])

	#utils.print_processing_status ('DOD PATENTS')
	# Dodpatents.py
	#dodpatents.process_dod_patents(*data_dirs)

	utils.print_processing_status ('WIPO')
	# Wipo.py
	wipo.process_wipo(data_dirs[2], fileDict['wipo'], fileDict['wipo_field'])

	utils.print_processing_status ('CLASSIFICATIONS')
	# PatentClassification.py
	patentClassification.process_classifications(data_dirs[2], fileDict['cpc_current'], fileDict['ipcr'],
												 fileDict['cpc_subsection'], fileDict['cpc_group'],
												 fileDict['cpc_subgroup'])
	
	utils.print_processing_status ('GOV DATA')
	# govInterest.py
	govInterest.process_gov_data(data_dirs[2], fileDict['government_organization'], fileDict['patent_govintorg'],
								 fileDict['government_interest'], fileDict['patent_contractawardnumber'])
	
	utils.print_processing_status ('APPLICATIONS')
	# Application.py
	application.process_applications(data_dirs[2], fileDict['application'], fileDict['rel_app_text'])
	
	utils.print_processing_status ('MAINTENANCE')
	# Maintenance.py
	maintenance.process_maintenance(data_dirs[2], fileDict['MaintFeeEvents'], fileDict['MaintFeeEventsDesc'])
	
	utils.print_processing_status ('CITATIONS')
	# Citations.py
	citation.process_citations(data_dirs[2], fileDict['uspatentcitation'], fileDict['foreigncitation'])
	
	utils.print_processing_status ('MISC TABLES')
	miscellaneous.process_misc_tables(data_dirs[2], fileDict['foreign_priority'], fileDict['otherreference'],
									  fileDict['nber'], fileDict['nber_category'], fileDict['nber_subcategory'])
	
	#utils.print_processing_status ('PATENT STATS')
	# Patentstats.py
	#patentStats.process_patentStats(data_dirs[1])

	utils.print_processing_status('CLAIMS')
	# Claim.py
	claim_files = [fileDict[k] for k in fileDict.keys() if 'claim' in k]
	claim.process_claims(data_dirs[3], claim_files)

	utils.print_status ('DONE PROCESSING.')


def get_file_locations(raw_data_dir, claims_dir):
	######################
	# GET FILE LOCATIONS #
	######################
	'''
		For some stupid reason, (most) PatentsView files are now consildated into a folder (20190820/download) 
		even when the code tries to unzip the files and store them directly in the raw-data/ folder. Some files
		do not follow this logic, for instance, "patent_govintorg.tsv" will be unzipped directly in the raw-data/ 
		folder and the patent maintenenace files will be unzipped directly in the raw-data/ folder because they
		come from http://patents.reedtech.com/. This section can be updated and will allow us to collect the file
		locations as PatentsView decides to change them.
	'''

	main_files = [f for f in os.listdir(raw_data_dir) if f != 'claims']
	main_files_dict = {file.split('.')[0]: raw_data_dir + file for file in main_files}

	claim_files = os.listdir(claims_dir)
	claim_files_dict = {file.split('.')[0]: claims_dir + file for file in claim_files}

	main_files_dict.update(claim_files_dict)
	return main_files_dict


if __name__== "__main__":
	main()
