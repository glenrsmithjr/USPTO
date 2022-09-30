from requirements import *
import utils


# Downloads all USPTO tables
# download_date: The data of publication of these tables, defined in uspto.py
# dataDir: 		 The relative path of the location the raw data will be stored in
def download_all(raw_data_dir, claims_dir):
    files = get_files()
    # Just used for the display
    count = 1
    num_files = str(len(files))

    for file, url in files.items():
        print_download_status(file, count, num_files)
        count += 1
        # split url on '/' to get file name
        filename = url.split('/')[-1]
        download_file(url, raw_data_dir, filename)

    # Special code for MaintFeeEvents data files
    '''
	files = os.listdir(dataDir)
	# Find Maint files
	maintFiles = [filename for filename in files if 'Maint' in filename]
	for file in maintFiles:
		# Keep the first part of the filename before the '_' (Ex: MaintFeeEventsDesc_20190610.txt => MaintFeeEventsDesc.txt)
		toKeep = file.split('_')[0]
		# Rename file [os.rename(src, dst)]
		os.rename(dataDir + file, dataDir + toKeep + '.txt')
	'''
    claim_files = get_claim_files()
    # Just used for the display
    count = 1
    num_files = str(len(claim_files))

    for url in claim_files:
        # split url on '/' to get file name
        filename = url.split('/')[-1]
        print_download_status(filename, count, num_files)
        count += 1
        download_file(url, claims_dir, filename)

    utils.print_status('DOWNLOADING DONE.')


# A helper function to clean up the above code. Prints the current file being downloaded.
def print_download_status(file, count, num_files):
    utils.print_status('DOWNLOADING ' + file + '...(' + str(count) + '/' + num_files + ')')


# Downloads a particular uspto table given the url of the file
# url: 		Location of the file to be downloaded
# dataDir: 	The name of the folder the raw data will be stored in
# filename: The name of the file the data will be downloaded to
def download_file(url, dataDir, filename):
    # Defines the relative path of the file (in a data sub-directory)
    filepath = dataDir + filename
    try:
        # Download file
        # Streams the data so it is not all loaded into memory at once
        with requests.get(url, allow_redirects=True, stream=True) as response:
            with open(filepath, 'wb') as file:
                # read 500 MB at a time
                for chunk in response.iter_content(chunk_size=524288000):
                    # filter out 'keep-alive' chunks
                    if chunk:
                        file.write(chunk)
    except Exception as e:
        utils.print_status('COULD NOT DOWNLOAD FILE: ' + filename + '. PRINTING EXCEPTION...')
        utils.print_status(e)

    try:
        # Unzip file (can maybe figure out how to do this without first saving file)
        z = zipfile.ZipFile(filepath, 'r')
        z.extractall(dataDir)
        z.close()
        # Delete the zipped file, we only care about the unzipped version
        os.remove(filepath)
    except Exception as e:
        utils.print_status('COULD NOT UNZIP FILE: ' + filename + '. PRINTING EXCEPTION')
        utils.print_status(e)


# The files are encoded as a dictionary to easily keep track of the tables being downloaded.
def get_files():
    return {
        "application"				: "https://s3.amazonaws.com/data.patentsview.org/download/application.tsv.zip",
		"assignee"					: "https://s3.amazonaws.com/data.patentsview.org/download/assignee.tsv.zip",
		"cpc_current"				: "https://s3.amazonaws.com/data.patentsview.org/download/cpc_current.tsv.zip",
		"cpc_group"					: "https://s3.amazonaws.com/data.patentsview.org/download/cpc_group.tsv.zip",
		"cpc_subgroup"				: "https://s3.amazonaws.com/data.patentsview.org/download/cpc_subgroup.tsv.zip",
		"cpc_subsection"			: "https://s3.amazonaws.com/data.patentsview.org/download/cpc_subsection.tsv.zip",
		"foreign_priority"			: "https://s3.amazonaws.com/data.patentsview.org/download/foreign_priority.tsv.zip",
		"foreigncitation"			: "https://s3.amazonaws.com/data.patentsview.org/download/foreigncitation.tsv.zip",
		"government_interest"		: "https://s3.amazonaws.com/data.patentsview.org/download/government_interest.tsv.zip",
	    "government_organization"	: "https://s3.amazonaws.com/data.patentsview.org/download/government_organization.tsv.zip",
	    "inventor"					: "https://s3.amazonaws.com/data.patentsview.org/download/inventor.tsv.zip",
	    "ipcr"						: "https://s3.amazonaws.com/data.patentsview.org/download/ipcr.tsv.zip",
	    "location"					: "https://s3.amazonaws.com/data.patentsview.org/download/location.tsv.zip",
	    "nber"						: "https://s3.amazonaws.com/data.patentsview.org/download/nber.tsv.zip",
	    "nber_category"				: "https://s3.amazonaws.com/data.patentsview.org/download/nber_category.tsv.zip",
	    "nber_subcategory"			: "https://s3.amazonaws.com/data.patentsview.org/download/nber_subcategory.tsv.zip",
	    "otherreference"			: "https://s3.amazonaws.com/data.patentsview.org/download/otherreference.tsv.zip",
	    "patent"					: "https://s3.amazonaws.com/data.patentsview.org/download/patent.tsv.zip",
	    "patent_assignee"			: "https://s3.amazonaws.com/data.patentsview.org/download/patent_assignee.tsv.zip",
	    "patent_contractawardnumber": "https://s3.amazonaws.com/data.patentsview.org/download/patent_contractawardnumber.tsv.zip",
	    "patent_inventor"			: "https://s3.amazonaws.com/data.patentsview.org/download/patent_inventor.tsv.zip",
	    "patent_govintorg"			: "https://s3.amazonaws.com/data.patentsview.org/download/patent_govintorg.tsv.zip",
		"rawassignee"				: "https://s3.amazonaws.com/data.patentsview.org/download/rawassignee.tsv.zip",
		"rawinventor"				: "https://s3.amazonaws.com/data.patentsview.org/download/rawinventor.tsv.zip",
		"rawlocation"				: "https://s3.amazonaws.com/data.patentsview.org/download/rawlocation.tsv.zip",
	    "rel_app_text"				: "https://s3.amazonaws.com/data.patentsview.org/download/rel_app_text.tsv.zip",
	    "uspatentcitation"			: "https://s3.amazonaws.com/data.patentsview.org/download/uspatentcitation.tsv.zip",
	    "wipo"						: "https://s3.amazonaws.com/data.patentsview.org/download/wipo.tsv.zip",
	    "wipo_field"				: "https://s3.amazonaws.com/data.patentsview.org/download/wipo_field.tsv.zip"
	}


def get_claim_files():
	return ["https://s3.amazonaws.com/data.patentsview.org/claims/claims_{}.tsv.zip".format(year)
			for year in range(1976, 2022)]

