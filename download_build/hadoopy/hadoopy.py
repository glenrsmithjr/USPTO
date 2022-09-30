from hadoopy.requirements import *
#########################################################################################################
# ALL GUIDANCE FROM: https://hadoop.apache.org/docs/r3.0.3/hadoop-project-dist/hadoop-hdfs/WebHDFS.html #
#########################################################################################################

##################
# MAIN FUNCTIONS #
##################

# Function to append to a file in hadoop
def hadoop_append(hadoop_path, filename):
	# Url definition to contact the namenode and get location of data
	url = 'http://' + HADOOP_HOST + ':' + PORTS['namenode'] + '/webhdfs/v1' + hadoop_path + '?op=APPEND'
	try:
		# Get the initial response from hadoop
		appendResponse = requests.post(url, allow_redirects = False)

		# A status code of 307 (temporary redirect) is what we want 
		if appendResponse.status_code == 307:
			# The datapath returned is the path and operation to insert the data into a particular datanode
			dataPath = appendResponse.headers['Location']
			# The datapath url must be changed so that we are accessing the datanode via the IP of server 1 and the appropriate port
			writePath = _clean_path(dataPath)
			# Create a session object to keep connection alive when sending data
			session = requests.session()
			with open(filename, 'rb') as f:
				insertResponse = session.post(url=writePath, data = f)
				# Check for 200 Ok response
				if insertResponse.status_code == 200:
					_print_status('File ' + filename + ' successfully appended.')
				else:
					_print_status ('ERROR IN: append_to_file: File ' + filename + ' was not successfully appended. Check your inputs and ensure that the (full) hadoop path includes the name and file extension of the file to be appended to.')
					_print_status (insertResponse.content)
		else:
			_print_status (appendResponse.content)

	except Exception as e:
		_print_status (e)

# Function to delete a file or directory
def hadoop_delete(hadoop_path):
	# Url definition to contact the namenode and get location of file/directory
	url = 'http://' + HADOOP_HOST + ':' + PORTS['namenode'] + '/webhdfs/v1' + hadoop_path + '?op=DELETE&recursive=true'
	try:
		# Response from hadoop contact request
		response = requests.delete(url)
		if response.status_code == 200:
			if _check_response(response.content):
				_print_status ('Delete successful.')
			else:
				_print_status ('ERROR IN: delete: The file or directory ' + hadoop_path + ' was not deleted. Check your inputs and ensure the parameter begins with a forward slash.')
		else:
			_print_status ('ERROR IN: delete: The file or directory ' + hadoop_path + ' was not deleted. Check your inputs and ensure the parameter begins with a forward slash.')
	except Exception as e:
		_print_status (e)	

# Lists the files in a hadoop directory
def hadoop_listdir(hadoop_path, verbose = False):
	# Url definition to contact the namenode and get location of directory
	url = 'http://' + HADOOP_HOST + ':' + PORTS['namenode'] + '/webhdfs/v1' + hadoop_path + '?op=LISTSTATUS'
	try:
		# Response from hadoop contact request
		response = requests.get(url)
		if response.status_code == 200:
			results = json.loads(response.content)

			if verbose:
				return (results['FileStatuses']['FileStatus'])
			else:
				files = []
				for file in results['FileStatuses']['FileStatus']:
					files.append(file['pathSuffix'])
				return (files)
		else:
			_print_status ('ERROR IN: listdir: The directory ' + hadoop_path + ' could not be returned. Check your inputs and ensure the parameter begins with a forward slash.')
	except Exception as e:
		_print_status (e)
# Creates a directory
def hadoop_mkdir(hadoop_path):
	# Url definition to contact the namenode and get location of what datanode to put directory on
	url = 'http://' + HADOOP_HOST + ':' + PORTS['namenode'] + '/webhdfs/v1' + hadoop_path + '?op=MKDIRS'
	try:
		# Response from hadoop contact request
		response = requests.put(url)
		if response.status_code == 200:
			if _check_response(response.content):
				_print_status ('Directory ' + hadoop_path + ' created successfully.')
			else:
				_print_status ('ERROR IN: mkdir: The directory ' + hadoop_path + ' was not created successfully. Check your inputs and ensure the parameter begins with a forward slash.')
		else:
			_print_status ('ERROR IN: mkdir: The directory ' + hadoop_path + ' was not created successfully. Check your inputs and ensure the parameter begins with a forward slash.')
	except Exception as e:
		_print_status (e)	

# Function to download some or all files in the given directory
def hadoop_mread(hadoop_path, download_dir = '', files = None):
	download_dir = _validate_directory(download_dir)
	if files is None:
		# Collect files in the provided hadoop directory
		files = hadoop_listdir(hadoop_path)
		# Filter out 
		files = [file for file in files if '.' in file]
		
	for file in files:
		hPath = hadoop_path + '/' + file
		dwnldPath = download_dir + file
		hadoop_read(hPath, save_to_disk=True, filename=dwnldPath) 

# Function to upload some or all files in the given directory
def hadoop_mwrite(hadoop_path, upload_dir = '', files = None, overwrite = True):
	upload_dir = _validate_directory(upload_dir)

	if files in None:
		# Get filenames from directory
		files = os.listdir(upload_dir)
		# Get full relative paths for files
		files = [file for file in files if '.' in file]

	for file in files:
		hPath = hadoop_path + '/' + file
		hadoop_write(hPath, file, overwrite)


# Returns a file or saves to disk
def hadoop_read(hadoop_path, save_to_disk = False, filename = None, dtypes = None, nrows = None):
	# Url definition to contact the namenode and get location of data
	url = 'http://' + HADOOP_HOST + ':' + PORTS['namenode'] + '/webhdfs/v1' + hadoop_path + '?op=OPEN'
	try:
		readResponse = requests.get(url, allow_redirects = False)
		# A status code of 307 (temporary redirect) is what we want 
		if readResponse.status_code == 307:
			# The datapath returned is the path and operation to insert the data into a particular datanode
			dataPath = readResponse.headers['Location']
			# The datapath url must be changed so that we are accessing the datanode via the IP of server 1 and the appropriate port
			readPath = _clean_path(dataPath)
			if save_to_disk:
				_save_file(readPath, filename)
			else:
				return (_return_file(readPath, hadoop_path, dtypes, nrows))
		else:
			_print_status (readResponse.content)
	except Exception as e:
		_print_status (e)

def hadoop_rename(source, destination):
	# Url definition to contact the namenode and get location of file/directory to rename
	url = 'http://' + HADOOP_HOST + ':' + PORTS['namenode'] + '/webhdfs/v1' + source + '?op=RENAME&destination=' + destination
	try:
		# Response from hadoop contact request
		response = requests.put(url)
		if response.status_code == 200:
			if _check_response(response.content):
				_print_status ('Directory or file ' + source + ' successfully renamed to ' + destination + '.')
			else:
				_print_status ('ERROR IN: rename: The directory or file ' + source + ' was not renamed. Check your inputs and ensure each parameter begins with a forward slash.')	
		else:
			_print_status ('ERROR IN: rename: The directory or file ' + source + ' was not renamed. Check your inputs and ensure each parameter begins with a forward slash.')
	except Exception as e:
		_print_status (e)	

# Function to write to a file
def hadoop_write(hadoop_path, filename, overwrite = False):
	# Convert overwrite param to string representation
	overwrite = str(overwrite).lower()
	# Url definition to contact the namenode and get location of datanode to write data on
	url = 'http://' + HADOOP_HOST + ':' + PORTS['namenode'] + '/webhdfs/v1' + hadoop_path + '?op=CREATE&overwrite=' + overwrite
	try:
		createResponse = requests.put(url, allow_redirects = False)

		# A status code of 307 (temporary redirect) is what we want 
		if createResponse.status_code == 307:
			# The datapath returned is the path and operation to insert the data into a particular datanode
			dataPath = createResponse.headers['Location']
			# The datapath url must be changed so that we are accessing the datanode via the IP of server 1 and the appropriate port
			writePath = _clean_path(dataPath)
			session = requests.session()
			with open(filename, 'rb') as f:
				#insertResponse = session.put(url=writePath, data = data.encode('utf-8'))
				insertResponse = session.put(url=writePath, data = f)
				# 201 is a "created" response
				if insertResponse.status_code == 201:
					_print_status('File ' + filename + ' uploaded successfully.')
				else:
					_print_status ('ERROR IN: write_file: The file ' + filename + ' could not be uploaded. Check your inputs and ensure that the (full) hadoop path includes the name and file extension of the file to be created.')
					_print_status (createResponse.content)
		else:
			_print_status (createResponse.content)

	except Exception as e:
		_print_status (e)


####################
# HELPER FUNCTIONS #
####################

# Checks boolean response returned from requests
# Ex: b'{"boolean":false}'
def _check_response(response):
	x = json.loads(response)
	return (x['boolean'])

# This function takes a datanode path and changes it to the format we need to interact with the hadoop cluster
# filePath ex: http://datanode1:9864/webhdfs/v1/example_data/example.txt?op=CREATE...
def _clean_path(filePath):
	# Ex: ['http:', '', 'datanode1:9864', 'webhdfs', 'v1', ...]
	pathTokens = filePath.split('/')
	# Find out which datanode port we are writing to
	node, _ = pathTokens[2].split(':')
	# Set those values in the list of path components
	pathTokens[2] = HADOOP_HOST + ':' + PORTS['datanode' + node[-1]]
	# Combine path components to get full path
	path = '/'.join(pathTokens)
	return (path)

# Used to print messages to the console. Linux for some reason requires a flush command.
def _print_status(status):
	print (status)
	sys.stdout.flush()

# Returns a file's contents
def _return_file(readPath, hadoop_path, dtypes, nrows):		
	ext = hadoop_path.split('.')[-1]
	# If file is a csv, use pandas read_csv method, same for tsv
	if ext == 'csv':
		data = pd.read_csv(readPath, dtype = dtypes, nrows = nrows)
	elif ext == 'tsv':
		data = pd.read_csv(readPath, dtype = dtypes, nrows = nrows, sep = '\t')
		
	# If file is a text file, return decoded text contents
	else:
		data = requests.get(readPath)
		if data.status_code == 200:
			return (data.content.decode('utf-8'))
		else:
			_print_status(data.content)
	return (data)

# Saves a file to disk by using streaming during request
def _save_file(readPath, filename):
	if filename is None:
		_print_status('ERROR IN: read_file: You opted to save a file but did not provide a file name. Please provide a value for the "filename" parameter.')
		return
	# Streams the data so it is not all loaded into memory at once
	with requests.get(readPath, stream=True) as response:
		if response.status_code == 200:
			with open(filename, 'wb') as file:
				# Read in 500 MB at a time
				for chunk in response.iter_content(chunk_size=524288000): 
					# filter out 'keep-alive' chunks
					if chunk: 
						file.write(chunk)
		else:
			_print_status(response.content)

# Validate and correct directories that are inputted by the user
def _validate_directory(directory):
	if directory == '':
		return directory
	else:
		if directory[-1] == '/':
			return directory
		else:
			directory += '/'
			return directory

