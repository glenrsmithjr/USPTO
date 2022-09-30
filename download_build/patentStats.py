from requirements import *
def process_patentStats(cleanDataDir):
	'''
	print ('getting group counts')
	#citingGroupCounts = pd.read_csv(cleanDataDir + 'citingGroupCounts.tsv', sep = '\t', usecols = ['outgoingCitationId', 'incomingCitationId', 'cpcSubSectionId', 'cpcGroupId', 'groupCount'])
	
	# --- patentId, cpcSubSectionId, cpcGroupId, groupCount ---
	# Get number of subgroups per patent
	#groupCounts, cpc = get_cpc_group_counts(cleanDataDir)
	#groupCounts.to_csv(cleanDataDir + 'groupCounts.csv', index=False)
	#exit(0)
	groupCounts = pd.read_csv(cleanDataDir + 'groupCounts.csv', dtype={'patentId': str})
	groupCounts['incomingCitationsUS'] = groupCounts['incomingCitationsUS'].fillna(0)

	# Calculate citation percentile
	calculate_citation_percentile(groupCounts)
	print ('getting citing group counts')
	# --- outgoingCitationId, incomingCitationId, cpcSubSectionId, cpcGroupId, groupCount, grantYear ---
	# Get number of times a patent has cited each sub group
	citingGroupCounts = get_citing_group_count(cleanDataDir, groupCounts)
	print ('saving')
	#citingGroupCounts = get_citing_group_count(cleanDataDir, citingGroupCounts)
	citingGroupCounts.to_csv(cleanDataDir + 'citingGroupCounts.tsv', sep = '\t', index=False)
	exit(0)
	'''
	print ('calculating diversity')
	# Calculate Diversity
	#citingGroupCounts.groupby('outgoingCitationId').apply(lambda x: compute_diversity(x)).reset_index()
	citingGroupCounts.groupby('incomingCitationId').apply(lambda x: compute_diversity(x)).reset_index()
	print ('saving stats')
	# temporarily save stats
	citingGroupCounts.to_csv(cleanDataDir + 'stats.tsv', sep = '\t', index=False)
	print ('calculating radicalness')
	# Calculate Radicalness
	citingGroupCounts = calculate_radicalness(citingGroupCounts, cpc)
	print ('saving stats')
	# temporarily save stats
	citingGroupCounts.to_csv(cleanDataDir + 'stats.tsv', sep = '\t', index=False)
	# Rename patent ID column and collect relevant columns
	stats = citingGroupCounts.rename(columns = {'outgoingCitationId': 'patentId'})[['patentId', 'diversity', 'radicalness']]
	# Bring in patents table
	patents =  pd.read_csv(cleanDataDir + 'patent.tsv', sep = '\t', dtype = {'grantYear': int, 'incomingCitationsUS': int, 'outgoingCitationsForeign': int, 'outgoingCitationsUS': int, 'numClaims': int})
	# Select stats to join with other stats
	patentCitations = patents['patentId', 'grantYear', 'incomingCitationsUS', 'outgoingCitationsUS', 'outgoingCitationsForeign', 'numClaims']
	print ('merging stats with patent citations metrics')
	stats = stats.merge(patentCitations, on = 'patentId', how = 'inner')
	print ('getting cpc tables')
	# set up two patent cpc tables, one for subsection and one for group
	cpcSubSection, cpcGroup = get_cpc_tables(cpc)
	print ('calculating incoming citation metrics')
	# aggregate and normalize incoming citation metrics
	stats = calculate_incoming_citation_metrics(stats, cpcSubSection)
	print ('calculating other metrics')
	# Calculae other metrics
	stats = calculate_other_metrics(stats)
	print ('merging all patents with stats')
	# Merge patents and metrics
	patents = patents.merge(stats, on = 'patentId', how = 'left', suffixes = ('', '_x'))
	# Only save columns that don't have _x suffix in them
	colsToKeep = [col for col in list(patents.columns) if '_' not in col]
	patents = patents[colsToKeep]
	print ('saving')
	# Save
	patents.to_csv(cleanDataDir + 'patent.tsv', sep ='\t', index=False)
'''
# get num cpc group instances per sub-section/patent (for normalization and diversity metrics)
def get_cpc_group_counts(cleanDataDir):
	cpc = pd.read_csv(cleanDataDir + 'patentCPC.tsv', sep = '\t', dtype={'patentId':str}, usecols = ['patentId', 'cpcSubSectionId', 'cpcGroupId'])
	patents = pd.read_csv(cleanDataDir + 'patent.tsv', sep = '\t', dtype = { 'patentId': str}, usecols = ['patentId', 'grantYear', 'incomingCitationsUS'])
	# Help from: https://stackoverflow.com/questions/48770035/adding-a-count-column-to-the-result-of-a-groupby-in-pandas
	groupCounts = cpc.groupby(['patentId', 'cpcSubSectionId', 'cpcGroupId']).size().reset_index(name='groupCount')
	# Fill na value
	groupCounts['groupCount'] = groupCounts['groupCount'].fillna(0)

	groupCounts = groupCounts.merge(patents, on = 'patentId', how = 'left')
	return (groupCounts, cpc)

def calculate_citation_percentile(groupCounts):
	yearGroups = groupCounts.groupby('grantYear')

	pool = mp.Pool(6)
	results = pool.map(compute_citation_percentile, [df for _, df in yearGroups])
	return pd.concat(results)

def compute_citation_percentile(df):
	avgIncomingCites = df['incomingCitationsUS'].mean()

	classGroups = df.groupby('cpcGroup')
	
	for _, cg in classGroups:
		# Calculate group count sum
		totalGroupCounts = cg['groupCount'].sum()
		# Groupby patentId to calculate citation percentile for each patent
		patentGroups = cg.groupby('patentId')

		for _, pg in patentGroups:
			patentGroupCount = pg['groupCount'].sum()

def get_citing_group_count(cleanDataDir, groupCounts):
	print ('reading citations')
	# Get US to US citations
	citations = pd.read_csv(cleanDataDir + 'usCitations.tsv', sep = '\t', dtype={'incomingCitationId':str}, usecols = ['outgoingCitationId', 'incomingCitationId'])
	print ('merging')
	# Join in number of groups each cited patent is a part of 
	citingGroupCounts = citations.merge(groupCounts, left_on = 'incomingCitationId', right_on = 'patentId', how = 'inner')
	citingGroupCounts = citingGroupCounts[['outgoingCitationId', 'incomingCitationId', 'cpcSubSectionId', 'cpcGroupId', 'groupCount', 'grantYear']]

	return citingGroupCounts
	# Get number of times a patent has cited each group
	# Solution from here: https://stackoverflow.com/questions/30244952/how-do-i-create-a-new-column-from-the-output-of-pandas-groupby-sum#comment90956254_30244979
	#citingGroupCounts['numTimesCited'] = citingGroupCounts['groupCount'].groupby(citingGroupCounts[['incomingCitationId', 'cpcGroupId']]).tranform('sum')
	# Split data into chunks
	#citingGroupCounts.to_csv(cleanDataDir + 'citingGroupCounts.tsv', sep = '\t', index=False)
	chunksize = 500000
	splitBy = int(np.ceil(citingGroupCounts.shape[0] / chunksize))
	chunks = np.array_split(citingGroupCounts, splitBy)
	df = pd.DataFrame()
	for chunk in chunks:
		chunk['numTimesCited'] = chunk.groupby(['incomingCitationId', 'cpcGroupId'])['groupCount'].agg('sum')
		df = df.append(chunk)
	#citingGroupCounts['numTimesCited'] = citingGroupCounts.groupby(['incomingCitationId', 'cpcGroupId'])['groupCount'].agg('sum')
	return (df)

def get_citing_group_count(cleanDataDir, citingGroupCounts):
	chunksize = 1000000
	splitBy = int(np.ceil(citingGroupCounts.shape[0] / chunksize))
	chunks = np.array_split(citingGroupCounts, splitBy)
	df = pd.DataFrame()
	for chunk in chunks:
		print (str(chunksize / citingGroupCounts.shape[0]) + '%')
		chunk['numTimesCitedByGroup'] = chunk.groupby(['incomingCitationId', 'cpcGroupId'], as_index=False)['groupCount'].agg('sum')['groupCount']
		df = df.append(chunk)
	#citingGroupCounts['numTimesCited'] = citingGroupCounts.groupby(['incomingCitationId', 'cpcGroupId'])['groupCount'].agg('sum')
	return (df)
'''
def compute_diversity(df):
	numTimesCitedSum = df['numTimesCitedByGroup'].sum()
	df['percentile_squared'] = (df['numTimesCitedByGroup'] / numTimesCitedSum)**2
	percSquaredSum = df['percentile_squared'].sum()
	df['diversity'] = 1 - percSquaredSum
	return (df)

# The function that is used in the main function above (process_patentStats)
def calculate_radicalness(citingGroupCounts, cpc):
	citingGroupCounts = citingGroupCounts.merge(cpc, left_on = ['outgoingCitationId', 'cpcGroupId'], right_on = ['patentId', 'cpcGroupId'], how = 'left')
	citingGroupCounts = citingGroupCounts[['outgoingCitationId', 'cpcGroupId', 'groupCount', 'numTimesCited', 'cpcSubSectionId']]
	citingGroupCounts = citingGroupCounts.groupby('outgoingCitationId').apply(lambda x: compute_radicalness(x)).reset_index()
	return (citingGroupCounts)

# The function used by the apply method in calculate_radicalness
def compute_radicalness(df):
	# Define a portion of the radicalness function in this column
	df['radicalness_values'] = [row['numTimesCitedByGroup'] if row['groupCount'] != 0 else 0 for _, row in df.iterrows()]
	# Create sum values that are also used in the equation
	radicalValuesSum = df['radicalness_values'].sum()
	numTimesCitedSum = df['numTimesCitedByGroup'].sum()
	df['radicalness'] = 1 - (radicalValuesSum / numTimesCitedSum)
	return (df)

def get_cpc_tables(cpc):
	cpcSubSection = cpc.groupby(['patentId', 'cpcSubSectionId']).size().reset_index(name='subSectionCount')
	cpcGroup = cpc.groupby(['patentId', 'cpcGroupId']).size().reset_index(name='groupCount')
	return (cpcSubSection, cpcGroup)

# The function that is used in the main function above (process_patentStats)
def calculate_incoming_citation_metrics(stats, cpcSubSection):
	# Add in cpcSubSection counts
	stats = stats.merge(cpcSubSection, on = 'patentId', how = 'left')

	groupByYearMap = get_group_by_year_map(stats[['cpcGroup', 'grantYear']])
	# Group by subSection and year
	#stats = stats.groupby(['cpcGroup', 'grantYear']).apply(lambda x: compute_incoming_citation_metrics(x)).reset_index()
	stats = stats.groupby('incomingCitationId').apply(lambda x: compute_citation_percentile(x)).reset_index()
	return (stats)

def get_group_by_year_map(df):
	return df.groupby(['cpcGroup', 'grantYear']).size().reset_index(name='countsByYear')

# The function used by the apply method in calculate_incoming_citation_metrics
# MAYBE CHANGE TO CITATION PERCENTILE FUNCTION
def compute_incoming_citation_metrics(df):
	incomingCitesAvg = df['incomingCitationsUS'].mean(skipna=True)
	# fill na values if necessary
	df['incomingCitationsUS'] = df['incomingCitationsUS'].fillna(0)
	# Compute incoming norm citation metric
	df['incomingNCI'] = df['incomingCitationsUS'] / incomingCitesAvg
	# Compute rank of value
	df['incomingNCIRank'] = df.incomingNCI.rank(pct=True)
	return (df)

def calculate_other_metrics(stats):
	# Cites per claim
	stats['citesPerClaim'] = (stats['OutgoingCitationsUS'].astype(int) + stats['OutgoingCitationsForeign'].astype(int)) / patentStats['numClaims'].astype(int)
	# diversityRank
	stats['diversityRank'] = stats.diversity.rank(pct=True)
	# radicalnessRank
	stats['radicalnessRank'] = stats.radicalness.rank(pct=True)
	# citesPerClaimRank	
	stats['citesPerClaimRank'] = stats.citesPerClaim.rank(pct=True)
	return (stats)

### OLD CODE ###
'''
	allOutgoingCites, allIncomingCites, inCitesPerYear, outCitesPerYear = get_citation_counts(patents)
	# Cites per claim
	patents['citesPerClaim'] = (patents['OutgoingCitationsUS'].astype(int) + patents['OutgoingCitationsForeign'].astype(int)) / patentStats['numClaims'].astype(int)
	# outgoing normalized citation
	patents['outgoingNC'] = patents.apply(lambda x: (x.numOutgoingCitationsUS + x.numOutgoingCitationsForeign) / outCitesPerYear[x.grantYear])
	# incoming normalized citation
	patents['incomingNC'] = patents.apply(lambda x: x.numIncomingCitationsUS / inCitesPerYear[x.grantYear])
	# Diversity
	patents['diversity'] = 1 - (patents.numIncomingCitationsUS / allIncomingCites)**2
	# Radicalness
	patents['radicalness'] = 1 - (patents.numIncomingCitationsUS / allIncomingCites)
	# outgoingNC rank
	patents['outgoingNCRank'] = patents.outgoingNC.rank(pct=True)
	# incomingNC rank
	patents['incomingNCRank'] = patents.incomingNC.rank(pct=True)
'''

def get_patents(cleanDataDir):
	patents = pd.read_csv(cleanDataDir + 'patent.tsv', sep = '\t', dtype = str, parse_dates = ['grantDate'])
	# This will split the date based on '-' char and collect the first part (the year) ex: 2019-07-29
	patents['grantYear'] = patents['grantDate'].str.split('-', n = 1, expand = True)[0]
	#patents['grantYear'] = pd.to_datetime(patents['grantDate'], yearfirst=True).dt.year
	return (patents)

def get_citation_counts(patents):
	# Help from: https://stackoverflow.com/questions/29836477/pandas-create-new-column-with-count-from-groupby

	allOutgoingCites = (patents['outgoingCitationsUS'] + patents['outgoingCitationsForeign']).sum()
	allIncomingCites = patents['incomingCitationsUS'].sum()

	inCitesPerYear = {}
	outCitesPerYear = {}

	yearGroups = patents.groupby('grantYear')
	for year, data in yearGroups:
		inCitesPerYear[year] = (patents['outgoingCitationsUS'] + patents['outgoingCitationsForeign']).sum()
		outCitesPerYear[year] = patents['incomingCitationsUS'].sum()
	return (allOutgoingCites, allIncomingCites, inCitesPerYear, outCitesPerYear)
