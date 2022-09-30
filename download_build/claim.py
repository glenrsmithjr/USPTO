from requirements import *
def process_claims(cleanDataDir, claim_files):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep ='\t', dtype = str)
	for file in claim_files:
		claims = pd.read_csv(file, sep = '\t', dtype = str, usecols = ['patent_id', 'dependent', 'sequence', 'claim_number', 'exemplary'])
		# Assign unique IDs to each claim based on uuid
		# Makes identifying claims easier
		#claims['claimId'] = claims.groupby(['uuid']).ngroup()

		claims_table(cleanDataDir, claims)

def claims_table(cleanDataDir, claims):
	# Join these two tables to get the (in-house-generated) patent IDs
	#claims = claims.merge(patentMap, on = 'patent_id', how = 'inner')
	# Rename patent__id field
	claims = claims.rename(columns = {'patent_id': 'patentId'})
	# Select subset of fields
	#claims = claims[['patentId', 'dependent', 'sequence', 'exemplary']]
	# Fill na values
	#claims['exemplary'] = claims['exemplary'].fillna('-')
	#claims['sequence'] = claims['sequence'].fillna(-1)
	# Since -1 is used if a claim is independent, -2 is used for na values
	#claims['dependent'] = claims['dependent'].fillna(-2)
	#claims['exemplary'] = claims['exemplary'].str.lower().replace({'false': 0, 'true': 1})
	# Save data
	path = pathlib.Path(cleanDataDir + 'claim.tsv')
	if path.exists():
		claims.to_csv(cleanDataDir + 'claim.tsv', sep = '\t', index=False, mode='a', header=None)
	else:
		claims.to_csv(cleanDataDir + 'claim.tsv', sep='\t', index=False)
