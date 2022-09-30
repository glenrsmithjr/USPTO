from requirements import *
# HAVE TO REMOVE THE FIRST FEW LINES OF THE MaintFeeEvents FILE BECAUSE THEY'RE JUNK
def process_maintenance(cleanDataDir, filePath_mfe, filePath_mfe_desc):
	#patentMap = pd.read_csv(cleanDataDir + 'patentMap.tsv', sep = '\t', dtype = str)
	# Set engine to python (instead of default C parser because of the regex separator)
	maintEvents = pd.read_csv(filePath_mfe, sep = '\s', dtype = str, engine = "python",
			names = ['patent_id', 'applicationNumber', 'smallEntity', 'appFileDate', 'grantDate', 'maintFeeDate', 'maintFeeCode'])

	maintFeeCode(cleanDataDir, filePath_mfe_desc)
	pme = patentMaintEvent(cleanDataDir, maintEvents)
	patentMaint(cleanDataDir, pme)

def maintFeeCode(cleanDataDir, filePath_mfe_desc):
	with open(filePath_mfe_desc, 'r') as file:
		lines = file.readlines()
	# Split on first space only
	lines = [x.split(' ', 1) for x in lines]
	# Convert to dataframe
	temp = pd.DataFrame(lines, columns = ['maintFeeCode', 'maintFeeCodeDesc'])
	temp['maintFeeCodeDesc'] = temp['maintFeeCodeDesc'].str.lower().str.strip()
	# Fill na values
	temp[['maintFeeCode', 'maintFeeCodeDesc']] = temp[['maintFeeCode', 'maintFeeCodeDesc']].fillna('-')
	
	temp.to_csv(cleanDataDir + 'maintFeeCode.tsv', sep = '\t', index=False)

def patentMaintEvent(cleanDataDir, maintEvents):
	# Remove leading zeros from patent IDs
	maintEvents['patent_id'] = maintEvents['patent_id'].str.lstrip('0')
	# Convert date columns to nicer format (ex: 20190709 -> 2019-07-09)
	#toConvert = ['appFileDate', 'grantDate', 'maintFeeDate']
	#for col in toConvert:
	#	maintEvents[col] = pd.to_datetime(maintEvents[col], errors = "coerce")
	# Join in patent IDs (in-house-generated)
	#temp = maintEvents.merge(patentMap, on = 'patent_id', how = 'inner')
	# Rename patent_i columns
	maintEvents = maintEvents.rename(columns = {'patent_id': 'patentId'})
	# Select subset of fields
	maintEvents = maintEvents[['patentId', 'smallEntity', 'maintFeeDate', 'maintFeeCode']]
	# Fill na values
	maintEvents[['smallEntity', 'maintFeeCode']] = maintEvents[['smallEntity', 'maintFeeCode']].fillna('-')
	maintEvents['maintFeeDate'] = maintEvents['maintFeeDate'].fillna('1000-01-01')

	maintEvents.to_csv(cleanDataDir + 'patentMaintEvent.tsv', sep = '\t', index=False)
	# The patentMaintEvent table is needed to created the patentMaint table
	return (maintEvents)

def patentMaint(cleanDataDir, patentMaintEvent):
	temp = patentMaintEvent[['patentId', 'maintFeeCode', 'maintFeeDate']]
	# Create new column that maps the code EXP. to -1 (patent has expired) or EXPX to 1 (patent has expired, but has been reinstated)
	# Values of 0 means that code doesn't matter
	# First, use codes from maintFeeCode column and replace with -1 or 1 in expiryCode column. Other string codes will not be mapped
	# and just brought over into expiryCode column
	temp['expiryCode'] = temp['maintFeeCode'].replace({'EXP.': -1, 'EXPX': 1})
	# Then, use the types (1, -1 are ints, codes are strings) to map codes to values of 0
	temp['expiryCode'] = [val if type(val) == int else 0 for val in temp['expiryCode']]
	'''
		Sum the expired and reinstated values for each patent. If the value is < 0, then the patent is expired, because that patent has one
		more "expired" code than "reinstated" codes (any one patent can have exactly one more "expired" code than "reinstated" codes, but not more
		"reinstated" codes than "expired" codes)
	'''
	# Sums of expiry codes to determine if patent is expired or not
	expSum = temp.groupby('patentId', as_index=False)['expiryCode'].sum()
	# Map sums to values 0 and 1 
	expSum['isMaintained'] = [0 if value < 0 else 1 for value in expSum['expiryCode']]
	# Join in the new values
	temp = temp.merge(expSum, on = 'patentId', how = 'left')
	# Collect relevant columns to compute lastPayment (days since last payment)
	patentMaint = temp[['patentId', 'maintFeeDate', 'isMaintained']]
	

	# Next, need to select only most recent payment per patentId
	# Here, we can groupby both patentId and isMaintained since by design, this value will be the same for all
	# entries of the patentId, even if that particular entry specifies an "expired" event
	patentMaint = patentMaint.groupby(['patentId', 'isMaintained'], as_index=False)['maintFeeDate'].max()
	# Finally, need to calculate number of days since last payment
	today = datetime.datetime.today()

	patentMaint['maintFeeDate'] = pd.to_datetime(patentMaint['maintFeeDate'], errors = "coerce", yearfirst=True)
	# Calculate days since last payment
	patentMaint['daysSinceLastPayment'] = [(today - feeDate).days for feeDate in patentMaint['maintFeeDate']]
	patentMaint = patentMaint.rename(columns = {'maintFeeDate': 'lastMaintFeeDate'})

	# Fill na values
	patentMaint[['isMaintained', 'daysSinceLastPayment']] = patentMaint[['isMaintained', 'daysSinceLastPayment']].fillna(-1)
	patentMaint['lastMaintFeeDate'] = patentMaint['lastMaintFeeDate'].fillna('1000-01-01')

	patentMaint.to_csv(cleanDataDir + 'patentMaint.tsv', sep = '\t', index=False)

