ES_INDEX_USPTO_PATENTS = 'uspto_patents'

MAPPINGS_USPTO_PATENTS = [
	("patentId", {"type": "text"}),
	("grantDate", {"type": "date"}),
	("grantDateString", {"type": "text"}),
	("appFileDate", {"type": "date"}),
	("appFileDateString", {"type": "text"}),
	("patentTitle", {"type": "text"}),
	("patentAbstract", {"type": "text"}),
	("patentKind", {"type": "text"}),
	("patentType", {"type": "integer"}),
	("numClaims", {"type": "text"}),
	("grantYear", {"type": "integer"}),
	#("incomingCitationsUS", {"type": "integer"}),
	#("outgoingCitationsUS", {"type": "integer"}),
	#("outgoingCitationsForeign", {"type": "integer"}),
	#("citesPerClaim", {"type": "float"}),
	#("incomingNC_", {"type": "float"}),
	#("diversity", {"type": "float"}),
	#("radicalness", {"type": "float"}),
	#("incomingNCIRank", {"type": "float"}),
	#("diversityRank", {"type": "float"}),
	#("radicalnessRank", {"type": "float"}),
	#("citesPerClaimRank", {"type": "float"}),

	("appId", {"type": "text"}),
	("appNum", {"type": "text"}),
	("seriesCode", {"type": "text"}),

	("isMaintained", {"type": "text"}),
	("lastMaintFeeDate", {"type": "date"}),
	("daysSinceLastPayment", {"type": "integer"}),

	("citationsUS", {"type": "text"}), # List
	("citationsForeign", {"type": "text"}), # List

	("contractNumbers", {"type": "text"}), # List

	("govOrgStatement", {"type": "text"}),

	("inventors", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"inventorId": {"type": "text"},
		"firstName": {"type": "text"},
		"lastName": {"type": "text"},
		"lat": {"type": "float"},
		"long": {"type": "float"},
		"city": {"type": "text"},
		"state": {"type": "text"},
		"country": {"type": "text"},
		"county": {"type": "text"},
		"stateFips": {"type": "text"},
		"countyFips": {"type": "text"}}}),
	# Combine patentAssignee and assigneeType tables
	("assignees", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"assigneeId": {"type": "text"},
		"firstName": {"type": "text"},
		"lastName": {"type": "text"},
		"assigneeType": {"type": "text"},
		"organization": {"type": "text"},
		"lat": {"type": "float"},
		"long": {"type": "float"},
		"city": {"type": "text"},
		"state": {"type": "text"},
		"country": {"type": "text"},
		"county": {"type": "text"},
		"stateFips": {"type": "text"},
		"countyFips": {"type": "text"}}}),

	("claims", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"dependent": {"type": "text"},
		"sequence": {"type": "text"},
		"exemplary": {"type": "text"}}}),

	("cpc", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"sequence": {"type": "integer"},
		"cpcSectionId": {"type": "text"},
		"cpcSubSectionId": {"type": "text"},
		"cpcGroupId": {"type": "text"},
		"cpcSubGroupId": {"type": "text"},
		"subGroupPrime": {"type": "text"},
		"category": {"type": "text"}}}),

	("ipc", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"sequence": {"type": "integer"},
		"ipcSectionId": {"type": "text"},
		"ipcClassId": {"type": "text"},
		"ipcSubClassId": {"type": "text"},
		"ipcGroupId": {"type": "text"},
		"ipcSubGroupId": {"type": "text"},
		"symbolPosition": {"type": "text"},
		"classValue": {"type": "text"},
		"classStatus": {"type": "text"},
		"dataSource": {"type": "text"},
		"actionDate": {"type": "text"},
		"ipcVersion": {"type": "text"}}}),

	# Combine patentGovOrg, GovOrg
	("govInterests", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"govOrgId": {"type": "text"},
		"govOrgName": {"type": "text"},
		"levelOne": {"type": "text"},
		"levelTwo": {"type": "text"},
		"levelThree": {"type": "text"}}}),
	# Combine patentWipo, wipoField
	("wipoFields", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"sequence": {"type": "integer"},
		"sectorDescription": {"type": "text"},
		"fieldDescription": {"type": "text"}}}),

	("foreignPriorities", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"priorityType": {"type": "text"},
		"sequence": {"type": "integer"},
		"foreignAppNum": {"type": "text"},
		"foreignAppDate": {"type": "date"},
		"foreignCountry": {"type": "text"}}}),

	("otherReferences", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"text": {"type": "text"},
		"sequence": {"type": "integer"}}}),

	("nber", {"type": "nested", "properties": {
		"patentId": {"type": "text"},
		"nberCategory": {"type": "text"},
		"nberSubCategory": {"type": "text"}}})
]