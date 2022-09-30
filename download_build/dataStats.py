import pandas as pd
# Quality Statistics for USPTO data
cleanDataDir = 'clean-data/'

patents = pd.read_csv(cleanDataDir + 'patent.tsv', sep = '\t', usecols = ['patentId'])
uniquePatents = patents.patentId.unique()
numUniquePatents = len(uniquePatents)

statsList = []

##################
# Citation Stats #
##################

citations = pd.read_csv(cleanDataDir + 'usCitations.tsv', sep = '\t', usecols = ['outgoingCitationId', 'incomingCitationId'])
# Get outgoing citations
outgoingCites = citations.outgoingCitationId.unique()
numOutgoingCites = len(outgoingCites)
# Get incoming citations
incomingCites = citations.incomingCitationId.unique()
numIncomingCites = len(incomingCites)

# Get number of outgoing citation patents that exist in citations table and patents table
outgoingCitesMatch = len( list( set(outgoingCites) & set(uniquePatents) ) )
outgoingCitesPercentage = round( (outgoingCitesMatch / numUniquePatents ) * 100, 2)
incomingCitesMatch = len( list( set(incomingCites) & set(uniquePatents) ) )
incomingCitesPercentage = round( (incomingCitesMatch / numUniquePatents ) * 100, 2)

statsList.append('CITATIONS')
statsList.append(str(outgoingCitesMatch) + '/' + str(numUniquePatents) + ' patents have outgoing citations: ' + str(outgoingCitesPercentage) + '%')
statsList.append(str(incomingCitesMatch) + '/' + str(numUniquePatents) + ' patents have incoming citations: ' + str(incomingCitesPercentage) + '%') 

########################
# Classification Stats #
########################

cpc = pd.read_csv(cleanDataDir + 'patentCPC.tsv', sep = '\t', usecols = ['patentId'])
# Get patents that have classifications in classifications table
classifiedPatents = cpc.patentId.unique()
# Get number of patents that have classificartions in classifications table and also exist in patents table
numPatentsWithClassification = len( list( set(uniquePatents) & set(classifiedPatents) ) )
# Percentage of num patents with classifications out of total number of patents
classifiedPercentage = round( (numPatentsWithClassification / numUniquePatents ) * 100, 2)
statsList.append('CLASSIFICATION')
statsList.append(str(numPatentsWithClassification) + '/' + str(numUniquePatents) + ' patents have classifications: ' + str(classifiedPercentage) + '%') 

# Print stats
for string in statsList:
	print (string)