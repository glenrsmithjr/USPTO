utils.print_status('UPLOADING TO ELASTICSEARCH...')
# upload.upload_to_elasticsearch(data_dirs[1])

utils.print_uploading_status('patents')
indexData.index_patents('clean-data/patent.tsv', 'clean-data/patentType.tsv')

utils.print_uploading_status('applications')
indexData.index_table('clean-data/application.tsv', columns_to_use=['patentId', 'appId', 'seriesCode', 'appNum'])

utils.print_uploading_status('maintenance data')
indexData.index_table('clean-data/patentMaint.tsv')

utils.print_uploading_status('government interests')
indexData.index_gov_org('clean-data/govOrg.tsv', 'clean-data/patentGovOrg.tsv')

utils.print_uploading_status('patent wipo')
indexData.index_patent_wipo('clean-data/patentWipo.tsv', 'clean-data/wipoField.tsv')

utils.print_uploading_status('assignees')
indexData.index_assignees('clean-data/patentAssignee.tsv', 'clean-data/assigneeType.tsv')

utils.print_uploading_status('abstracts')
indexData.index_table('clean-data/patentAbstract.tsv')

utils.print_uploading_status('foreign citations')
indexData.index_table('clean-data/foreignCitations.tsv', convert_to_list=True, list_column='incomingCitationId',
                      converted_column_names=['patentId', 'citationsForeign'], group_by_column='outgoingCitationId')

utils.print_uploading_status('government contract numbers')
indexData.index_table('clean-data/patentGovOrgGrant.tsv', convert_to_list=True, list_column='contractNum',
                      converted_column_names=['patentId', 'contractNumbers'], group_by_column='patentId')

utils.print_uploading_status('government statements')
indexData.index_table('clean-data/patentGovOrgStatement.tsv')

utils.print_uploading_status('inventors')
indexData.index_table('clean-data/inventor.tsv', convert_to_dict=True, converted_column_names=['patentId', 'inventors'],
                      group_by_column='patentId')

utils.print_uploading_status('claims')
indexData.index_table('clean-data/claim.tsv', convert_to_dict=True, converted_column_names=['patentId', 'claims'],
                      group_by_column='patentId')

utils.print_uploading_status('cpc')
indexData.index_table('clean-data/patentCPC.tsv', convert_to_dict=True, converted_column_names=['patentId', 'cpc'],
                      group_by_column='patentId',
                      columns_to_use=['patentId', 'cpcSectionId', 'cpcSubSectionId', 'cpcGroupId',
                                      'cpcSubGroupId', 'category', 'sequence', 'subGroupPrime'])
utils.print_uploading_status('ipc')
indexData.index_table('clean-data/patentIPC.tsv', convert_to_dict=True, converted_column_names=['patentId', 'ipc'],
                      group_by_column='patentId')

utils.print_uploading_status('foreign priorities')
indexData.index_table('clean-data/foreignPriorities.tsv', convert_to_dict=True,
                      converted_column_names=['patentId', 'foreignPriorities'],
                      group_by_column='patentId')

utils.print_uploading_status('other references')
indexData.index_table('clean-data/otherReferences.tsv', convert_to_dict=True,
                      converted_column_names=['patentId', 'otherReferences'],
                      group_by_column='patentId')

utils.print_uploading_status('nber')
indexData.index_table('clean-data/nber.tsv', convert_to_dict=True, converted_column_names=['patentId', 'nber'],
                      group_by_column='patentId')

utils.print_uploading_status('us citations')
indexData.index_table('clean-data/usCitations.tsv', convert_to_list=True, list_column='incomingCitationId',
                      converted_column_names=['patentId', 'citationsUS'], group_by_column='outgoingCitationId')

utils.print_processing_status('DONE UPLOADING TO ELASTICSEARCH.')