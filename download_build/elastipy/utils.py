from elastipy.requirements import *

####################
# HELPER FUNCTIONS #
####################

# Add the specified fieldsa to return in a query
def _add_fields_to_return(query, fields_to_return):
	if type(fields_to_return) is list:
		query["_source"] = fields_to_return
	return query

# Returns field mappings to create index with
def _get_mapping(fields, index, types, ignore_malformed, num_shards, num_replicas):
	mapping = { "settings": {"index.mapping.ignore_malformed": ignore_malformed, "number_of_shards": num_shards, "number_of_replicas": num_replicas}, "mappings": { "properties": { } } } 

	if types == 'text':
		for field in fields:
			mapping['mappings']['properties'][field] = { "type": "text" }
	
	elif types == 'dynamic':
		for tupl in fields:
			mapping['mappings']['properties'][tupl[0]] = tupl[1]

	return (mapping)

def _get_boolean_query(fields_to_search, query, fields_to_return):
	query = {
		"track_total_hits": True,
		'query': {
			'bool': {
				'must': {
					'query_string': {
						'fields': fields_to_search,
						'query': query
					}
				}
			}
		}
	}
	query = _add_fields_to_return(query, fields_to_return)
	return query

def _get_vector_query(query_vector, vector_field_name, fields_to_return):
	query = {
			"track_total_hits": True,
				"query": {
				"script_score": {
					"query": {
						"match_all": {}
					},
					"script": {
						"source": "cosineSimilarity(params.queryVector, doc[" + vector_field_name + "])+1.0",
						"params": {
							"queryVector": query_vector 
						}
					}
				}
				}
			}
	query = _add_fields_to_return(query, fields_to_return)
	return query

def _get_match_all_query(fields_to_return):
	query = {
		"track_total_hits": True,
 	 	"query": {
			"match_all": {}
		}
	}
	# If user has specified which fields to return, add them to the query
	query = _add_fields_to_return(query, fields_to_return)
	return query

def _get_count(es, index, query):
	return es.search(index = index, size = 0, body = query)['hits']['total']['value']

def _get_total_count_query():
	query = {
		"track_total_hits": True,
		"query": {
			"match_all": {}
		}
	}
	return query

def _save_data_to_file(data, filename):
	path = Path(filename)
	sep = _save_data_to_file_get_separator(filename)
	if path.exists():
		data.to_csv(filename, sep = sep, mode='a', header=False, index=False)
	else:
		data.to_csv(filename, sep = sep, index=False)

def _save_data_to_file_get_separator(filename):
	ext = filename.split('.')[-1]
	if ext == 'tsv':
		return '\t'
	else:
		return ','

def _search(es, index, result_window, query, verbose, stream_to_file, filename):
	res = es.search(index = index, size = result_window, body = query)

	if verbose:
		# Return json object with all details
		return (res)
	else:
		if stream_to_file:
			_save_data_to_file( _process_results(res), filename )
			return None
		# Otherwise, just return dataframe of results
		return ( _process_results(res) )

def _search_scroll(es, index, result_window, query, max_results, stream_to_file, filename):
	# If max_results is -1, then all results will be collected
	if max_results < 0:
		max_results = es.search(index = index, size = result_window, body = _get_total_count_query())['hits']['total']['value']

	res = es.search(index = index, size = result_window, body = query, scroll = '2m')
	sid = res['_scroll_id']

	# Process first set of results
	df = _process_results(res)
	# Save data to file if requested by user
	if stream_to_file:
		_save_data_to_file(df, filename)
	# If there are no documents, return None
	if df is None:
		return None

	# If user puts in -1, max_results will continue to increase and all records
	# will be collected eventually
	max_results -= result_window
	while max_results > 0:
		res = es.scroll(scroll_id = sid, scroll = '2m')
		sid = res['_scroll_id']
		res = _process_results(res)

		# If user has requested to save data, only save and don't collect
		if stream_to_file:
			# If there are no more results, return None
			if res is None:
				return None
			_save_data_to_file(res, filename)
			# Decrease # of results left to collect
			max_results -= result_window
			continue

		# If there are no more results, return the dataframe
		if res is None:
			return df
		# Only add the number of results we want
		if max_results < result_window:
			df = df.append(res.head(max_results))
		else:
			# Otherwise, add it all
			df = df.append(res)
		# Decrease # of results left to collect
		max_results -= result_window
	# Return None if saving to file
	if stream_to_file:
		return None
	return df

def _process_results(results):
	if _validate_result_count(results):
		return pd.DataFrame( [ hit['_source'] for hit in results['hits']['hits'] ] ) 
	else:
		return None

def _validate_result_count(results):
	return (results['hits']['total']['value'] != 0)

def _validate_mappings(fields, field_mappings, index, ignore_malformed, num_shards, num_replicas):
	if field_mappings is None:
		if type(fields) is not list:
			raise InputError('In elastipy.utils._validate_mappings: If you provide no field mappings, the fields must be passed in a list object.')
		else:
			mapping = _get_mapping(fields, index, types = 'text', ignored_malformed=ignore_malformed, num_shards=num_shards, num_replicas=num_replicas)

	if type(field_mappings) is not list:
		raise InputError('In elastipy.utils._validate_mappings: The field mappings must be a list of tuples containing the string field name, and a dictionary of mapping values.')
	else:
		mapping = _get_mapping(field_mappings, index, types = 'dynamic', ignore_malformed=ignore_malformed, num_shards=num_shards, num_replicas=num_replicas)

	return mapping	



def _validate_indexing_params(use_column_ids, id_column):
	if use_column_ids is not True and use_column_ids is not False:
		raise InputError('In elastipy.utils._validate_indexing_params: The "use_column_ids" field must be a boolean value' )
	else:
		if not use_column_ids:
			return
		else:
			if id_column is None:
				raise InputError('In elastipy.utils._validate_indexing_params: If you elect to use custom id values, you must give the string column name in "id_column".' )
			
							
# Defines a custom error class
class InputError(Exception):
	pass