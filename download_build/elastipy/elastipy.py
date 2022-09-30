from elastipy.requirements import *
import elastipy.utils as utils
#######################
# INTERFACE FUNCTIONS #
#######################

def es_connect(host = ES_HOST, port = ES_PORT):
	# Connect to ES instance at HOST and PORT
	es = Elasticsearch(host = host, port = port, timeout=1800)
	return (es)

# ADD MORE ERROR HANDLING
def create_index(es, index, field_mappings = None, fields = None, ignore_malformed=False, num_shards=1, num_replicas=1):
	mapping = utils._validate_mappings(fields, field_mappings, index, ignore_malformed, num_shards, num_replicas)
	# Create index with these mappings
	es.indices.create(index, body=mapping)

def delete_index(es, index):
	es.indices.delete(index)

def index_exists(es, index):
	return (es.indices.exists(index))

# id_counter keeps a global record of the ids so that no ids are overwritten
def bulk_index_data(es, index, data, id_counter=0, use_column_ids = False, id_column = None):
	utils._validate_indexing_params(use_column_ids, id_column)
	try:
		#dataType = '_doc'
		# Convert dataframe to JSON
		json_parsed = json.loads(data.to_json(orient='records'))
		# Delete data object
		del data
		gc.collect()

		# Prepare json docs in format to be bulk-indexed into elasticsearch
		for doc in json_parsed:
			doc['_index'] = index
			#doc['_type'] = dataType
			if use_column_ids:
				doc['_id'] = doc[id_column]
			else:
				doc['_id'] = id_counter
				id_counter += 1

		helpers.bulk(es, json_parsed, chunk_size=1000, request_timeout=1000)
	except Exception as e: 
		print ('ERROR IN: elastipy.bulk_index_data(). There was an error indexing data to elasticsearch. Printing error...')
		print(e)

# id_counter keeps a global record of the ids so that no ids are overwritten
def bulk_update_data(es, index, data, id_column):
	try:
		# Get fields to update
		update_fields = _get_fields_to_update(id_column, list(data.columns))
		# Convert dataframe to JSON
		json_parsed = json.loads(data.to_json(orient='records'))
		# Delete data object
		del data
		gc.collect()

		for doc in json_parsed:
			# Set operation type to update
			doc['_op_type'] = 'update'
			doc['_index'] = index

			doc['_id'] = doc[id_column]
			# Remove id field
			del doc[id_column]
			doc['doc'] = {}
			# Put data to be updated in update field
			for field in update_fields:
				# Add data to doc field
				doc['doc'][field] = doc[field]
				# Remove field from first level of dictionary
				del doc[field]
		# Bulk update data
		helpers.bulk(es, json_parsed, chunk_size=1000, request_timeout=1000, raise_on_error=False)
	except Exception as e:
		print ('ERROR IN: elastipy.bulk_update_data(). There was an error indexing data to elasticsearch. Printing error...')
		print(e)

def _get_fields_to_update(id_column, fields):
	fields.remove(id_column)
	return fields


####################
# SEARCH FUNCTIONS #
####################

# Search function used to search elasticsearch with a boolean query
def boolean_search(index, fields_to_search, query, fields_to_return = 'all', es = None, result_window=10000, verbose=False, scroll=False, max_results=100000, stream_to_file = False, filename = None):
	try:
		if es is None:
			es = es_connect()
		if scroll:
			return utils._search_scroll(es, 
										index, 
										result_window, 
										utils._get_boolean_query(fields_to_search, query, fields_to_return), 
										max_results,
										stream_to_file=stream_to_file,
										filename=filename)
		else:
			return utils._search(es, 
								 index, 
								 result_window, 
								 utils._get_boolean_query(fields_to_search, query, fields_to_return), 
								 verbose,
								stream_to_file=stream_to_file,
								filename=filename)
		
	except Exception as e: 
		print ('ERROR IN: elastipy.boolean_search(). Printing error...')
		print(e)

# Search function used to search elasticsearch with any user defined query
def custom_search(index, query, es = None, fields_to_return = 'all', result_window=10000, verbose=False, scroll=False, max_results = 100000, stream_to_file=False, filename=None):
	try:
		if es is None:
			es = es_connect()
		if scroll:
			return utils._search_scroll(es, 
										index, 
										result_window, 
										utils._add_fields_to_return(query, fields_to_return), 
										max_results,
										stream_to_file=stream_to_file,
										filename=filename)
		else:
			return utils._search(es, 
								 index, 
								 result_window, 
								 utils._add_fields_to_return(query, fields_to_return),
								 verbose,
								 stream_to_file=stream_to_file,
								 filename=filename)
	except Exception as e: 
		print ('ERROR IN: elastipy.custom_search(). Printing error...')
		print(e)

# Function to get count of all documents returned by user-defined query
def get_count(index, query, es=None):
	try:
		if es is None:
			es = es_connect()
		return utils._get_count(es, index, query)
	except Exception as e: 
		print ('ERROR IN: elastipy.get_count(). Printing error...')
		print(e)


# Function to get count of all documents in index
def get_total_count(index, es=None):
	try:
		if es is None:
			es = es_connect()
		return utils._get_count(es, index, utils._get_total_count_query())
	except Exception as e: 
		print ('ERROR IN: elastipy.get_total_count(). Printing error...')
		print(e)

# Function to pull all documents from an index
def search_all(index, es = None, fields_to_return = 'all', stream_to_file = False, filename = None):
	try:
		if es is None:
			es = es_connect()
		return utils._search_scroll(es,
							 index,
							 result_window=10000,
							 query=utils._get_match_all_query(fields_to_return),
							 max_results = -1,
							stream_to_file=stream_to_file,
							filename=filename)
	except Exception as e: 
		print ('ERROR IN: elastipy.search_all(). Printing error...')
		print(e)

# Search function used to search elasticsearch using cosine similarity vector comparisons
def vector_search(index, query_vector, vector_field_name, fields_to_return = 'all', es = None, result_window=10000, verbose=False, scroll=False, max_results = 100000, stream_to_file = False, filename=None):
	try:
		if es is None:
			es = es_connect()
		if scroll:
			return utils._search_scroll(es, 
										index, 
										result_window, 
										utils._get_vector_query(query_vector, vector_field_name, fields_to_return), 
										max_results,
										stream_to_file=stream_to_file,
										filename=filename)
		else:
			return utils._search(es, 
								 index, 
								 result_window, 
								 utils._get_vector_query(query_vector, vector_field_name, fields_to_return), 
								 verbose,
								stream_to_file=stream_to_file,
								filename=filename)
	except Exception as e: 
		print ('ERROR IN: elastipy.vector_search(). Printing error...')
		print(e)

############################
# QUERY BUILDING FUNCTIONS #
############################

# Utility function that builds a query for the user given the type of query requested
# This can be used to build a query before passing into get_count()
def get_query_by_type(query, query_type, fields_to_search = None, fields_to_return = 'all', query_vector = None, vector_field_name = None):
	if query_type == 'boolean':
		return utils._get_boolean_query(fields_to_search, query, fields_to_return)
	elif query_type == 'vector':
		return utils._get_vector_query(query_vector, vector_field_name, fields_to_return)
	elif query_type == 'search_all':
		return utils._get_match_all_query(fields_to_return)
