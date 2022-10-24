from pysql.requirements import *
from sqlalchemy import Table, Column, insert, MetaData, String, Text

def connect(host, port, user, passwd, dbType, dbName):
	if dbType == 'mysql':
		engine = sql.create_engine('mysql://' + user + ':' + passwd + '@' + host + ':' + port)
	if dbType == 'postgres':
		print ('postgresql+psycopg2://' + user + ':' + passwd + '@' + host + ':' + port + '/' + dbName)
		#exit(0)
		engine = sql.create_engine('postgresql+psycopg2://' + user + ':' + passwd + '@' + host + ':' + port + '/')

	return engine.connect()

def create_database(connection, dbName):
	# Remove the database if it already exists
	connection.execute("DROP DATABASE IF EXISTS " + dbName)
	# Recreate database (if not exists just to be sure?)
	connection.execute("CREATE DATABASE IF NOT EXISTS " + dbName)

def delete_table(connection, tableName):
	connection.execute("DROP TABLE IF EXISTS " + tableName)

def disconnect(db_object):
	db_object.close()

#def insert(data, tableName, connection, if_exists = 'replace'):
#	data.to_sql(name = tableName, con = connection, if_exists = if_exists, index=False, chunksize = 10000)

def insert_data(data, table_name, connection):
	meta = MetaData()
	columns = [Column(c, Text) for c in data.columns]
	t = Table(table_name, meta, *columns)
	meta.create_all(connection)

	data = data.to_dict(orient="records")
	count = 0
	max_records = 5000
	while count < len(data):
		chunk = data[count:count+max_records]
		statement = t.insert().values(chunk)
		connection.execute(statement)
		count += max_records


def select(dbName, connection, bool_query, columns_to_search, columns_to_return, columnNames = None, baseTable = None, joins = None, groupby = None, limit = 1000, query_only=False):
	sqlQuery = _build_sql_query(bool_query, baseTable, columns_to_search, columns_to_return, joins, groupby, limit)
	if query_only:
		return sqlQuery
	# Execute query
	res = connection.execute(sqlQuery)
	# Collect results in list
	resList = [row for row in res]
	# If no preferred column names are given, derive names from columns in "columns_to_return"
	if columnNames is None:
		columnNames = [column.split(".")[-1] for column in columns_to_return]
	# Return data as a pandas dataframe
	return (pd.DataFrame(resList, columns = columnNames))

def use(connection, dbName):
	connection.execute('USE ' + dbName)

def _build_sql_query(bool_query, baseTable, columns_to_search, columns_to_return, joins, groupby, limit):
	#if not _validate()

	# Add in columns to be returned
	full_query = _build_sql_query_return_columns(baseTable, columns_to_return)
	# Add in any joins to the query
	full_query = _build_sql_query_joins(full_query, joins)
	# Add WHERE clause
	full_query = _build_sql_query_where(full_query, bool_query, columns_to_search)
	# Add groupby statements
	full_query = _build_sql_query_groupby(full_query, groupby)

	full_query = _build_sql_query_limit(full_query, limit)

	return (full_query)

def _build_sql_query_limit(full_query, limit):
	return (full_query + " LIMIT " + str(limit))

def _build_sql_query_return_columns(baseTable, columns_to_return):
	query = "SELECT "
	for column in columns_to_return:
		query += column + ","
	query = query.rstrip(",")
	# If no base table is provided, it is assumed to be the first table entry in the list of columns to return
	if baseTable is None:
		query += " FROM " + columns_to_search[0].split('.')[0]
	else:
		query += " FROM " + baseTable
	return (query)

def _build_sql_query_joins(query, joins):
	if joins is None:
		return (query)

	for join, column1, column2 in joins:
		query +=  " " + join.upper() + " JOIN " + column2.split(".")[0] + " ON " + column1 + " = " + column2
	return (query)

def _build_sql_query_where(query, bool_query, columns_to_search):
	# Remove parenthesis 
	rawTerms = bool_query.replace("(", "").replace(")", "")
	# Split on AND operators to remove them
	noAnds = rawTerms.split('AND')
	# Split on OR operators to remove them
	noAndOrs = [i.split('OR') for i in noAnds]
	# "noAndOrds" is a nested list that needs to be converted to one list
	terms = [item.strip() for sublist in noAndOrs for item in sublist]

	# Begin the query with a parenthesis (even if the original boolean query already has one)
	query += " WHERE "
	# This must be done for each column in the list
	for column in columns_to_search:
		query += "("
		# We want to preserve the user-defined query 
		temp = bool_query
		# For each term in the "terms" list, use it to format the sql query string
		for term in terms:
			temp = temp.replace(term, column +  " LIKE '%%" + term + "%%'")
		query += temp + ") OR "
	# Remove final OR
	query = query.rstrip(" OR")
	return (query)

def _build_sql_query_groupby(query, groupby):
	if groupby is None:
		return (query)
	query += " GROUP BY "
	for group in groupby:
		query += group + ","
	query = query.rstrip(",")
	return (query) 

# Defines a custom error class
class InputError(Exception):
	pass