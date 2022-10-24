PySQL is a template package that allows a user to inferface with a postgres or mysql server on the ISAD servers.

See the [documentation](http://revan/isad/documentation#servers) for MySQL and Postgres on Revan for how to access the SQL servers using gui applications.

- [Using the Code](#using-the-code)
- [Package Functions](#package-functions)
- [Example Code](#example-code)

# Using the Code

Before you can use these functions to interface with a sql server, you need to have a few modules installed: 
[mysql.connector, pandas, sqlalchemy, sys]

Some of these come packaged with python, the others will need to be installed via whatever method you use to install modules in your environment. Usually, this is through pip. Ex: `pip install pandas`

To use the pysql module:

1. Copy the "pysql" directory (containing the database.py file, requirements folder, and README) into the same directory as your script or shell.
2. If using a shell, run the command `from pysql.database import *`. If using a script, place the command at the top of your file.

This will import all of the functions from the pysql module and all necessary requirements.

# Package Functions

Below outlines the functions available to the user (to be expanded).

| Function | Description | Parameters | Examples | Return |
|----------|-------------|------------|----------|--------|
| connect | Connect to a mysql or postgres server | **host** (string): The IP address of the host where the sql server resides <br> **port** (string): The port of the host where the sql server resides <br> **user** (string): The username of the user you wish to authenticate with <br> **passwd** (string): The password of the user you with to authenticate with <br> **dbType** (string): The type of sql server to connect to. This is one of {mysql, postgres} <br> **dbName** (string): The name of the database you wish to connect to. | **host**: '192.168.10.126' <br> **port**: '3305' <br> **user**: 'alice.bob' <br> **passwd**: 'alice$bob!' <br> **dbType**: 'mysql' <br> **dbName**: 'uspto' | Connection Object |
| create_database | Create a database given a connection object and database name | **connection** (Connection Object): Connection object created from calling the "connect" function <br> **dbName** (string): The name of the database you'd like to create | **connection**: Connection Object <br> **dbName**: 'myDbName' | N/A |
| disconnect | Disconnects from the sql server. Pass in the database connection object given from "connect" to properly close the connection | **db\_object** (Connection Object): Connection object to disconnect | **db\_object**: myConnection | N/A |
| insert | Inserts data into a database. Using this function, you do not need to create a table before insertion | **data** (Dataframe): The data to insert in the form of a pandas dataframe <br> **tableName** (string): The name of the table to create or append to when inserting the data <br> **connection** (Connection Object): The connection object returned from the "connect" function <br> **if\_exists** (string): What to do if the table already exists in the database. You can choose one of {replace, append, fail}. If you choose replace, the table will be dropped and recreated before inserting new data. If you choose append, the data is appended to an already-existing table. If you choose fail, a ValueError will be thrown. Default: replace | **data**: DataFrame <br> **tableName**: 'myTable' <br> **connection**: Connection Object <br> **if\_exists**: 'replace' | N/A |
| select | Generates a select statement and queries the database. The user provides a boolean query that is converted to a sql select statement. The boolean query is composed of "AND" and "OR" statements\*, and all terms on either side of these operators are considered as a phrase. For example, to search "Machine Learning" as an exact phrase, you would just need to type "Machine Learning". To search the words seperately but appearing in the same text, type "Machine AND Learning". \*This function does not currently support "NOT" operators| **dbName** (string): The name of the database to query <br> **connection** (Connection Object): The connection object returned from the "connect" function <br> **bool\_query** (string): A simple boolean query utilizing "AND" and "OR" operators <br> **columns\_to\_search** (list): A list of columns to perform the search on in the form of "[table_name].[column_name]" <br> **columns\_to\_return** (list): Same format as "columns_to_search". You can also specify column functions here. See examples <br> **columnNames** (list): The preferred names of the returned columns. If not provided, the names will be defaulted to the columns specified in "columns_to_return". Default: None <br> **baseTable** (string): The name of the table to select from. This table comes directly after the "FROM" keyword in a sql select statement. If not provided, it will be defaulted to the first table name provided in "columns_to_return". Default: None <br> **joins** (list): A list of three-tuples that contain the type of join as the first element of the tuple, the "left" table and column to join on in the form of "[table_name].[column_name]" as the second element, and the "right" table and column to join on in the form of "[table_name].[column_name]" as the third element. If you are selecting from multiple tables, you must provide a value to join those tables. Default: None <br> **groupby** (list): A list of columns to group by. Each column is in the form of "[table_name].[column_name]". Default: None <br> **limit** (integer): Limits the resultset to this value. Default: 1000 <br> **query\_only** (boolean): If set to true, function only returns string representation of sql query and does not query database. Default: False | **dbName**: 'myDb' <br> **connection**: Connection Object <br> **bool\_query**: '(machine AND learning) OR computer vision' <br> **columns\_to\_search**: ['patent.title', 'patentAbstract.abstract'] <br> **columns\_to\_return**: ['year(patent.pubDate)', 'count(*)'] <br> **columnNames**: ['year', 'patentCounts'] <br> **baseTable**: 'patent' <br> **joins**: [('inner', 'patent.patentId', patentAbstract.patentId)] <br> **groupby**: ['year(patent.pubDate)'] <br> **limit**: 5000 | DataFrame |
| use | Select which database to use | **connection** (Connection Object): The Connection Object returned from the "connect" function <br> **dbName** (string): The name of the database to use | **connection**: Connection Object <br> **dbName**: 'myDb' | N/A |

# Example Code

```python
# Import pysql package
from pysql.database import *

# Specify connection vars
host = "192.168.10.105"
port = "5432"
user = "newUser"
passwd = "newPwd"
dbType = "postgres"
dbName = "DATA WAREHOUSE"

# Create connection object
con = connect(host, port, user, passwd, dbType, dbName)

# Specify query vars
bool_query = "machine AND learning"
columns_to_search = ["ciqbusinessdescription.businessdescription"]
columns_to_return = ["ciqcompany.companyname", "ciqbusinessdescription.businessdescription"]
columnNames = ["companyname", "description"]
baseTable = "ciqcompany"
joins = [("inner", "ciqcompany.companyid", "ciqbusinessdescription.companyid")]
limit = 100

res = select(dbName, con, bool_query, columns_to_search, columns_to_return, columnNames, 
			 baseTable=baseTable, joins=joins, limit=limit)

print (res)
```

The preceding code produces the following SQL syntax:
```sql
SELECT ciqcompany.companyname,ciqbusinessdescription.businessdescription FROM ciqcompany 
INNER JOIN ciqbusinessdescription ON ciqcompany.companyid = ciqbusinessdescription.companyid 
WHERE (ciqbusinessdescription.businessdescription LIKE '%%machine%%' AND ciqbusinessdescription.businessdescription LIKE '%%learning%%') 
LIMIT 100
```