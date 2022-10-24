# Import required packages
from requirements import *
# Import utility functions
import utils
from pysql import database
import psycopg2

def upload_to_mysql(cleanDataDir):
	# Get DB connection
	creds = json.loads(open("credentials.json").read())
	con = database.connect(**creds)
	database.create_database(con, creds["dbName"])
	database.use(con, creds["dbName"])

	# Get name of files to upload
	files = [cleanDataDir + file for file in os.listdir(cleanDataDir) if 'claim' not in file and 'application' not in file and 'assignee' not in file]

	for file in files:
		table_name = file.split('/')[-1].split('.')[0]
		database.delete_table(con, table_name)
		print(f"UPLOADING TABLE: {table_name}")

		df=pd.read_csv(file, dtype=str, sep='\t')
		df.replace({None: ""}, inplace=True)
		df.fillna("", inplace=True)
		#print(df.to_records(index=False))
		database.insert_data(df, table_name, con)




