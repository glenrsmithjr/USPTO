from requirements import *

def print_status(status):
	print (status)
	sys.stdout.flush()

def print_processing_status(table_name):
	print_status ('PROCESSING ' + table_name.upper() + ' TABLE(S)...')

def print_uploading_status(table_name):
	print_status ('UPLOADING ' + table_name.upper() + ' TABLE(S)...')