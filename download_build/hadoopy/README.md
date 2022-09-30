Hadoopy is a template package that allows a user to inferface with the hadoop cluster on the ISAD servers.

Browse the filesystem by going here: http://192.168.10.105:7000 -> Utilities (top nav bar) -> Browse the file system

**NOTE: Please do not delete or edit anything in the "rmstate" directory. This directory stores pertinent information about the cluster.**

- [Using the Code](#using-the-code)
- [Package Functions](#package-functions)

# Using the Code

Before you can use these functions to interface with hadoop, you need to have a few modules installed: [json, pandas, requests, sys]

Some of these come packaged with python, the others will need to be installed via whatever method you use to install modules in your environment. Usually, this is through pip. Ex: `pip install pandas`

To use the hadoopy module:

1. Copy the "hadoopy" directory (containing the hadoopy.py file and requirements folder) into the same directory as your script or shell.
2. If using a shell, run the command `from hadoopy.hadoopy import *`. If using a script, place the command at the top of your file.

This will import all of the functions from the hadoopy module and all necessary requirements.

Note: If using this package on the ISAD servers in a container, use it on servers 2-6. You will not be able to use it on ISAD 1 (192.168.10.105) since that IP is used to access hadoop, and containers on ISAD 1 can't access ISAD 1 via its IP address.

# Package Functions

Below outlines the functions available to the user (to be expanded).

| Function | Description | Parameters | Examples |
|----------|-------------|------------|----------|
| hadoop_append | Appends data to an existing file the hadoop DFS | **hadoop\_path** (string): Location (full path) of file to append to in hadoop. This must begin with a forward slash but end without one. <br> **file** (string or Dataframe): Location (full or relative path) of the file to append to the hadoop file, or a Pandas DataFrame object <br> **fileType** (string): Either 'string' or 'dataframe' depending on what is passed in| **hadoop\_path**: '/data/sbir/sbir_data1.json' <br> **file**: DataFrame <br> **filetype** 'dataframe'|
| hadoop_delete | Deletes a file or directory in the hadoop DFS | **hadoop\_path** (string): Location (full path) of file or directory to delete in hadoop. This must begin with a forward slash but end without one. | **hadoop\_path**: See _hadoop_append_ |
| hadoop_listdir | Lists the contents of a directory in the hadoop DFS | **hadoop\_path** (string): Location (full path) of directory to collect in hadoop. <br> **verbose** (boolean) Whether to return all information about files found in the directory or just the filenames. Default: False. <br> If verbose is True, the function returns a list of dictionary objects, one for each file or directory in the listed directory. For each entry of the list, the "length" field is in bytes, the "pathSuffix" field names the file or directory, and the "type" field shows if it is a file or directory. | **hadoop\_path**: '/data/sbir' <br> **verbose**: True
| hadoop_mkdir | Creates a new directory in the hadoop DFS. All parent directories are created as well. | **hadoop\_path** (string): Location (full path) of directory to create in hadoop. This must begin with a forward slash but end without one. | **hadoop\_path** : '/data/newFolder' | 
| hadoop_mread | Read multiple files from a hadoop directory. The files are saved directly to the specified directory (download_dir) and will share the filenames as they are in the hadoop DFS | **hadoop\_path** (string): Location (full path) of file to read from in hadoop. This must begin with a forward slash but end without one. <br> **download\_dir** (string): Location (full or relative path) of directory to save the data locally. <br> **files** (list): A list of string filenames to collect from hadoop. If this value is not provided, all files in the hadoop directory are downloaded. Default: None | **hadoop\_path**: '/data/uspto/uspto-clean' <br> **download\_dir**: 'myData/' <br> **files**: ['patent.tsv', 'patentAbstract.tsv']
| hadoop_read | Reads a file from the hadoop DFS | **hadoop\_path** (string): Location (full path) of file to read from in hadoop. This must begin with a forward slash but end without one. <br> **save\_to\_disk** (boolean): Whether to save the file to disk. Default: False <br> **filename** (string): Location (full or relative path) of where to save the data locally. If you provide "True" to _save_to_disk_ then you must provide a value here or you will recieve an error. Default: None <br> **dtypes** (dictionary): Optional parameter to specify dtypes of columns in data to be read in. Can be used if you are reading in a csv or tsv file. <br> **nrows** (integer): The number of rows to read in. This is useful if you do not want to read in an entire file. This can only be used when reading in a csv or tsv file. This cannot be used when saving a file directly to disk.  | **hadoop\_path**: '/data/arxiv/test.csv' <br> **save\_to\_disk**: True <br> **filename**: 'test.csv' <br> **dtypes**: {'column1': str}  <br> **nrows**: 10000 |
| hadoop_rename | Renames a file or directory in the hadoop DFS | **source** (string): Location (full path) of file or directory to rename in hadoop. This must begin with a forward slash but end without one. <br> **destination** (string): Location (full path) of renamed file or directory in hadoop. This must begin with a forward slash but end without one. | **source**: '/data/sbir/sbir1.txt' <br> **destination**: '/data/sbir/sbirOld.txt' |
| hadoop_write | Writes a file to the hadoop DFS | **hadoop\_path** (string): Location (full path) of file to write to in hadoop. This must begin with a forward slash, end without one, and include the name of the file to write with its file extension. The filename used to store the data in hadoop need not be the same name as the file the data is coming from. <br> **file** (string or Dataframe): See _hadoop_append_ <br> **fileType** (string): See _hadoop_append_ <br> **overwrite** (boolean): Whether to overwrite the file in the hadoop dfs if it already exists. Default: False | **hadoop\_path**: '/data/sbir/sbir1.txt' <br> **file**: 'sbir_data.txt' <br> **overwrite**: True <br> **filetype** 'string'
