creds = {
	"account": "snowflake account name",
	"warehouse": "",
	"database": "",
	"schema": "",
	"user": "",
	# currently this is only equipped to handle single files within zip folders, or one csv/txt file
	"infile": "full filepath to .zip, .csv, or .txt file",
	".zip format": True,
		"staging_folder_for_filechunks": "name for local staging folder",
		"remove_local_staging_filechunks": True,
		"delete_staging_folder_after_process": True,
		"filechunks_exist": False,
		"filename_in_zip": "if the file is compressed, the file name within the .zip folder",
		"rows_to_read_for_chunking": 1000000,
		"field_delimiter": ",",
		"record_delimiter": "\n",
	"snow_table": "table in snowflake to create/replace",
	"create_new_if_table_exists": False,
	"replace_if_table_exists": True
	}