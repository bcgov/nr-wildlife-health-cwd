# TESTING:  Create an empty ArcGIS Online Hosted TABLE using field definitions from an xls Data Dictionary
# Then populate it with records from an xls file.
 
# Then can populate with data from xls or a pandas dataframe.  Populating from a pandas dataframe is working in the cwd_data_workflow.py script. 
# Look there for reference.  See 'def save_web_results' function.

# WARNING: Be careful using the delete function, as it may include items with similar names!!  TEST FIRST by listing found items.

# History:
# 2025-08-15:  Initialized by Sasha Lees
# 2025-12-15:  Testing - CREATING A HOSTED TABLE IS NOT WORKING DUE TO OBJECTID BUG??  HAVE TO CREATE HOSTED TABLE MANUALLY TO GET OID WORKING PROPERLY. 


# Create a hosted table and populate with records from XLS
# 
# Script Outline:
#  1. Connect to ArcGIS Online
#  2. Create dataframe from Excel records
#  3. Optional file cleanup to delete hosted table, and hosted xls - when testing.  Otherwise, do not delete the hosted table, as we need the ID to be static.
#  4. If hosted xls file already exists, update it.  If not, create it using add content.
#  5. If hosted table layer already exists, truncate and append new records.  If not, create it with new records using publish.
#  6. Delete xls, as it is no longer needed.

# Testing using xls field definitions to create a feature layer
# It seems you can't publish directly from a dataframe for a feature layer, so need to upload a xls/csv records first etc.
# Ensure xls field names have no spaces and match the feature layer field names
# Check Time Conversions!
# Add records from xls.  XLS file can have a different name than when originally loaded to AGO.


import os, sys
import pandas as pd
#import geopandas as gpd
#import numpy as np
#from io import BytesIO, StringIO
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection
#import json
from io import BytesIO, StringIO
import boto3
import botocore

import logging
import timeit
from datetime import datetime, timedelta, date 
import pytz
from pytz import timezone

pacific_timezone = pytz.timezone('America/Vancouver')


def connect_to_os(ENDPOINT, ACCESS_KEY, SECRET_KEY):
    """
    Returns a connection to Object Storage
    """ 
    try:
        s3_client = boto3.client(
            's3', 
            endpoint_url=ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=botocore.client.Config(
                retries={'max_attempts': 10, 'mode': 'standard'}
            )
        )
        
        s3_client.list_buckets()  # Check if connection is successful
        logging.info('..connected successfully to Object Storage')
        return s3_client
    
    except botocore.exceptions.ClientError as e:
        logging.error(f'..failed to connect to Object Storage: {e.response["Error"]["Message"]}')
        return None
    

def connect_to_AGO (HOST, USERNAME, PASSWORD):
    """ 
    Connects to AGOL
    """     
    gis = GIS(HOST, USERNAME, PASSWORD)

    # Test if the connection is successful
    if gis.users.me:
        logging.info('..successfully connected to AGOL as {}'.format(gis.users.me.username))
    else:
        logging.error('..connection to AGOL failed.')
    
    return gis

def delete_existing_item(gis, title, item_type):
    logging.info(f"Searching for existing {item_type} with title: {title}")
    #search_results = gis.content.search(query=f"title:{title}", item_type=item_type)
    search_results = gis.content.search(
        query=f'title:"{title}" AND owner:{AGO_USERNAME}',
        item_type=item_type,
        max_items=1
    )

    for item in search_results:
        try:
            #print(f"Deleting {item_type}: {item.title} (ID: {item.id})")
            logging.info(f"Permanently Deleting {item_type}: {item.title} (ID: {item.id})")
            item.delete(permanent=True)
        except Exception as e:
            #print(f"Error deleting {item_type}: {e}")
            logging.error(f"Error deleting {item_type}: {e}")

def get_data_dictionary(s3_client, bucket_name):
    """
    Fetches the latest Data Dictionary Excel file from S3 and returns its DataFrame.
    """
    logging.info('Fetching latest Data Dictionary from Object Storage')

    prefix= 'incoming_from_idir/data_dictionary/'
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']
    xlsx_files = [obj for obj in objects if obj['Key'].endswith('.xlsx')]
    latest_file = max(xlsx_files, key=lambda x: x['LastModified'])

    latest_file_key = latest_file['Key']
    obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
    data = obj['Body'].read()
    excel_file = pd.ExcelFile(BytesIO(data))

    df_datadict = pd.read_excel(excel_file, sheet_name='Data Dictionary')
    return df_datadict

def get_field_definitions(df_datadict_filtered):
    """
    Reads field definitions from an Excel file and returns them as a list of dictionaries.

    Optional:  Add a field Query to get specific fields.  e.g. 
    filterQuery = df_datadict['Skip_for_AGO'].isnull()
    filterQuery = df_datadict['For_Public_Dashboarding'] == 'Yes'
    filterQuery = df_datadict['For_Results_Query'] == 'Yes'

    """
    logging.info('Reading field definitions from Data Dictionary')
    #print(filterQuery)

    '''# Filter the DataFrame based on the filterQuery
    if filterQuery:
        df = df_datadict.query(filterQuery)
    else:
        df = df_datadict'''
    
    df = df_datadict_filtered

    # Mapping from custom types to ArcGIS field types
    type_mapping = {
        'TEXT': 'esriFieldTypeString',
        'DATEONLY': 'esriFieldTypeDateOnly',  # Date Only
        'DATE': 'esriFieldTypeDate',      # Date and Time
        'LONG': 'esriFieldTypeInteger',
        'SHORT': 'esriFieldTypeSmallInteger',  #does not work if coded domain values are not integers e.g. 1 vs 1.0.
        'FLOAT': 'esriFieldTypeSingle',
        'DOUBLE': 'esriFieldTypeDouble'}
    
    fields = []
    for _, row in df.iterrows():
        
        original_type = str(row["Type"]).upper()
        mapped_type = type_mapping.get(original_type, original_type)  # fallback to original if not found

        field = {
            "name": row["GIS_FIELD_NAME"],
            "type": mapped_type,
            "length": row["Length"] if pd.notnull(row["Length"]) else None,
            "alias": row["Alias"],
            #"nullable": row["Nullable"] if pd.notnull(row["Nullable"]) else None,  #Use Default
            #"required": row["Required"] if pd.notnull(row["Required"]) else None,     #Use Default
            #"domain": row["Domain Name"] if pd.notnull(row["Domain Name"]) else None,  #Cannot set the Domain yet, add later.
        }

        # Ensure length is an integer for string fields
        if pd.notnull(row["Length"]) and original_type == "TEXT":
            field["length"] = int(row["Length"])

        logging.info(f'{field}')
        fields.append(field)

    return fields

# Preprocess DataFrame to handle esriFieldTypeDateOnly fields
def preprocess_dateonly_fields(df, field_definitions):
    """
    Converts fields mapped to esriFieldTypeDateOnly to datetime.date to remove time.
    """
    dateonly_fields = [field['name'] for field in field_definitions if field['type'] == 'esriFieldTypeDateOnly']
    for field in dateonly_fields:
        if field in df.columns:
            print(f"Processing 'esriFieldTypeDateOnly' field: {field} and values: {df[field].values}  ")
            df[field] = pd.to_datetime(df[field], errors='coerce').dt.date  # Convert to date only
            #replace Not a Time (NaT) for entire dataframe
            #df = df.replace(['NaT'], '')
            #df[field] = df[field].where(df[field].notna(), None)
            df[field] = df[field].where(df[field].notna(), pd.Timestamp('1900-01-01'))
            

            # Convert to datetime and fill nulls
            #default_date = pd.Timestamp('1900-01-01')
            #df['date_field'] = pd.to_datetime(df['date_field'], errors='coerce').fillna(default_date)


            logging.info(f"Processed 'esriFieldTypeDateOnly' field: {field}")
            print(f"Fixed? 'esriFieldTypeDateOnly' field: {field} and values: {df[field].values}  ")
    return df

def preprocess_date_fields(features):
    """
    Converts all date fields in the features list to UNIX timestamps (milliseconds since epoch).

    NOT USED
    """
    for feature in features:
        for key, value in feature['attributes'].items():
            if isinstance(value, (datetime, pd.Timestamp)):
                # Convert to UNIX timestamp in milliseconds
                feature['attributes'][key] = int(value.timestamp() * 1000)
            elif isinstance(value, date):
                # Convert datetime.date to datetime.datetime and then to UNIX timestamp
                value_as_datetime = datetime.combine(value, datetime.min.time())
                feature['attributes'][key] = int(value_as_datetime.timestamp()) # * 1000)
                #feature['attributes'][key] = int(value_as_datetime.timestamp() / 1000)
            elif pd.notnull(value) and isinstance(value, int):
                # Log a warning if an Int64 value is found in a date field
                logging.warning(f"Field '{key}' has an Int64 value '{value}' that MAY need conversion.")
    return features

# Convert timestamps safely - Sample from BIER
def convert_timestamp(ts):
    return (
        datetime.datetime.utcfromtimestamp(ts / 1000)
        .replace(tzinfo=datetime.timezone.utc)
        .astimezone(tz=None)
        if ts
        else None
    )

#----------------MAIN--------------------

if __name__ == "__main__":
    start_t = timeit.default_timer() #start time

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    logging.info('Connecting to Object Storage')
    S3_ENDPOINT = os.getenv('S3_ENDPOINT')
    S3_CWD_ACCESS_KEY = os.getenv('S3_CWD_ACCESS_KEY')
    S3_CWD_SECRET_KEY = os.getenv('S3_CWD_SECRET_KEY')
    s3_client = connect_to_os(S3_ENDPOINT, S3_CWD_ACCESS_KEY, S3_CWD_SECRET_KEY)
    bucket_name= 'whcwdp'  #PRODUCTION

    # Connect to ArcGIS Online
    logging.info('\nConnecting to AGO')
    AGO_HOST = os.getenv('AGO_HOST')
    AGO_USERNAME = os.getenv('AGO_USERNAME')
    AGO_PASSWORD = os.getenv('AGO_PASSWORD')
    gis = connect_to_AGO(AGO_HOST, AGO_USERNAME, AGO_PASSWORD)

    
    # Create dataframe from Excel records
    excel_path = r"\\spatialfiles.bcgov\work\srm\kam\Workarea\ksc_proj\Wildlife\Ungulates\202404_CWD_Chronic_Wasting_Disease\work\temp_qa\cwd_sampling_results_for_public_web_20250815.xlsx"
    #excel_path = r"\\spatialfiles.bcgov\work\srm\kam\Workarea\ksc_proj\Wildlife\Ungulates\202404_CWD_Chronic_Wasting_Disease\work\temp_qa\cwd_sampling_results_for_public_web_trunc.xlsx"
    xls_df = pd.read_excel(excel_path)
    #print(xls_df.dtypes)

    #Test updated xls
    #excel_path_upd = r"\\spatialfiles.bcgov\work\srm\kam\Workarea\ksc_proj\Wildlife\Ungulates\202404_CWD_Chronic_Wasting_Disease\work\temp_qa\cwd_sampling_results_for_public_web_short_test.xlsx"
    #excel_path_upd = r"\\spatialfiles.bcgov\work\srm\kam\Workarea\ksc_proj\Wildlife\Ungulates\202404_CWD_Chronic_Wasting_Disease\work\temp_qa\cwd_sampling_results_for_public_web_trunc2.xlsx"
    excel_path_upd = excel_path
    upd_xls_df = pd.read_excel(excel_path_upd)
    #print(upd_xls_df.dtypes)

    # Configuration
    item_title = "junk_table"      #"CWD_FL_Test" (all), "CWD_Public_Table_Test" (public), "CWD_Public_Sampling_Results" (test_results)

    #  Specify the type of item to create: "table" or "feature_layer"
    create_type = "table"  # Values: feature_layer, table

    #build filter dictionary depending  on the item field list requirement
    fieldFilter_dict = {
        "CWD_FL_Test": "all",
        "CWD_Public_Table_Test": "public",
        "CWD_Public_Sampling_Results": "test_results",
        "junk_table": "test_results",
        #"CWD_Public_Test_Results_Trunc_Test": "test_results",
        #"table_1_junk": "test_results"
    }

    #  Data Dictionary Field Filter: all (skip particular fields as indicated in the DataDictionary), public, or test_results
    dd_filter_type = fieldFilter_dict[item_title]

    folder_name = "2024_CWD_TEST"
    hosted_service_name = f"{item_title}"  #f"{item_title}_service"
    hosted_table_name = f"{item_title}"  #f"{item_title}_table"

    

    # Print AGO folder contents (optional)
    file_list = 'n'
    if file_list == 'y':
        folder_items = gis.users.me.items(folder=folder_name)
        folder_items.sort(key=lambda x: x.title)
        logging.info(f'\nItems found in {folder_name}: {len(folder_items)}')
        for item in folder_items:
                logging.info(f"Title: {item.title}, \tID: {item.id}, \tType: {item.type}")

    # File Cleanup  - delete Hosted table first, then xls
    # Is there a time lag for deletion?? If I delete it once, it does not seem to be deleted if I run the script again immediately.
    file_cleanup = 'y'
    if file_cleanup == 'y':

        logging.info(f"\nDeleting existing items with title: {item_title}")
        #delete_existing_item(gis, 'cwd_sampling_results_for_public_web_trunc.xlsx', item_type="Microsoft Excel")
        delete_existing_item(gis, item_title, item_type="Microsoft Excel")
        #delete_existing_item(gis, hosted_service_name, item_type="Feature Service")
        #delete_existing_item(gis, hosted_table_name, item_type="Feature Service")
        #delete_existing_item(gis, hosted_table_name, item_type="Feature Layer")


        '''# Search for the item by title
        search_results = gis.content.search("cwd_sampling_results_for_public_web_trunc.xlsx", max_items=10)
        print(f'\nItems found with title "cwd_sampling_results_for_public_web_trunc.xlsx": {len(search_results)}')
        # Print matching items
        for item in search_results:
            print(f"Title: {item.title}, ID: {item.id}, Owner: {item.owner}, Type: {item.type}")
            delete_existing_item(gis, 'cwd_sampling_results_for_public_web_trunc.xlsx', item_type="Microsoft Excel")'''


    #print('DONE')
    #sys.exit()

    if create_type == "table":
        # Search for the XLS table item by title or other keywords
        search_results = gis.content.search(item_title, item_type="Microsoft Excel")
        logging.info(f'\nItems found with title "{item_title}": {len(search_results)}')
        for item in search_results:
            logging.info("SEARCH RESULTS:")
            logging.info(f"\nTitle: {item.title}, \tID: {item.id}, \tType: {item.type}")
        
        # Load xls item if it does not exist
        if not search_results:
            logging.warning(f"\nNo Excel item found with title: {item_title}")
            logging.info("Creating new Excel item")

            table_item = gis.content.add({
            "type": "Microsoft Excel",
            "title": item_title,
            "tags": "data, table",
            "description": "A sample data table published from Excel SL Delete"
        }, data=excel_path, folder=folder_name)

            #published_table = table_item.publish()  # Publish the new table as a hosted table??  Forces creation of OID?

        # Or Replace/Update XLS item
        if search_results:
            for item in search_results:
                logging.info(f"\nFound existing Excel item: {item.title}")
                logging.info(f"Replacing existing Excel item with new data from excel file")
                print(f"Title: {item.title}, ID: {item.id}, Type: {item.type}")
                xls_item_id = item.id
                xls_item = gis.content.get(xls_item_id)
                xls_item.update({}, data=excel_path_upd)
                logging.info(f"Excel item updated successfully: {xls_item.title}")
            


    #----------------------------------------
    # Get field definitions from the data dictionary
    df_datadict = get_data_dictionary(s3_client, bucket_name)
    #print(df_datadict.dtypes)

    # Filter the DataFrame based on the filter type and fields noted in the Data Dictionary
    if dd_filter_type == 'all':
        df_datadict_filtered = df_datadict[df_datadict['Skip_for_AGO'].isnull()]
    elif dd_filter_type == 'public':
        df_datadict_filtered = df_datadict[df_datadict['For_Public_Dashboarding'] == 'Yes']
    elif dd_filter_type == 'test_results':
        df_datadict_filtered = df_datadict[df_datadict['For_Results_Query'] == 'Yes']

    print("\n")
    field_definitions = get_field_definitions(df_datadict_filtered)

    # Add Object ID field definition if not already present.  Not sure if this is needed?
    ''' OIDdef = {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID"}
    if OIDdef not in field_definitions:
        field_definitions.insert(0, OIDdef)'''

    print(f"\nField Definitions for {item_title}:")
    for field in field_definitions:
        print(f" - {field['name']} ({field['type']})")

    #print("DONE")
    #sys.exit()

    #----------------------------------------
    #Create Hosted Feature Layer or Table Template from Data Dictionary and Get new Item ID

    
    published_flag = None

    #Check if hosted table already exists
    print(f"\nChecking if hosted {create_type} exists: {hosted_service_name}")
    search_results = gis.content.search(hosted_service_name, item_type="Feature Layer")
    print(f'Items found with title "{hosted_service_name}": {len(search_results)}')
    for item in search_results:
            print(f"PUBLISHED Title: {item.title}, ID: {item.id}, Type: {item.type}")

    

    '''
    print("DONE")
    sys.exit()'''

    if search_results:
        published_flag = 'True'

    #Test
    #fs_item_id = 'f5beb7e7add14d219d91115121937c02'
    #published_flag = 'True'


    # If a hosted FL or table does not exist, then create it  (Blank Template)
    # CREATING A HOSTED TABLE IS NOT WORKING DUE TO OBJECTID BUG??  hAVE TO CREATE HOSTED TABLE MANUALLY TO GET OID WORKING PROPERLY.
    if not published_flag:   
        # Define the schema programmatically
        if create_type == "feature_layer":
            layer_schema = {
                "layers": [{
                    "name": hosted_service_name,
                    "type": "Feature Layer",
                    "geometryType": "esriGeometryPoint",  # Change as needed
                    "fields": field_definitions,
                    "spatialReference": {"wkid": 4326},  # WGS84 spatial reference
                    "extent": {
                        "xmin": -140,
                        "ymin": 45,
                        "xmax": 100,
                        "ymax": 65
                    }
                }]
            }

        
        elif create_type == "table":
            '''layer_schema = {
                "layers": [],
                "tables": [
                    {
                        "name": hosted_table_name,
                        "locationType": "none",  #no spatial data
                        "type": "Table" #,
                        #"fields": field_definitions
                    }
                ]
            }'''
             
            layer_schema = {
                "layers": [],
                "tables": [
                    {
                        "name": hosted_service_name,  #hosted_table_name
                        "type": "Table",
                        #"locationType": "none",
                        "fields": [
                            {
                                "name": "OBJECTID",
                                "type": "esriFieldTypeOID",
                                "alias": "OBJECTID",   #THIS DOES NOT SEEM TO WORK PROPERLY! 
                                "nullable": False
                            }
                        ] + field_definitions  # Append your custom fields here
                    }
                ]
            }


             

        # Create the hosted item
        print(f"\nCreating new {create_type}: {hosted_service_name}")
        try:
            # Create a new feature service
            service_item = gis.content.create_service(
                name=hosted_service_name,
                service_type="featureService",
                folder=folder_name
            )

            # Access the FeatureLayerCollection for the service
            feature_layer_collection = FeatureLayerCollection.fromitem(service_item)

            # Add the schema to the feature service
            feature_layer_collection.manager.add_to_definition(layer_schema)
            logging.info(f"Hosted {create_type} '{hosted_service_name}' created successfully.")
            logging.info(f"Item ID: {service_item.id}")
        except Exception as e:
            logging.error(f"Error creating hosted layer: {e}")




    #-- If it exists, truncate and Load records
    # If hosted table exists, truncate (delete) records and overwrite with new records
    '''search_results = gis.content.search(hosted_table_name, item_type="Feature Layer")
    if search_results:
        published_flag = 'True'''

    if published_flag == 'True':

        search_results = gis.content.search(hosted_service_name, item_type="Feature Service")
        for item in search_results:
            print(f"\nEXISTING SERVICE:  Title: {item.title}, ID: {item.id}, Type: {item.type}")
            fs_item_id = item.id

        
        for table in item.tables:
            print(f"Table name: {table.properties.name}, URL: {table.url}")

        
        '''# Protect the item from deletion
        item.protect(enable=True)
        print(f"Item '{item.title}' is now protected from accidental deletion.")'''



        # Access the FeatureLayerCollection or table for the service
        # Add dd fields if not already
        #feature_layer = item.layers[0]
        #field_names = [field['name'] for field in feature_layer.properties.fields]
        feature_table = item.tables[0]
        field_names = [field['name'] for field in feature_table.properties.fields]

        #print(field_definitions)
        logging.info(f"\nExisting fields in hosted {create_type}: {field_names}")

        if 'CWD_TEST_STATUS' not in field_names:
            # Add the data dictionary field schema to the feature service
            #feature_layer_collection = FeatureLayerCollection.fromitem(item)  #NO
            #feature_layer_collection.manager.add_to_definition({"fields": field_definitions})  #NO

            feature_table.manager.add_to_definition({"fields": field_definitions})
            logging.info(f"Hosted {create_type} '{hosted_service_name}':  fields added successfully.")
            
            # Check updated fields
            updated_fields = [field['name'] for field in feature_table.properties.fields]
            print(updated_fields)

            print("\n-----------------")
        else:
            logging.info(f"Fields already exist!")

        

        #search_results = gis.content.search(item_title, item_type="Feature Layer")
        #for item in search_results:
         #   print(f"Title: {item.title}, ID: {item.id}, Type: {item.type}")
          #  fl_item_id = item.id

        print(f"\nTruncating and Loading new records to existing hosted table: {item.title}")

        #Truncate Table
        hosted_table_item = gis.content.get(fs_item_id)
        table_layer = hosted_table_item.tables[0]
        hosted_table_manager = table_layer.manager
        try:
            hosted_table_manager.truncate()
            logging.info(f"Data truncated")
        except:
            logging.warning("Truncate failed")

   
        #print("DONE")
        #sys.exit()

        # -----------------------Load Table records (non-spatial)
        print("\nDataFrame dtypes before loading:")
        print(upd_xls_df.dtypes)

        # Preprocess the DataFrame for esriFieldTypeDateOnly fields
        # NOTE:  MAY NEED TO FILL any null or Not Recorded dates as blanks or an obscure date, then fill with None later.
        # It seems AGO accepts None for empty date fields, but the dataframe type can be Object, vs a date type.
        upd_xls_df = preprocess_dateonly_fields(upd_xls_df, field_definitions)

        #TEMP
        #drop_cols = ['SUBMITTER_FIRST_NAME', 'SUBMITTER_LAST_NAME', 'SUBMITTER_PHONE', 'FWID','STATUS_ID']
        #df = df.drop(columns=[col for col in drop_cols if col in df.columns])

        '''for col in upd_xls_df.columns:
            if '_DATE' in col:
                upd_xls_df = upd_xls_df.drop(columns=col, axis=1)'''

        #drop_cols = ['CWD_EAR_CARD_ID','MORTALITY_DATE', 'SAMPLED_DATE','GIS_LOAD_VERSION_DATE']
        #drop_cols = ['SAMPLED_DATE']
        #upd_xls_df = upd_xls_df.drop(columns=[col for col in drop_cols if col in upd_xls_df.columns])

        
        
        #upd_xls_df['CWD_EAR_CARD_ID'] = upd_xls_df['CWD_EAR_CARD_ID'].astype(int)
        #Convert objects to string
        '''for col in upd_xls_df.columns:
            #if upd_xls_df[col].dtype == 'object':
            #if '_DATE' not in col:
            #print(f"Converting column '{col}' to string")
            upd_xls_df[col] = upd_xls_df[col].fillna('').astype(str)
            #upd_xls_df[col] = upd_xls_df[col].astype(str)'''
        
        
        for col in upd_xls_df.columns:
            #if '_DATE' not in col:
            upd_xls_df[col] = upd_xls_df[col].fillna('').astype(str)
       


         # Convert DATE fields to datetime, ensure they are timezone-aware
         #TEST
        for col in upd_xls_df.columns:
            if 'GIS_LOAD_VERSION_DATE' in col:

                print(f"Converting column '{col}' to datetime with timezone")
                upd_xls_df[col] = pd.to_datetime(upd_xls_df[col], errors='coerce').dt.tz_localize(pacific_timezone, 
                                                                                ambiguous='NaT', 
                                                                                nonexistent='shift_forward')
                upd_xls_df[col] = pd.to_datetime(upd_xls_df[col]).dt.tz_convert('UTC')
        

        '''if pd.api.types.is_datetime64_any_dtype(upd_xls_df[col]):
            print(f"Column '{col}' is datetime64 dtype")
            #upd_xls_df[col] = upd_xls_df[col].apply(lambda x: x.isoformat() if not pd.isna(x) else '')
            upd_xls_df[col] = pd.to_datetime(upd_xls_df[col], errors='coerce').dt.tz_localize(pacific_timezone, 
                                                                            ambiguous='NaT', 
                                                                            nonexistent='shift_forward')'''

        #for col in upd_xls_df.columns:
            #if '_DATE' not in col:
        upd_xls_df[col] = upd_xls_df[col].fillna('')
        
        
        # Replace default date with None
        print(upd_xls_df.head(20)['MORTALITY_DATE'])
        # if field is a date type
        #upd_xls_df['MORTALITY_DATE'] = upd_xls_df['MORTALITY_DATE'].apply(lambda x: None if x == pd.Timestamp('1900-01-01 00:00:00') else x)
        # if field is an object type
        upd_xls_df['MORTALITY_DATE'] = upd_xls_df['MORTALITY_DATE'].apply(lambda x: None if str(x) == '1900-01-01 00:00:00' else x)

        #Replace Not Recorded with None in Integer fields
        # upd_xls_df['CWD_EAR_CARD_ID'] = upd_xls_df['CWD_EAR_CARD_ID'].apply(lambda x: None if str(x).strip() == 'Not Recorded' else x)


        # print("\nDataFrame dtypes after manipulation loading:")
        # print(upd_xls_df.dtypes)
        # print(upd_xls_df[['CWD_EAR_CARD_ID','MORTALITY_DATE']].head(20))
        
        #sys.exit()
        if create_type == "table":
            # Load new Table from dataframe
            logging.info(f"\nLoading new records to hosted table from dataframe...")
            features = []
            
            

            # Get valid field names and types from the table schema
            field_info = {field['name']: field['type'] for field in table_layer.properties.fields}
            print(f"\nFeature Layer Field Info:")
            for field in field_info:
                print(f"Field: {field}, Type: {field_info[field]}")

            logging.info(f"\nField names in hosted table: {[field['name'] for field in table_layer.properties.fields]}")

            for _, row in upd_xls_df.iterrows():
                attributes = {} 
                for k, v in row.to_dict().items():
                    if k in field_info:
                        field_type = field_info[k]
                        # Validate and convert value based on field type
                        if field_type == "esriFieldTypeInteger" or field_type == "esriFieldTypeSmallInteger":
                            try:
                                v = int(v) if pd.notnull(v) else None
                            except Exception:
                                logging.warning(f"Field '{k}' expects integer, got '{v}'")
                                v = None
                        '''
                        elif field_type == "esriFieldTypeDouble" or field_type == "esriFieldTypeSingle":
                            try:
                                v = float(v) if pd.notnull(v) else None
                            except Exception:
                                logging.warning(f"Field '{k}' expects float, got '{v}'")
                                v = None
                        elif field_type == "esriFieldTypeDate":
                            if pd.notnull(v):
                                if not isinstance(v, (datetime, pd.Timestamp)):
                                    try:
                                        v = pd.to_datetime(v)
                                    except Exception:
                                        logging.warning(f"Field '{k}' expects date, got '{v}'")
                                        v = None
                        elif field_type == "esriFieldTypeDateOnly":
                            # Ensure the value is a date (already handled by preprocess_dateonly_fields)
                            #v = v if isinstance(v, datetime.date) else None
                            v = v if isinstance(v, date) else None
                            #v = v if isinstance(v, (date, pd.Timestamp)) else None
                        elif field_type == "esriFieldTypeString":
                            v = str(v) if pd.notnull(v) else None
                        # Add other type checks as needed
                        '''
                        attributes[k] = v
                features.append({"attributes": attributes})
            #logging.info(f"\nFeatures to add: {features}")

        """for feature in features:
            for key, value in feature['attributes'].items():
                logging.info(f"Field: {key}, Value: {value}, Type: {type(value)}")
                if value is None or value == '':
                    logging.warning(f"Field '{key}' is missing or empty.")  """  

        '''features = [{'attributes': {'CWD_EAR_CARD_ID': 11171, 'DROPOFF_LOCATION': 'COS Cranbrook', 'SPECIES': 'White Tailed Deer', 
                                          'SEX': 'Male', 'MORTALITY_DATE': datetime.date(2024, 11, 1), 'SAMPLED_DATE': datetime.date(2024, 12, 6), 
                                          'CWD_NOT_SAMPLED_REASON': 'Incorrect species', 'CWD_TEST_STATUS': 'Negative', 'UPDATED_WMU': '4-22', 
                                          'GIS_LOAD_VERSION_DATE': Timestamp('2025-08-15 04:10:46')}}, 
                                          {'attributes': {'CWD_EAR_CARD_ID': 11172, 'DROPOFF_LOCATION': 'Not Recorded', 'SPECIES': 'White Tailed Deer', 
                                            'SEX': 'Male', 'MORTALITY_DATE': datetime.date(2024, 11, 1), 'SAMPLED_DATE': datetime.date(2024, 12, 7), 
                                            'CWD_NOT_SAMPLED_REASON': None, 'CWD_TEST_STATUS': 'Negative', 'UPDATED_WMU': '4-7', 
                                            'GIS_LOAD_VERSION_DATE': Timestamp('2025-08-15 04:10:46')}}]'''
                          

        

        #Or Load Feature Records (spatial) - NOT YET TESTED
        if create_type == "feature_layer":
            logging.info(f"\nLoading new records to hosted feature layer from dataframe...")
            features = []
            for _, row in upd_xls_df.iterrows():
                attributes = row.to_dict()
                geometry = {"x": row["Longitude"], "y": row["Latitude"]}  # Replace with your geometry fields
                features.append({"attributes": attributes, "geometry": geometry})

        
        #features = preprocess_date_fields(features)

        # Add features to the table
        logging.info(f"\nAdding {len(features)} records to the hosted table...")
        #logging.info(f"\nFeatures to add (after preprocessing): {features}")
        '''for feature in features:
            for key, value in feature['attributes'].items():
                if isinstance(value, int):
                    logging.warning(f"Field '{key}' has an Int64 value: {value}")'''


        #features = [{'attributes': {'DROPOFF_LOCATION': 'COS Cranbrook', 'SPECIES': 'White Tailed Deer', 'SEX': 'Male', 'CWD_NOT_SAMPLED_REASON': 'Incorrect species', 'CWD_TEST_STATUS': 'Negative', 'UPDATED_WMU': '4-22'}}, {'attributes': {'DROPOFF_LOCATION': 'Not Recorded', 'SPECIES': 'White Tailed Deer', 'SEX': 'Male', 'CWD_NOT_SAMPLED_REASON': 'test', 'CWD_TEST_STATUS': 'Negative', 'UPDATED_WMU': '4-7'}}, {'attributes': {'DROPOFF_LOCATION': 'BHA Freezer', 'SPECIES': 'White Tailed Deer', 'SEX': 'Male', 'CWD_NOT_SAMPLED_REASON': 'test', 'CWD_TEST_STATUS': 'Negative', 'UPDATED_WMU': '4-4'}}, {'attributes': {'DROPOFF_LOCATION': "Rick's Fine Meat and Sausage", 'SPECIES': 'White Tailed Deer', 'SEX': 'Male', 'CWD_NOT_SAMPLED_REASON': 'test', 'CWD_TEST_STATUS': 'Negative', 'UPDATED_WMU': '4-3'}}, {'attributes': {'DROPOFF_LOCATION': 'BHA Freezer', 'SPECIES': 'White Tailed Deer', 'SEX': 'Male', 'CWD_NOT_SAMPLED_REASON': 'test', 'CWD_TEST_STATUS': 'Negative', 'UPDATED_WMU': '4-4'}}
        #]

        result = table_layer.edit_features(adds=features)

        #result = table_layer.append(features) #?? Append does not work
        print("\n-----------------")
        print(result)

        if result['addResults'][0]['success']:
            logging.info("Records added successfully.")
        else:
            # Extract the error message or reason for failure
            error_message = result['addResults'][0].get('error', {}).get('message', 'Unknown error')
            logging.warning(f"Failed to add records. Reason: {error_message}")

        print("\n-----------------")
        # error handling and logging of editing result
        try:
            # check if all the features were added successfully
            if all(res.get('success') for res in result.get('addResults', [])):
                # log a success message with the number of features added
                logging.info(f"..{len(features)} features added successfully.")
            else:
                # log an error if one or more features failed to add
                logging.error("..some features failed to add.")
                # log the full result object for debugging purposes
                logging.error(f"..full result: {result}")
        except Exception as e:
            # catch any unexpected errors during the result handling process and log the full exception traceback for easier debugging
            logging.exception(f"..unexpected error: {e}")

   
    



    # Final Delete of xls, as it is no longer needed
    #delete_existing_item(gis, item_title, item_type="Microsoft Excel")

    print("DONE")

    #-------------------------------------------------

   