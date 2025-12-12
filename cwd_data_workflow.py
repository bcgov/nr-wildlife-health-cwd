#-------------------------------------------------------------------------------
# Name:        Chronic Wasting Disease (CWD) Data Workflow
#
# Purpose:     This script streamlines the Chronic Wasting Disease (CWD) data processing pipeline by:
#                 (1) Retrieve Incoming Data: Fetches incoming lab data and hunter survey data from Object Storage and ArcGIS Online (AGO).
#                 (2) Merge and Process Data: Merges and processes data from various sources to create a master dataset.
#                 (3) Generate Master Dataset: Creates a comprehensive master dataset that includes lab and hunter survey data.
#                 (4) Publish to AGO: Publishes the master dataset to ArcGIS Online and updates Hunter Survey in AGO with QA flags.
#                 (5) Generate Files for Web and Lab Submissions: Creates files for the CWD webpage and lab submissions.
#                 (6) Summarize Sampling Statistics: Rolls up summary sampling statistics by Management Unit (MU) and Region.
#                 (7) Perform QA: Conducts quality assurance checks on the hunter survey data against the master sampling dataset
#                     and compares MU and Environment Region from the Ear Card vs spatial location.

#              
# Input(s):    (1) Object Storage credentials.
#              (1) AGO credentials.           
#
# Author:      Moez Labiadh - GeoBC
#              Emma Armitage - GeoBC
#              Sasha Lees - GeoBC
#
# Created:     2024-08-15
# Updates ongoing - see GitHub for details.  https://github.com/bcgov/nr-wildlife-health-cwd 
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os, sys
import re
import requests
import boto3
import botocore
import json
import pandas as pd
import geopandas as gpd
import numpy as np
from io import BytesIO, StringIO
from arcgis.gis import GIS
from arcgis.features import FeatureLayer

import logging
import timeit
import pytz
from pytz import timezone
from datetime import datetime, timedelta



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

def s3_file_exists_check(s3_client, s3_bucket_name, key):
    try:
        s3_client.head_object(Bucket=s3_bucket_name, Key=key)
        logging.info(f"File {key} EXISTS in bucket {s3_bucket_name}")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.info(f"File {key} DOES NOT EXIST in bucket {s3_bucket_name}")
            return False
        else:
            raise

def chefs_api_request(base_url, endpoint, form_id, api_key, params=None):
    """
    Makes a GET request to the CHEFS API.

    Returns: the JSON response if the request is successful, otherwise logs an error and exits.
    """

    url = f"{base_url}/{form_id}/{endpoint}"

    r = requests.get(url, auth=(form_id, api_key), params=params)

    if r.status_code == 200:
        logging.info(f"Request to {endpoint} successful")
        return r.json()
    else:
        logging.error(f"Failed to fetch {url}: {r.status_code} - {r.text}")


def convert_datetime(df, columns, strip_time=False, as_string=False, date_format="%Y-%m-%d"):
    """
    Convert columns with mixed ISO 8601 formats (Z and offsets) to Pacific Time.
    Handles Z and offset-based timestamps separately.

    Handles:
    - Mixed time zones in input (e.g., Z, -06:00, -07:00)
    - tz-aware and tz-naive values
    - None or invalid values gracefully
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        columns (str or list): Column name or list of column names to convert.
        strip_time (bool): If True, keep only the date (YYYY-MM-DD).
        as_string (bool): If True, return formatted strings instead of datetime/date objects.
        date_format (str): Format for string output when strip_time=True (default: '%Y-%m-%d').
    
    Returns:
        pd.DataFrame: Updated DataFrame with converted columns.

    """
    if isinstance(columns, str):
        columns = [columns]

    # pacific_timezone = pytz.timezone('America/Vancouver')

    for col in columns:
        converted = pd.Series(index=df.index, dtype="object")

        # Detect Zulu time
        mask_z = df[col].astype(str).str.endswith("Z", na=False)
        if mask_z.any():
            converted[mask_z] = pd.to_datetime(df.loc[mask_z, col], errors="coerce", utc=True)

        # Detect offset time
        mask_offset = ~mask_z & df[col].notna()
        if mask_offset.any():
            converted[mask_offset] = pd.to_datetime(df.loc[mask_offset, col], errors="coerce", utc=True)

        # Convert to datetime 
        # Don't convert to Pacific time as CHEFS assumes date/times are in UTC when the user completes the survey.
        converted = pd.to_datetime(converted, errors="coerce", utc=True)
        # converted = converted.dt.tz_convert(pacific_timezone).dt.tz_localize(None)

        # Format output
        if strip_time:
            if as_string:
                converted = converted.apply(lambda x: x.strftime(date_format) if pd.notnull(x) else None)
            else:
                converted = converted.dt.date
        elif as_string:
            # Include time in string format
            converted = converted.apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None)

        df[col] = converted

    return df

def append_xls_files_from_os(s3_client, bucket_name, folder, file_text, header_index_num=None,required_headers=None, sheet_name=0):
    """
    Returns an appended df of specific xls files from Object Storage.
    Will check that fields match between files.
    You must provide either the header_index_num or the required_headers.  
    This is to account for potentially different header row positions in the xls files.
    The incoming xls files must have required columns (e.g., WLH_ID, CWD_EAR_CARD_ID)  or a stable header_index_num.
    The incoming xls files must have a WLH_ID column.

    Will look for files in the specified folder, or recursively in all subfolders.

    Inputs:
    file_text:  text to search for in the xls file name  (stored as lower case in S3)
    header_index_num:  row number of the header in the excel file (0 based).  If header_index_num is None, the function will auto-detect the header row by searching for all required_headers.
    required_headers:  list of required headers to check for in the excel file.  If provided, the function will check if all required headers are present in the file.
    sheet_name:  name of the sheet to read from the excel file.  Default of 0, will read the first sheet.
    """
    if required_headers is None and header_index_num is None:   # Default, if neither are specified
        required_headers = ['CWD_LAB_SUBMISSION_ID','WLH_ID','CWD_EAR_CARD_ID']

    df_list = []

    logging.info(f"..processing files in bucket: {bucket_name}/{folder}")

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=folder):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.xlsx') and file_text in key.lower():
            #if key.endswith('.xlsx') and 'cwd_sample_collection_01april2023_to_11aug2025_master_dataset_20250815' in key.lower():  #For testing
                try:
                    logging.info(f"...reading file: {key}")
                    obj_data = s3_client.get_object(Bucket=bucket_name, Key=key)
                    file_bytes = BytesIO(obj_data['Body'].read())
                    
                    # Auto-detect header row if not provided
                    if header_index_num is None:
                        preview = pd.read_excel(file_bytes, sheet_name=sheet_name, header=None, nrows=10)
                        file_bytes.seek(0)
                        header_index_num = None
                        for i, row in preview.iterrows():
                            # Check if all required headers are present in this row
                            if all(h in row.values for h in required_headers):
                                header_index_num = i
                                break
                        if header_index_num is None:
                            raise ValueError(f"Could not find header row containing {required_headers} in file: {key}")

                    file_bytes.seek(0)
                    df = pd.read_excel(file_bytes, header=header_index_num, sheet_name=sheet_name)
                    
                    # Either WLH_ID or CWD_EAR_CARD_ID must be populated
                    df = df[df['WLH_ID'].notna() | df['CWD_EAR_CARD_ID'].notna()] #remove empty rows

                    # Check that sampling sheet column names are within the column names in the Data Dictionary (fprop_dict)
                    if file_text == 'cwd_sample_collection':
                        if not df.columns.isin(fprop_dict.keys()).all():
                            # Log the mismatched columns.
                            mismatched_cols = df.columns[~df.columns.isin(fprop_dict.keys())]
                            logging.error(f"Mismatched columns in file {key}: {mismatched_cols.tolist()}\n")
                            # raise ValueError(f"Dataframe columns do not match field properties in file: {key}")
                    

                    # Check that subsequent file columns match the columns in the first file and if any columns are missing.
                    if df_list:
                        reference_cols = df_list[0].columns  #get columns from first df
                        current_cols = df.columns
                        
                        missing_cols = [col for col in reference_cols if col not in current_cols]
                        extra_cols   = [col for col in current_cols   if col not in reference_cols]

                        # Order check (optional): True if order differs but names are the same
                        same_names_diff_order = (
                            set(current_cols) == set(reference_cols)
                            and not current_cols.equals(reference_cols)
                        )

                        # Log detailed schema issues
                        if missing_cols or extra_cols or same_names_diff_order:
                            parts = []
                            if missing_cols:
                                parts.append(f"Missing columns: {missing_cols}")
                            if extra_cols:
                                parts.append(f"Extra columns: {extra_cols}")
                            if same_names_diff_order:
                                parts.append("Column order differs from the first file")

                            logging.error(
                                "Schema mismatch relative to the first file. "
                                f"Mismatched file: {key}\n" + "\n".join(parts)
                            )
                        
                    df_list.append(df)

                except botocore.exceptions.BotoCoreError as e:
                    logging.error(f"...failed to retrieve file: {e}")
    if df_list:
        listlen = len(df_list)
        logging.info(f"..appending dataframes for {listlen} incoming files")
        return pd.concat(df_list, ignore_index=True)
    else:
        logging.info("..no dataframes to append")
        return pd.DataFrame()


def get_email_data_from_os(s3_client, bucket_name,filepathname):
    """
    Returns a DataFrame for the Email Submission file with verified lat/longs, from Object Storage.
    Reads Default header (row 0) and default sheet (first sheet)
    """
    target_key = filepathname
    # List objects in the bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    # Check if the target file exists
    for obj in response.get('Contents', []):
        if obj['Key'] == target_key:
            logging.info("..reading email submissions .xlsx")

            # Get the object and read into pandas
            resp = s3_client.get_object(Bucket=bucket_name, Key=target_key)
            data_bytes = resp['Body'].read()

            # Use defaults: header=0, sheet_name=0
            df = pd.read_excel(BytesIO(data_bytes), engine='openpyxl')

            return df

    # If file not found
    raise FileNotFoundError(f"Object not found: s3://{bucket_name}/{target_key}")


def get_lookup_tables_from_os(s3_client, bucket_name):
    """
    Returns dataframes of lookup tables for Region and MU centroid locations
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    
    for obj in response.get('Contents', []):
        file_name = obj['Key']
        folder= 'lookup_tables/'
        
        if file_name == folder + 'region_lookup.csv':
            logging.info("..reading regions table")
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            df_rg = pd.read_csv(BytesIO(obj['Body'].read()))
        
        elif file_name == folder + 'mu_lookup.csv':
            logging.info("..reading mu table")
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            df_mu = pd.read_csv(BytesIO(obj['Body'].read()))
            
    return df_rg, df_mu


def get_hunter_data_from_ago(AGO_HUNTER_ITEM_ID):
    """
    Returns a df containing legacy hunter survey123 data from AGO and manipulates time columns to pacific time vs UTC.
    
    The Survey123 form is 'retired' so no new data should be coming in, but we keep this function to grab the old data.
    """

    # get the ago item
    #hunter_survey_item = gis.content.search(query=AGO_HUNTER_ITEM, item_type="Feature Layer")
    hunter_survey_item = gis.content.get(AGO_HUNTER_ITEM_ID)
    #print(hunter_survey_item)

    # get the ago feature layer
    hunter_flayer = hunter_survey_item.layers[0]

    # query the feature layer to get its data
    hunter_data = hunter_flayer.query().sdf

    # convert the feature layer data to pandas dataframe
    hunter_df = pd.DataFrame(hunter_data)

    
    #backup initial hunter data from AGO, before updating with new QA flags.
    # NOTE THAT Date/times are in UTC!  Cannot export to excel if 'time aware'. 
    # Also, AGO stores date/times in UTC behind the scenes, but would appear as Pacific time in the table.
    # So keep as UTC in case the data is re-imported to AGO.
    
    #Disable for now, as Survey123 form is 'retired'
    #save_xlsx_to_os(s3_client, s3_bucket_name, hunter_df, f'hunter_survey/ago_backups/cwd_hunter_survey_data_from_ago_backup_{current_datetime_str}.xlsx')

    #Convert UTC Date Times from AGO to Pacific Time
    for col in ['CreationDate','EditDate','HUNTER_MORTALITY_DATE','HUNTER_SUBMIT_DATE_TIME']:  
        # Convert to datetime and localize to UTC
        hunter_df[col] = pd.to_datetime(hunter_df[col], errors='coerce').dt.tz_localize('UTC')
        # Convert to Pacific time
        hunter_df[col] = hunter_df[col].dt.tz_convert(pacific_timezone)
        #Then remove time aware, as this is required below.
        hunter_df[col] = hunter_df[col].dt.tz_localize(None)

    # Add field for SURVEY_SOURCE and populate as 'Survey123'
    hunter_df['SURVEY_SOURCE'] = 'Survey123'
            
    logging.info(f"Number of Hunter Survey Records from AGO:  {len(hunter_df)}")

    return hunter_df

def get_hunter_data_from_chefs(BASE_URL, FORM_ID, API_KEY, CHEFS_HIDDEN_FIELDS):
    # TODO:  Investigate if the CHEFS Reviewer Notes field can be pulled in from the API.
    logging.info("\nFetching the published version ID of the CHEFS form")
    
    logging.info(f"FORM_ID: {FORM_ID if FORM_ID else 'Missing'}")
    logging.info(f"API_KEY: {'***' if API_KEY else 'Missing'}")


    version_data = chefs_api_request(base_url=BASE_URL, 
                                     endpoint="version", 
                                     form_id=FORM_ID, 
                                     api_key=API_KEY)

    #print("version_data:", version_data)
    if version_data and 'versions' in version_data and version_data['versions']:
        version_id = version_data['versions'][0]['id']
        logging.info(f"..published version ID: {version_id}")
    else:
        logging.error("No versions found in CHEFS response.")
        return pd.DataFrame()  # Return empty DataFrame if no versions found


    '''# extract version ID from the response
    version_id = version_data['versions'][0]['id']
    logging.info(f"..published version ID: {version_id}")'''

    logging.info("..fetching the published version ID of the CHEFS form")
    field_name_data = chefs_api_request(base_url=BASE_URL,
                                        endpoint=f"versions/{version_id}/fields",
                                        form_id=FORM_ID,
                                        api_key=API_KEY)
    
    # add hidden fields to the list of field names as they are not returned by the API
    for hidden_field in CHEFS_HIDDEN_FIELDS:
        field_name_data.append(hidden_field)
    # format the field names into a comma-separated string
    chefs_fields = ",".join(field_name_data)

    logging.info("..fetching CHEFS submissions")
    chefs_data = chefs_api_request(base_url=BASE_URL,
                                   endpoint="submissions",
                                   form_id=FORM_ID,
                                   api_key=API_KEY,
                                   params={"fields": chefs_fields})
    
    # convert json reponse to dataframe
    chefs_df = pd.json_normalize(chefs_data)
    # ignore deleted submissions for the dataframe
    chefs_df = chefs_df[chefs_df['deleted'] == False]

    # Force CWD_EAR_CARD_ID to integer
    chefs_df['CWD_EAR_CARD_ID'] = pd.to_numeric(chefs_df['CWD_EAR_CARD_ID'], errors='coerce').astype('Int64')

    logging.info("..saving raw CHEFS data to object storage")
    # TOGGLE as needed
    save_xlsx_to_os(s3_client, s3_bucket_name, chefs_df, f'chefs_survey/cwd_hunter_survey_data_from_chefs_backup_{current_datetime_str}.xlsx')


    # print(chefs_df.dtypes)
    #print(chefs_df[['createdAt','MORTALITY_DATE','SAMPLED_DATE']])
    # print(chefs_df.head(20))
    # print(chefs_df.tail(20))

    # Deal with Date/Time columns in CHEFS data and set as pacific time.  Mixed formats in the source data from CHEFS.
    # Convert UTC Date Times  to Pacific Time.  Assumes inputs are Object types.
    # Store as strings in order to export to excel

    # Keep full datetime for createdAt
    convert_datetime(chefs_df, ['createdAt'], strip_time=False, as_string=True)
    # Convert to date only for MORTALITY_DATE and SAMPLED_DATE
    convert_datetime(chefs_df, ['MORTALITY_DATE', 'SAMPLED_DATE'], strip_time=True, as_string=True)

    
    #print(chefs_df.dtypes)
    #print(chefs_df[['createdAt','MORTALITY_DATE','SAMPLED_DATE']])

    # Add field for SURVEY_SOURCE and populate as 'CHEFS'
    chefs_df['SURVEY_SOURCE'] = 'CHEFS'

    
    return chefs_df

def combine_survey_info(hunter_df, chefs_df, df):
    """
    Combines hunter survey data from Legacey AGO Survey123 (old) and CHEFS (new) into a single dataframe.
    """
    logging.info("..combining Hunter Survey data from AGO and CHEFS")

    hunter_df_renamed = hunter_df.rename(columns={
        'HUNTER_MORTALITY_DATE': 'MORTALITY_DATE',
        'HUNTER_SPECIES': 'SPECIES',
        'HUNTER_SEX': 'SEX',
        'HUNTER_LATITUDE_DD': 'LATITUDE_DD_CALC',
        'HUNTER_LONGITUDE_DD': 'LONGITUDE_DD_CALC',
        'HUNTER_CWD_EAR_CARD_ID': 'CWD_EAR_CARD_ID',
        'HUNTER_SUBMIT_DATE_TIME': 'SUBMIT_DATE_TIME'
    })


    # Get list of fields to keep from hunter_df that start with QA_
    hunter_qa_fields = [col for col in hunter_df.columns if col.startswith('QA_')]
    #print (hunter_qa_fields)
    #print("\n----------------")

    # rename chefs_df createdAt column to SUBMIT_DATE_TIME
    chefs_df = chefs_df.rename(columns={'createdAt': 'SUBMIT_DATE_TIME'})
    
    # Combine both dataframes
    surveys_df = pd.concat([hunter_df_renamed, chefs_df], ignore_index=True)
    #logging.info(f"Total number of records from Hunter Survey and CHEFS:  {len(surveys_df)}")

    # Prefix all fields except SURVEY_SOURCE and fields in hunter_qa_fields with SURVEY_ to distinguish from master sampling dataset fields when joined later
    surveys_df = surveys_df.rename(columns=lambda x: f"SURVEY_{x}" if x != "SURVEY_SOURCE" and x not in hunter_qa_fields else x)

    # Add a flag SURVEY_HAS_SAMPLING_MATCH to indicate if there is a matching sampling record in the master dataset
    surveys_df['SURVEY_HAS_SAMPLING_MATCH'] = 'No'  #default to No.  
    surveys_df.loc[surveys_df['SURVEY_CWD_EAR_CARD_ID'].isin(df['CWD_EAR_CARD_ID']), 'SURVEY_HAS_SAMPLING_MATCH'] = 'Yes'
    # Add SURVEY_CWD_EAR_CARD_ID_TEXT field as text and populate with SURVEY_CWD_EAR_CARD_ID, and where blank, fill with 'Not recorded'
    surveys_df['SURVEY_CWD_EAR_CARD_ID_TEXT'] = surveys_df['SURVEY_CWD_EAR_CARD_ID'].apply(lambda x: str(int(x)) if pd.notnull(x) else 'Not recorded')

    # Limit and re-order survey columns to only those GIS_FIELD names listed in the df_datadict starting with SURVEY_
    # Short version for exporting to xls, long version for next processing steps (not including GIS_LOAD_VERSION_DATE)
    survey_cols_short = [col for col in df_datadict['GIS_FIELD_NAME'] if col.startswith('SURVEY_')]
    survey_cols_long = survey_cols_short + hunter_qa_fields + ['SURVEY_CWD_EAR_CARD_ID_TEXT']
    # Filter surveys_df to only the columns in survey_cols_long
    filtered_surveys_df = surveys_df[[col for col in survey_cols_long if col in surveys_df.columns]]

    #--------------------
    # TEMP - as needed
    # Export short list of fields to XLS
    temp_xls_df = surveys_df[[col for col in survey_cols_short if col in surveys_df.columns]]
    # Add Temp GIS_LOAD_VERSION_DATE before exporting to excel
    temp_xls_df['GIS_LOAD_VERSION_DATE'] = current_datetime

    logging.info("..saving Filtered Survey data to object storage")
    save_xlsx_to_os(s3_client, s3_bucket_name, temp_xls_df, f'master_dataset/combined_survey_results/combined_survey_data.xlsx')
    
    #--------------------
    #print (filtered_surveys_df.dtypes)
    #print("\n----------------")

    return filtered_surveys_df

def get_ago_flayer(ago_flayer_id):
    """
    Returns AGO feature layers features and spatial dataframe

    Input:
    -ago_flayer_id:  AGO item ID for the feature layer

    """
    ago_flayer_item = gis.content.get(ago_flayer_id)
    ago_flayer_lyr = ago_flayer_item.layers[0]
    ago_flayer_fset = ago_flayer_lyr.query()
    ago_features = ago_flayer_fset.features
    ago_flayer_sdf = ago_flayer_fset.sdf

    return ago_flayer_lyr, ago_flayer_fset, ago_features, ago_flayer_sdf

def save_xlsx_to_os(s3_client, s3_bucket_name, df, file_name):
    """
    Saves an xlsx to Object Storage bucket.
    """
    xlsx_buffer = BytesIO()
    df.to_excel(xlsx_buffer, index=False)
    xlsx_buffer.seek(0)

    #logging.info(f'..Trying to export {file_name} to XLS')

    try:
        s3_client.put_object(Bucket=s3_bucket_name, Key=file_name, Body=xlsx_buffer.getvalue())
        logging.info(f'..data successfully saved {file_name} to bucket {s3_bucket_name}')
    except botocore.exceptions.ClientError as e:
        logging.error(f'..failed to save data to Object Storage: {e.response["Error"]["Message"]}')

def process_master_dataset(df):
    """
    Populates missing Latitude and Longitude values
    Format Datetime columns
    Create a temporary MERGE_ID based on WLH_ID or CWD_EAR_CARD_ID.  One of these MUST be present.
    """
    logging.info("..cleaning-up data")
    df['LATITUDE_DD'] = pd.to_numeric(df['LATITUDE_DD'], errors='coerce')
    df['LONGITUDE_DD'] = pd.to_numeric(df['LONGITUDE_DD'], errors='coerce')
    
    
    def set_source_value(row):
        if pd.notna(row['LATITUDE_DD']) and pd.notna(row['LONGITUDE_DD']):
            # TODO: Check for  SPATIAL_CAPTURE_DESCRIPTOR is not null (i.e. can be 'Email Response') in sampling sheet, and keep that value.
            if 47.0 <= row['LATITUDE_DD'] <= 60.0 and -145.0 <= row['LONGITUDE_DD'] <= -113.0:
                return 'From Submitter'
            else:
                return 'Incorrectly Entered'
        return np.nan

    df['SPATIAL_CAPTURE_DESCRIPTOR'] = df.apply(set_source_value, axis=1)
    
    columns = list(df.columns)
    long_index = columns.index('LONGITUDE_DD')
    columns.remove('SPATIAL_CAPTURE_DESCRIPTOR')
    columns.insert(long_index + 1, 'SPATIAL_CAPTURE_DESCRIPTOR')

    df = df[columns]
    
    #correct errrors in MU column
    def correct_mu_value(mu):
        if pd.isna(mu):
            return mu  # Leave NaN values unchanged
        
        mu = str(mu)  # Convert to string to avoid re.sub error

        # Remove any letters and spaces
        mu = re.sub(r'[a-zA-Z\s]', '', mu)
        
        # Remove leading zeros from the part after the hyphen
        parts = mu.split('-')
        if len(parts) == 2 and parts[1].startswith('0'):
            parts[1] = parts[1][1:]
        return '-'.join(parts)
    
    # Apply correction to WMU column
    df['WMU'] = df['WMU'].apply(correct_mu_value)

    # Fill blank MU values with 'Not Recorded'
    # TODO: Check if this is working?  results seem to be blank
    df['WMU'] = df['WMU'].fillna('Not recorded')
    df['WMU'] = df['WMU'].replace('', 'Not recorded')
    
    logging.info("..populating missing latlon from MU and Region centroids")
    #Incorrectly Entered should be reviewed by the WH Team
    def latlong_from_MU(row, df_mu):
        #if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR']) or row['SPATIAL_CAPTURE_DESCRIPTOR'] == 'Incorrectly Entered') and (row['WMU'] != 'Not Recorded' or row['WMU'] != ''):
        if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR'])) and (row['WMU'] != 'Not Recorded' or row['WMU'] != ''):
            mu_value = row['WMU']
            match = df_mu[df_mu['MU'] == mu_value]
            if not match.empty:
                row['LATITUDE_DD'] = match['CENTER_LAT'].values[0]
                row['LONGITUDE_DD'] = match['CENTER_LONG'].values[0]
                row['SPATIAL_CAPTURE_DESCRIPTOR'] = 'MU Centroid'
        return row
    
    df = df.apply(lambda row: latlong_from_MU(row, df_mu), axis=1)
    
    #populate lat/long Region centroid
    def latlong_from_Region(row, df_rg):
        #if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR']) or row['SPATIAL_CAPTURE_DESCRIPTOR'] == 'Incorrectly Entered') and row['WMU_REGION_RESPONSIBLE'] != 'Not Recorded':
        if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR'])) and (row['WMU_REGION_RESPONSIBLE'] != 'Not Recorded' or row['WMU_REGION_RESPONSIBLE'] != ''):
            rg_value = row['WMU_REGION_RESPONSIBLE']
            match = df_rg[df_rg['REGION'] == rg_value]
            if not match.empty:
                row['LATITUDE_DD'] = match['CENTER_LAT'].values[0]
                row['LONGITUDE_DD'] = match['CENTER_LONG'].values[0]
                row['SPATIAL_CAPTURE_DESCRIPTOR'] = 'Region Centroid'
        return row
    
    df = df.apply(lambda row: latlong_from_Region(row, df_rg), axis=1)
      
    df['SPATIAL_CAPTURE_DESCRIPTOR'] = df['SPATIAL_CAPTURE_DESCRIPTOR'].fillna('Unknown')

    # Remove spaces from FWID (string) between numbers
    df['FWID'] = df['FWID'].str.strip()  #remove any leading and trailing spaces for all records
    
    #df = df[
    #    (~df['FWID'].isnull())   # ~  Select where string is not null
    #    ]  
    #logging.info(f"{len(df.index)}... records found where FWID is not null.  Filling spaces...")
    #print(df['FWID'].to_string())
    df['FWID'] = df['FWID'].replace(regex=r'(?<=\d)\s+(?=\d)', value='')  #remove spaces between numbers in FWID

    #fix date formatting in some PREP_LAB_CASSET_ID strings
    df['PREP_LAB_ID'] = df['PREP_LAB_ID'].fillna('')
    df['PREP_LAB_CASSET_ID'] = df['PREP_LAB_CASSET_ID'].fillna('')
    #print(df_wh[df_wh['PREP_LAB_ID'].str.contains('23-7434')]['PREP_LAB_CASSET_ID'])
    df['PREP_LAB_CASSET_ID']= df['PREP_LAB_CASSET_ID'].astype(str)
    df['PREP_LAB_CASSET_ID']= df['PREP_LAB_CASSET_ID'].replace(regex=r' 00:00:00', value='')

    # Add the 'GIS_LOAD_VERSION_DATE' column with the current date and timestamp (PACIFIC TIME)
    # Also save current date/time as a string value to use later in file names.
    pacific_timezone = pytz.timezone('America/Vancouver')
    current_datetime = datetime.now(pacific_timezone).strftime('%Y-%m-%d %H:%M:%S')
    current_datetime_str = datetime.now(pacific_timezone).strftime('%Y%m%d_%H%M%S%p')
    current_datetime_str = current_datetime_str.lower()
    timenow_rounded = datetime.now().astimezone(pacific_timezone)

    df['GIS_LOAD_VERSION_DATE'] = current_datetime

    # Convert all columns containing 'DATE' in their names to datetime
    # TODO: Check date conversions?  
    # Revised:  Use for AGO DATE(date and time) fields only.  DATEONLY fields can stay 
    date_columns = df.columns[df.columns.str.contains('_DATE')]
    #df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')


    #ADD THIS IN AGAIN to make POwerBI work?? 2024-10-10
    #print(df.MORTALITY_DATE)
    ##convert the 'DATE' columns to only show the date part as short - don't include 00:00:00 time
    ## DO IN Web and Lab exports instead.  Need full dates for AGO and PowerBI(?)
    ##  Warning:  This converts to a string vs a date type!?
    # TODO:  Check!  Most Date columns will not need the time.  Keep as string?
    for col in date_columns:
        if '_DATE' in col and col != 'GIS_LOAD_VERSION_DATE':  #exclude GIS_LOAD_VERSION_DATE
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date   #e.g.2024-09-08 string
        if col == 'GIS_LOAD_VERSION_DATE':
            #print('**Checking GIS_LOAD_VERSION_DATE**')
            df[col] = df[col].apply(pd.to_datetime, errors='coerce')
    
    #print(df.MORTALITY_DATE)

    # Sort
    #df = df.sort_values(by=['SAMPLING_SESSION_ID','WLH_ID'])
    df = df.sort_values(by=['WLH_ID'])

    # Create Temporary MERGE_ID.  One of WLH_ID or CWD_EAR_CARD_ID must be present.
    df['MERGE_ID_TEMP'] = df['WLH_ID'].fillna(df['CWD_EAR_CARD_ID'])

    return df, current_datetime_str, timenow_rounded


def hunter_qa_and_updates_to_master(df, surveys_df):
    """
    Inputs:
        - df: Master Sampling dataset - all combined sampling records
        - surveys_df: Hunter Survey data from AGO - all records

        Incorporate Email Submission lat/longs into the UPDATED lat/long and UPDATED_SPATIAL_CAPTURE_DESCRIPTOR fields.

    Outputs:  
        1. All Hunter Survey (hs) merged records from AGO and CHEFS with QA flags for where there are discrepancies between the Hunter Survey data and the Master Sampling dataset.
           Flagged discrepancies are exported as an xls file (cwd_hunter_survey_flags_for_review_<datetime>.xlsx) to Object Storage for WH Team review, and QA flags are updated back to the Hunter data AGO.
           The WH Team will review the discrepancies and update the individual Sampling sheets as needed. They will
           update the two Review fields as appropriate, which will subsequently be added to the Hunter Survey data in AGO by this script.
        2. The Master Sampling dataset for which the lat/long and map source descriptor is corrected based on the hunter survey data, where there is a matching hs record.
           This does not include Survey records where there is no matching Ear Tag ID and therefore there is no sampling data yet.

        - hs_merged_df:   all hunter survey records with new qa columns and duplicates identified
        - flagged_hs_df:  flagged hunter survey records for qa review
        - df_wh:          updated master sampling dataset with hunter (wh) survey matches
    
    """
    #logging.info("..calculating Hunter Survey QA flags")

    # Length of initial dataframes
    #print(f"\n\t{len(df.index)}... Total Sampling Records in df")
    #print(f"\t{len(surveys_df.index)}... Total Hunter Survey Records in surveys_df\n")

    #Columns from S123 to keep
    # TODO: Add additional fields from CHEFS SURVEY - read from DataDictionary where field starts with SURVEY_
    # or just use the surveys_df (that has already been filtered) and append the QA fields as needed
    # TODO:  Add back in SURVEY_CWD_EAR_CARD_ID_TEXT 
    cols = [
    "SURVEY_CWD_EAR_CARD_ID",
    "SURVEY_CWD_EAR_CARD_ID_TEXT",
    "SURVEY_SUBMIT_DATE_TIME",
    "SURVEY_MORTALITY_DATE",
    "SURVEY_SPECIES",
    "SURVEY_SEX",
    "SURVEY_LATITUDE_DD_CALC",
    "SURVEY_LONGITUDE_DD_CALC",
    #"GEO_CHECK_PROV",
    "QA_HUNTER_SURVEY_FLAG",
    "QA_HUNTER_SURVEY_FLAG_DESC",
    "QA_HUNTER_EARCARD_DUPLICATE",
    "QA_HUNTER_SURVEY_REVIEW_STATUS"]
    #"QA_HUNTER_SURVEY_REVIEW_COMMENTS"  #no longer used - 2025-03-31

    #surveys_df = surveys_df[cols]

    logging.info("..calculating Survey QA field defaults")
    # calc/reset default values for qa survey columns. This will be re-calculated below.
    #surveys_df[['QA_HUNTER_SURVEY_FLAG','QA_HUNTER_SURVEY_FLAG_DESC']] = None
    surveys_df[['QA_HUNTER_SURVEY_FLAG','QA_HUNTER_SURVEY_FLAG_DESC','QA_HUNTER_EARCARD_DUPLICATE']] = ''
    surveys_df['QA_HUNTER_CHECK_DATE_TIME'] = current_datetime

    logging.info("..manipulating CWD_EAR_CARD_ID column")
    # convert CWD Ear Card values from df to integer to string
    df['CWD_EAR_CARD_ID'] = df['CWD_EAR_CARD_ID'].apply(lambda x: str(int(x)) if pd.notnull(x) and x != 'None' else None)
 
    #print(df.dtypes['CWD_EAR_CARD_ID'])
    #print(df['CWD_EAR_CARD_ID'].head(20))

    #print(surveys_df.dtypes[['SURVEY_CWD_EAR_CARD_ID', 'SURVEY_CWD_EAR_CARD_ID_TEXT']])
    #print(surveys_df[['SURVEY_CWD_EAR_CARD_ID', 'SURVEY_CWD_EAR_CARD_ID_TEXT']].head(20))
 
    # NOTE!  There may be duplicate Ear Card IDs in both the sampling data and the Hunter Survey data.  
    # May need to drop duplicates in df before joining to avoid addition of cross rows?
    # Duplicates should be checked and resolved by the Wildlife Health Team.
    logging.info("..joining matching sampling records to hunter survey data")
    hs_merged_df = pd.merge(left=surveys_df,
                         right=df, 
                         left_on='SURVEY_CWD_EAR_CARD_ID_TEXT',   # SURVEY_CWD_EAR_CARD_ID or SURVEY_CWD_EAR_CARD_ID_TEXT use TEXT version?
                         right_on='CWD_EAR_CARD_ID', 
                         how='left', 
                         indicator=True)

    #print(f"\n\t{len(hs_merged_df.index)}... total records in hs_merged_df\n")

    # Create Temporary MERGE_ID.  One of WLH_ID or CWD_EAR_CARD_ID must be present from the sampling data.
    hs_merged_df['MERGE_ID_TEMP'] = hs_merged_df['WLH_ID'].fillna(hs_merged_df['CWD_EAR_CARD_ID'])
    
    logging.info("..calculating Hunter Survey QA flags")
        
    # Flag records that DO NOT have an ear card id in the master sampling dataset, Otherwise, Matched
    # Note again, that there may be duplicate Ear Card IDs in both the sampling data and the Hunter Survey data.
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where(hs_merged_df['_merge'] == 'left_only',
                                                'No Sampling Match', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])  #'Matched')

    #Flag records that DO have an ear card id in the master sampling dataset
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where(hs_merged_df['_merge'] == 'both',
                                                'Matched', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    # Update 'QA_HUNTER_SURVEY_FLAG' to 'Check' for records where the species do not match
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where((hs_merged_df['SPECIES'] != hs_merged_df['SURVEY_SPECIES']) &
                                               (hs_merged_df['SPECIES'].notna() & hs_merged_df['SURVEY_SPECIES'].notna()), 
                                               'Check', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'] = np.where((hs_merged_df['SPECIES'] != hs_merged_df['SURVEY_SPECIES']) &
                                                        (hs_merged_df['SPECIES'].notna() & hs_merged_df['SURVEY_SPECIES'].notna()), 
                                                        #hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('Mismatched value(s):') + ' Species;',
                                                        'Species',
                                                        hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'])

    # Update 'QA_HUNTER_SURVEY_FLAG' to 'Check' for records where the sex does not match
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where((hs_merged_df['SEX'] != hs_merged_df['SURVEY_SEX']) &
                                               (hs_merged_df['SEX'].notna() & hs_merged_df['SURVEY_SEX'].notna()), 
                                               'Check', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'] = np.where((hs_merged_df['SEX'] != hs_merged_df['SURVEY_SEX']) &
                                                     (hs_merged_df['SEX'].notna() & hs_merged_df['SURVEY_SEX'].notna()), 
                                                     #hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('Mismatched value(s):') + ' Sex;',
                                                     hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('') + ', Sex',
                                                     #', Sex',
                                                     hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'])

    # Update 'QA_HUNTER_SURVEY_FLAG' to 'Check' for records where the mortality dates do not match
    # Ensure date formats match - Converted to Pacific Time (above) and then to date part only
    hs_merged_df['MORTALITY_DATE'] = pd.to_datetime(hs_merged_df['MORTALITY_DATE'], errors='coerce').dt.date
    hs_merged_df['SURVEY_MORTALITY_DATE'] = pd.to_datetime(hs_merged_df['SURVEY_MORTALITY_DATE'], errors='coerce').dt.date
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where((hs_merged_df['MORTALITY_DATE'] != hs_merged_df['SURVEY_MORTALITY_DATE']) &
                                               (hs_merged_df['MORTALITY_DATE'].notna() & hs_merged_df['SURVEY_MORTALITY_DATE'].notna()), 
                                               'Check', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'] = np.where((hs_merged_df['MORTALITY_DATE'] != hs_merged_df['SURVEY_MORTALITY_DATE']) &
                                                     (hs_merged_df['MORTALITY_DATE'].notna() & hs_merged_df['SURVEY_MORTALITY_DATE'].notna()), 
                                                     #hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('Mismatched value(s):') + ' Mortality Date;',
                                                     hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('') + ', Mortality Date', 
                                                     #', Mortality Date',
                                                     hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'])
    
    # Update QA_HUNTER_SURVEY_FLAG_DESC if there are hanging commas
    def update_error_desc(desc):
        #print(f"Processing desc: {desc}")  # Debugging print statement
        if pd.isna(desc):
            return desc
        return desc.lstrip(", ")
    hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'] = hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].apply(update_error_desc)

    #Drop temporary column from above
    hs_merged_df = hs_merged_df.drop(columns='_merge')

    # Find duplicate Ear Card IDs and set QA_HUNTER_EARCARD_DUPLICATE to 'DUPLICATE'
    hs_merged_df['QA_HUNTER_EARCARD_DUPLICATE'] = np.where(hs_merged_df.duplicated(subset='SURVEY_CWD_EAR_CARD_ID', keep=False), 'DUPLICATE', '')
    

    # Convert time aware datetime columns to string, otherwise cannot export to xls
    for col in ['MORTALITY_DATE','SURVEY_MORTALITY_DATE','SURVEY_SUBMIT_DATE_TIME']: 
        #hs_merged_df[col] = hs_merged_df[col].dt.tz_convert(None)  #remove timezone info (but it's still time aware)
        hs_merged_df[col] = hs_merged_df[col].astype(str)
        #hs_merged_df[col] = hs_merged_df[col].dt.tz_localize(None)
    

    # Fill nan values with empty strings
    #hs_merged_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']] = hs_merged_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']].fillna('')
    hs_merged_df[['QA_HUNTER_SURVEY_REVIEW_STATUS']] = hs_merged_df[['QA_HUNTER_SURVEY_REVIEW_STATUS']].fillna('')

    print (hs_merged_df.dtypes)
    print("\n----------------")

    #Export the Hunter Survey QA flags and associated sampling data to an XLS file for review by the WH Team
    export_cols = [
        "SURVEY_CWD_EAR_CARD_ID",
        "SURVEY_SUBMIT_DATE_TIME",
        "SURVEY_MORTALITY_DATE",
        "MORTALITY_DATE",
        "SURVEY_SPECIES",
        "SPECIES",
        "SURVEY_SEX",
        "SEX",
        "SURVEY_LATITUDE_DD_CALC",
        "SURVEY_LONGITUDE_DD_CALC",
        #"GEO_CHECK_PROV",
        "QA_HUNTER_SURVEY_FLAG",
        "QA_HUNTER_SURVEY_FLAG_DESC",
        "QA_HUNTER_EARCARD_DUPLICATE",
        "QA_HUNTER_SURVEY_REVIEW_STATUS",
        #"QA_HUNTER_SURVEY_REVIEW_COMMENTS",
        "QA_HUNTER_CHECK_DATE_TIME",
        "SAMPLING_SESSION_ID",
        "CWD_LAB_SUBMISSION_ID",
        "FISCAL_YEAR",
        "WLH_ID",
        "CWD_EAR_CARD_ID",
        "MORTALITY_CAUSE",
        "AGE_CLASS",
        "AGE_ESTIMATE",
        "DROPOFF_LOCATION",
        "WMU_REGION_RESPONSIBLE",
        "WMU",
        "SUBMITTER_SOURCE",   #SUBMITTER_TYPE/SUBMITTER_SOURCE
        "SUBMITTER_FIRST_NAME",
        "SUBMITTER_LAST_NAME",
        "SUBMITTER_PHONE",
        "FWID",
        "SAMPLED_DATE",
        "CWD_SAMPLED_IND",
        "CWD_NOT_SAMPLED_REASON",
        "SAMPLING_NOTES",
        "CWD_TEST_STATUS",
        "CWD_TEST_STATUS_DATE",
        "GIS_LOAD_VERSION_DATE"
       ]
  
    
    #Re-orders the columns, as listed above. Then sort.
    flagged_hs_df = hs_merged_df[export_cols]
    flagged_hs_df = flagged_hs_df.sort_values(by=['QA_HUNTER_SURVEY_FLAG','QA_HUNTER_SURVEY_FLAG_DESC'])

    # Select only the records that have QA flags (Not Matched) or are duplicates of Ear Card IDs
    flagged_hs_df = flagged_hs_df[(flagged_hs_df['QA_HUNTER_SURVEY_FLAG'] != 'Matched') | (flagged_hs_df['QA_HUNTER_EARCARD_DUPLICATE'] == 'DUPLICATE')]
    
    #print(f"\n\t{len(flagged_hs_df.index)}... flagged hunter survey qa records found ... of a total of {len(hs_merged_df.index)} hs_merged_df records\n")
    
    
    ### XLS export turned off as of Aug 15, 2025
    # TODO: Determine if this should be re-activated.
    #logging.info("..saving the Hunter Survey QA flags to xls")
    #save_xlsx_to_os(s3_client, s3_bucket_name, flagged_hs_df, f'qa/hunter_survey_mismatches/bck/cwd_hunter_survey_1_flags_for_review_{current_datetime_str}.xlsx')
    # TEMP
    #save_xlsx_to_os(s3_client, s3_bucket_name, hs_merged_df, f'qa/hunter_survey_mismatches/bck/cwd_hunter_survey_2_all_records_and_flags_{current_datetime_str}.xlsx')
    

    #--------------------------------------------- end of the QA checks ----------------------------------------------
    #  Merge the Hunter Survey data with the Master Sampling data:
    
    # TODO: Confirm to use all columns from surveys_df
    logging.info("..preparing Hunter Survey data for merging to master sampling dataset")
    cols = [
    "SURVEY_MORTALITY_DATE",
    "SURVEY_SPECIES",
    "SURVEY_SEX",
    "SURVEY_LATITUDE_DD_CALC",
    "SURVEY_LONGITUDE_DD_CALC",
    'SURVEY_CWD_EAR_CARD_ID_TEXT',
    "SURVEY_CWD_EAR_CARD_ID",
    "SURVEY_SUBMIT_DATE_TIME"
    ]   

    #surveys_df = surveys_df[cols]  #Use all columns?
    surveys_df['SURVEY_MORTALITY_DATE'] = pd.to_datetime(surveys_df['SURVEY_MORTALITY_DATE'], errors='coerce').dt.date

    survey_df_no_dups = surveys_df.drop_duplicates(subset="SURVEY_CWD_EAR_CARD_ID_TEXT", keep="last")
    #survey_df_no_dups = surveys_df.drop_duplicates(subset="SURVEY_CWD_EAR_CARD_ID", keep="last")


    logging.info("\n..merging Survey results to sampling dataframe")
    # merge the dataframes
    # note that if there are duplicate Ear Card IDs in the hunter survey data, 
    # then there will be multiple records in the merged dataframe, if drop_duplicates is not used, above.
    # This could be a problem if the last record is not the 'best' but will be resolved once the hunter survey spatial data is corrected.
    # if drop_duplicates is used, then only the 'last' hunter survey record will be kept.  

    # Length of initial dataframes
    #print(f"\n\t{len(df.index)}...  Sampling Records in df")
    #print(f"\t{len(surveys_df.index)}...  Hunter Survey Records in surveys_df")
    #print(f"\t{len(survey_df_no_dups.index)}...  Hunter Survey Records in survey_df_no_dups\n")

    combined_df = pd.merge(left=df,
                       right=survey_df_no_dups,
                       how="left",
                       left_on="CWD_EAR_CARD_ID",
                       right_on="SURVEY_CWD_EAR_CARD_ID_TEXT")  #SURVEY_CWD_EAR_CARD_ID or SURVEY_CWD_EAR_CARD_ID_TEXT?
                       #right_on="SURVEY_CWD_EAR_CARD_ID_TEXT").drop_duplicates(subset="SURVEY_CWD_EAR_CARD_ID_TEXT", keep="last")
    
    #print(f"\t{len(combined_df.index)}... records in combined_df")
    
    logging.info("..cleaning dataframes")
    # filter df for where hunters have updated data.  i.e. SURVEY_SOURCE (or other mandatory field) cannot be null.
    hunter_matches_df = combined_df[combined_df.SURVEY_SOURCE.notnull()].copy()
    
    # filter df for where hunters have not updated data
    xls_df = combined_df[combined_df.SURVEY_SEX.isnull()].copy()

    
    # for hunter_matches_df - update UPDATED_SPATIAL_CAPTURE_DESCRIPTOR w/ value = Hunter Survey
    xls_df['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'] =  xls_df['SPATIAL_CAPTURE_DESCRIPTOR']
    hunter_matches_df['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'] = "Hunter Survey"
    
    # clean up xls_df to conform with ago field requirements 
    xls_df[['SURVEY_SPECIES', 'SURVEY_SEX', 'SURVEY_MORTALITY_DATE']] = None

    # populate UPDATED_LATITUDE and UPDATED_LONGITUDE columns
    hunter_matches_df[['UPDATED_LATITUDE', 'UPDATED_LONGITUDE']] = hunter_matches_df[['SURVEY_LATITUDE_DD_CALC', 'SURVEY_LONGITUDE_DD_CALC']]
    xls_df[['UPDATED_LATITUDE', 'UPDATED_LONGITUDE']] = xls_df[['LATITUDE_DD', 'LONGITUDE_DD']]

    #print(f"\n\t{len(hunter_matches_df.index)}... records in hunter_matches_df")
    #print(f"\t{len(xls_df.index)}... records in xls_df")

    # re-combine dataframes
    df_wh_temp = pd.concat([hunter_matches_df, xls_df], ignore_index=True)

    # Use df_email_submissions Latitude and Longitude fields to update UPDATED_LATITUDE, UPDATED_LONGITUDE where there is a matching Ear Card ID
    # Calculate  UPDATED_SPATIAL_CAPTURE_DESCRIPTOR to 'Email Submission'
    logging.info("..adding email submission lat/long to master sampling dataset")
    df_email_submissions['Ear Card'] = df_email_submissions['Ear Card'].astype(str)
    df_wh = pd.merge(left=df_wh_temp,
                    right=df_email_submissions[['Ear Card', 'Latitude', 'Longitude']],
                    how='left',                
                    left_on='CWD_EAR_CARD_ID',
                    right_on='Ear Card')

    # Replaces any existing values 
    mask = df_wh['Latitude'].notna() & df_wh['Longitude'].notna()

    df_wh['UPDATED_LATITUDE']  = df_wh['UPDATED_LATITUDE'].where(~mask, df_wh['Latitude'])
    df_wh['UPDATED_LONGITUDE'] = df_wh['UPDATED_LONGITUDE'].where(~mask, df_wh['Longitude'])
    df_wh['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'] = df_wh['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'].where(~mask, 'Email Submission')

    # Drop unneeded columns
    df_wh = df_wh.drop(columns=['Ear Card', 'Latitude', 'Longitude'])
    
    # Move columns to the last position
    df_wh['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'] = df_wh.pop('UPDATED_SPATIAL_CAPTURE_DESCRIPTOR')
    df_wh['GIS_LOAD_VERSION_DATE'] = df_wh.pop('GIS_LOAD_VERSION_DATE')

    # Convert all columns containing 'DATE' in their names to datetime
    # TURNED OFF - Sept.2025
    #date_columns = df_wh.columns[df_wh.columns.str.contains('_DATE')]
    #df_wh[date_columns] = df_wh[date_columns].apply(pd.to_datetime, errors='coerce')

    #ADD This back in?
    #strip time portion of date/time
    ##Beware! This converts to String type! May not be compatible if using in PowerBI,etc.
    #date_columns_convert = ['COLLECTION_DATE','MORTALITY_DATE','SAMPLED_DATE','SAMPLE_DATE_SENT_TO_LAB','REPORTING_LAB_DATE_RECEIVED','CWD_TEST_STATUS_DATE','PREP_LAB_LAB_DATE_RECEIVED','PREP_LAB_DATE_FORWARDED']
    #for col in date_columns_convert:
    #    df_wh[col] = pd.to_datetime(df_wh[col]).dt.date   #e.g.2024-09-08  

    # COLLECTION_DATE to string (for PowerBI?)
    # TODO: Check if this conversion is needed
    #df_wh['COLLECTION_DATE']= df_wh['COLLECTION_DATE'].astype(str)

    #replace Not a Time (NaT) and nan for entire dataframe
    df_wh = df_wh.replace(['NaT'], '')

    fill_values = {}
    for col in df_wh.columns:
        #if pd.api.types.is_numeric_dtype(df_wh[col]):
            #fill_values[col] = 0
        #elif pd.api.types.is_datetime64_any_dtype(df_wh[col]):
            #fill_values[col] = pd.Timestamp('1900-01-01')
        if pd.api.types.is_object_dtype(df_wh[col]):
            fill_values[col] = ''
        #else:
            #fill_values[col] = 'Unknown'  # Fallback for other types

    # Apply the fill values
    df_wh.fillna(value=fill_values, inplace=True)


    #Sort
    df_wh = df_wh.sort_values(by=['WLH_ID'])

    #Add logging for record counts
    logging.info(f"\n\t{len(df_wh.index)}... records in df_wh - master sampling dataset with hunter survey matches")
    logging.info(f"\t{len(hs_merged_df.index)}... records in hs_merged_df - hunter survey records")
    logging.info(f"\t{len(flagged_hs_df.index)}... records in flagged_hs_df - flagged hunter survey qa records \n")

    return df_wh, hs_merged_df, flagged_hs_df

def update_hs_review_tracking_list(flagged_hs_df):

    """
    Reads the existing master hunter survey QA tracking xls file into a dataFrame, reads new hs data from AGO, 
    compares the records, and appends new records based on SURVEY_CWD_EAR_CARD_ID to the master tracking dataFrame.

    This tracking xls preserves the original flags and review status for each record, before any changes may be made in the individual sampling sheets by the WH Team.
    This lets the team see what the original QA flags were, and what changes were made to the records.
    
    NOTE:  28-Feb-2025 - there is a glitch in S3 storage where the file is disappearing. This seems to be related to when the file is open
      in Excel and temp files are created. 

    Parameters:
    - flagged_hs_df: DataFrame containing the flagged hunter survey records.
    
    Outputs:
    - updated_tracking_df:  Updated static master hunter survey with newly flagged qa records appended.
    """

    # Read the static master XLS file into a DataFrame
    static_xls_path = 'qa/tracking/cwd_hunter_survey_qa_flag_status_tracking_master.xlsx'
    logging.info(f"...Checking for file: {static_xls_path}")
    try:
        obj = s3_client.get_object(Bucket=s3_bucket_name, Key=static_xls_path)
        data = obj['Body'].read()
        excel_file = pd.ExcelFile(BytesIO(data))

        logging.info(f"...reading file: {static_xls_path}")

        static_df = pd.read_excel(excel_file, sheet_name='Sheet1')

        # Compare records and append new flagged hunter survey records to the static DataFrame
        new_records = flagged_hs_df[~flagged_hs_df['SURVEY_CWD_EAR_CARD_ID'].isin(static_df['SURVEY_CWD_EAR_CARD_ID'])]
        
        #if there are new records to add, then append them to the static DataFrame
        if len(new_records) == 0:
            updated_tracking_df = static_df
            logging.info(f"...no new records to add to the Hunter QA tracking list")
        else:
            # Append new records to the static DataFrame
            updated_tracking_df = pd.concat([static_df, new_records], ignore_index=True)

            # Fill nan values with empty strings
            updated_tracking_df[['QA_SURVEY_SURVEY_REVIEW_STATUS']] = updated_tracking_df[['QA_HUNTER_SURVEY_REVIEW_STATUS']].fillna('')
            #replace Not a Time (NaT) for entire dataframe
            updated_tracking_df = updated_tracking_df.replace(['NaT'], '')

            # Sort the DataFrame
            updated_tracking_df = updated_tracking_df.sort_values(by=['QA_HUNTER_SURVEY_FLAG','QA_HUNTER_SURVEY_FLAG_DESC','SURVEY_CWD_EAR_CARD_ID'])

            # Overwrite to xls
            logging.info("..saving the revised tracking list to xls")
            logging.info(f"\n{len(new_records)}... new flagged records found for Hunter Survey... added to {len(static_df)} existing records\n")
            #save_xlsx_to_os(s3_client, s3_bucket_name, updated_tracking_df, f'qa/hunter_survey_mismatches/cwd_hunter_survey_qa_flag_status_tracking_master_{current_datetime_str}.xlsx')
            save_xlsx_to_os(s3_client, s3_bucket_name, updated_tracking_df, f'qa/tracking/cwd_hunter_survey_qa_flag_status_tracking_master.xlsx')

    except Exception as e:
        logging.info(f"\n***WARNING...tracking file not currently found: {static_xls_path}\n...Skipping the update of the xls tracking list.\n")
        # Create an empty dataframe as a place holder until xls is back online in Object Storage (S3)
        updated_tracking_df = pd.DataFrame()

    return updated_tracking_df


def update_hunter_flags_to_ago(df_hs, updated_tracking_df, spatial_fl_id):
    """
    Update the AGO Hunter Survey Feature layer with the latest QA flags (Species, Sex, Mortalitiy Date mismatches) 
    from df_hs, and the qa tracking status from updated_tracking_df
    
    Note that some hunter survey FL records aren't being updated by the script, 
    e.g. if there are duplicate IDs, or if new records are added in the hunter survey after the data is grabbed from AGO by this script.
    Also, if an EarCard Number is updated in AGO, it will not match the original tracking status record.  The status
    will need to be updated manually in AGO.

    Inputs:
    - df_hs: DataFrame containing all hunter survey data, with QA flags.
    - updated_tracking_df: DataFrame containing the QA master tracking status for the hunter survey data.
    - spatial_fl_id: AGO item ID for the hunter survey feature layer.

    Returns:
    - None  
    """ 

    #logging.info(f'\nSelecting Records to Update in Hunter Survey FL' )
    ## NOTE - may need to update/default all records, once a check is reviewed?  Will Flag status change? TBD
    #updated_df = df_hs[(df_hs['QA_HUNTER_DETAILS_FLAG'] == 'Check') | (df_hs['QA_HUNTER_DETAILS_FLAG'] == 'No Sampling Match')]
    #updated_df = df_hs[(df_hs['QA_HUNTER_SURVEY_FLAG'] != 'Matched')]
    #print('Records to Revise:  ',len(updated_df))
    
    #print('Records to Revise:  ',len(df_hs))

    
    cols = [
    "SAMPLING_SESSION_ID",
    "WLH_ID",
    "SURVEY_CWD_EAR_CARD_ID_TEXT",
    "SURVEY_CWD_EAR_CARD_ID",
    "QA_HUNTER_EARCARD_DUPLICATE",
    "QA_HUNTER_SURVEY_FLAG",
    "QA_HUNTER_SURVEY_FLAG_DESC",
    "QA_HUNTER_SURVEY_REVIEW_STATUS"
    #"QA_HUNTER_SURVEY_REVIEW_COMMENTS",
    ]   
    updated_df = df_hs[cols]
    logging.info(f'Hunter Survey Records to Revise :  {len(updated_df)}')
    
    
    ##### Use Pandas Dataframes to update AGO Layers
    ## get spatial features from AGO
    hunter_survey_item = gis.content.get(spatial_fl_id)
    hs_lyr = hunter_survey_item.layers[0]
    hs_fset = hs_lyr.query()
    hs_features = hs_fset.features
    hs_sdf = hs_fset.sdf
    #print(hs_sdf)
    logging.info(f'Records in Hunter Survey Layer:  {len(hs_sdf)}')
    

    uCount = 0   #initiate counter

    logging.info('Updating the AGO Hunter Survey Feature Layer with the latest QA Flags...please be patient')
    # Update the Feature Layer with the QA flags for matching records.  
    # This may overwrite existing values in the FL with the new qa values.  That is ok.
    uCount = 0   #initiate counter
    fl_sum_fld = 'SURVEY_CWD_EAR_CARD_ID'
    df_sum_fld = 'SURVEY_CWD_EAR_CARD_ID'
    for ROW_ID in updated_df[fl_sum_fld]:
        try:
            upd_feature = [f for f in hs_features if f.attributes[fl_sum_fld] == ROW_ID][0]  #get the spatial feature
            upd_row = updated_df.loc[updated_df[df_sum_fld] == ROW_ID]   #get the dataframe row
            upd_val_1 = upd_row['QA_HUNTER_SURVEY_FLAG'].values[0]
            upd_val_2 = upd_row['QA_HUNTER_SURVEY_FLAG_DESC'].values[0]
            upd_val_3 = upd_row['QA_HUNTER_EARCARD_DUPLICATE'].values[0]
            #print(upd_val)
            upd_feature.attributes['QA_HUNTER_SURVEY_FLAG'] = str(upd_val_1)
            upd_feature.attributes['QA_HUNTER_SURVEY_FLAG_DESC'] = str(upd_val_2)
            upd_feature.attributes['QA_HUNTER_EARCARD_DUPLICATE'] = str(upd_val_3)
            #upd_feature.attributes['GIS_LOAD_VERSION_DATE'] = timenow_rounded #current_datetime_str
            hs_lyr.edit_features(updates=[upd_feature])
            #print(f"Updated {upd_feature.attributes[fl_sum_fld]} sample count to {upd_val} at {timenow_rounded}", flush=True)
            uCount = uCount + 1
        except Exception as e:
            logging.info(f"\tCould not update {ROW_ID}. Exception: {e}.  Ear Card ID may have been updated.")
            continue
    logging.info(f'DONE updating the Hunter Survey AGO Feature Layer with the new QA Flags for:  {uCount} records.')

    # if updated_tracking_df is not empty (i.e. xls file is available and has records), then update the AGO FL with the review status and comments 
    # No longer updating comments to AGO - 2025-03-31 - as comments may contain personal names. 
    if not updated_tracking_df.empty:
        logging.info('Updating the AGO Hunter Survey Feature Layer with the latest QA tracking review status and comments ...')

        # Fill nan values with empty strings
        updated_tracking_df[['QA_HUNTER_SURVEY_REVIEW_STATUS']] = updated_tracking_df[['QA_HUNTER_SURVEY_REVIEW_STATUS']].fillna('')
    
        """# Testing: filter df for where there are review status /comments
        updated_tracking_df_select = updated_tracking_df[
            (updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_STATUS'].notna() & updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_STATUS'].str.strip() != '') |
            (updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_COMMENTS'].notna() & updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_COMMENTS'].str.strip() != '')
            ]
        
        print(f"{len(updated_tracking_df_select)}... records found for Hunter Survey review tracking... of a total of {updated_tracking_df} flagged records\n")
        """
        
        # Update the Feature Layer with the QA flags for matching records, if there are any available records.
        # Use all records in the updated_tracking_df, without any filter, in order to update with any revised review status or comments.
        # FIX - don't use QA_HUNTER_SURVEY_REVIEW_COMMENTS in AGO, as it may contain personal names. 2025-03-31
        
        uCount = 0   #initiate counter
        for ROW_ID in updated_tracking_df[fl_sum_fld]:
            try:
                upd_feature = [f for f in hs_features if f.attributes[fl_sum_fld] == ROW_ID][0]  #get the spatial feature
                upd_row = updated_tracking_df.loc[updated_tracking_df[df_sum_fld] == ROW_ID]   #get the dataframe row
                upd_val_1 = upd_row['QA_HUNTER_SURVEY_REVIEW_STATUS'].values[0]
                #upd_val_2 = upd_row['QA_HUNTER_SURVEY_REVIEW_COMMENTS'].values[0]
                upd_feature.attributes['QA_HUNTER_SURVEY_REVIEW_STATUS'] = str(upd_val_1)
                #upd_feature.attributes['QA_HUNTER_SURVEY_REVIEW_COMMENTS'] = str(upd_val_2)
                hs_lyr.edit_features(updates=[upd_feature])
                uCount = uCount + 1
            except Exception as e:
                logging.info(f"\tCould not update {ROW_ID}. Exception: {e}.  Ear Card ID may have been updated.")
                continue
        logging.info(f'DONE updating the Hunter Survey AGO Feature Layer with the Review Status and Comments for:  {uCount} records.')
    else:
        logging.info('No records found for Hunter Survey review tracking at the moment...no updates made to the AGO Feature Layer.')
    
    return

def find_and_save_duplicates(df, fld, output_path):
    """
    Finds duplicate field values (e.g. CWD_EAR_CARD_ID) in a DataFrame and saves them to an Excel report.
    
    Parameters:
    - df: DataFrame containing the data to be checked.
    - fld: Name of the field to check for duplicates.
    - output_path: Path to save the Excel report.
    
    Returns:
    - DataFrame containing the duplicate records, not including null fld values.
    """
    
    # Exclude null values
    df_non_null = df[df[fld].notna() & (df[fld] != '')]

    # Find duplicate values
    duplicate_mask = df_non_null.duplicated(subset= fld, keep=False)
    duplicate_df = df_non_null[duplicate_mask]
    
    # Sort by fld
    duplicate_df = duplicate_df.sort_values(by=fld)

    # Save the duplicate values to an Excel report
    if not duplicate_df.empty:
        save_xlsx_to_os(s3_client, s3_bucket_name, duplicate_df, output_path)
        #print(f"Duplicate values saved to {output_path}")
    else:
        logging.info(f"No duplicate values found for {fld}")
    
    return duplicate_df



def check_point_within_poly(df_wh, mu_flayer_sdf, municipality_sdf, buffer_distance, longcol, latcol):
    """
    Flags points where the sampling MU or Region does not match the spatial MU or MU Responsible Region.
    Note that there may be cases where the point is very close to a Region or MU border.
    
    This is only for records where the UPDATED_SPATIAL_CAPTURE_DESCRIPTOR is Hunter Survey or From Submitter i.e. not for centroid based locations where there is no lat/long available.

    Add new fields to the master xls and FL for QA checks - QA_REG_WMU_CHECK, (QA_REG_WMU_CHECK_DATE_TIME), QA_MUNI_CHECK

    Add fields for updated/calculated Region, WMU, and Muni.  Calc based on spatial overlay with submitter/survey x/y locations, if known.
    Otherwise populate from sampling sheet, if available for other centroid based locations.
    """
    # convert MU, Region, and Municipality feature layers to geodataframes
    mu_gdf = gpd.GeoDataFrame(mu_flayer_sdf, geometry='SHAPE', crs="EPSG:4326")[['WILDLIFE_MGMT_UNIT_ID','REGION_RESPONSIBLE_ID','REGION_RESPONSIBLE_NAME','SHAPE']]
    #rg_gdf = gpd.GeoDataFrame(rg_flayer_sdf, geometry='SHAPE', crs="EPSG:4326")[['REGION_NAME', 'SHAPE']]
    municipality_gdf = gpd.GeoDataFrame(municipality_sdf, geometry='SHAPE', crs="EPSG:3005")[['ADMIN_AREA_ABBREVIATION', 'SHAPE']]
    
    df_length = len(df_wh)
    #Drop rows where updated lat or long is missing
    df = df_wh.dropna(subset=[latcol, longcol])
    # filter for specific records from Hunter Survey or Submitter - i.e. based on specific lat/longs, not centroids.
    # TODO:  add "Email Submission"
    df_select = df[(df['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'] == 'Hunter Survey') | (df['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'] == 'From Submitter')]
    
    logging.info(f"\t{len(df_select)}... records found for Hunter or Submitter locations... of a total of {df_length} sampling records")
    
    # Calc / Recalc default values for QA Checks - overwrites previous values, if any.
    df_select['QA_REG_WMU_CHECK'] = ''
    df_select['QA_REG_WMU_CHECK_DATE_TIME'] = current_datetime
    # Add empty tracking columns for WH Team.  These are not published to the AGO FL, currently.
    df_select[['QA_REG_WMU_CHECK_STATUS','QA_REG_WMU_CHECK_COMMENTS']] = ''

    # convert df to geodataframe
    gdf = gpd.GeoDataFrame(df_select, geometry=gpd.points_from_xy(df_select[longcol], df_select[latcol]), crs="EPSG:4326")

    # spatial join the mu and region dataframes to the geodataframe
    logging.info("..performing spatial join")
    sjoin_mu_gdf = gpd.sjoin(gdf, mu_gdf, how='left', predicate='within', lsuffix='left', rsuffix='right')
    # drop the index column to prep for the next spatial join
    #sjoin_mu_gdf = sjoin_mu_gdf.drop(['index_left', 'index_right'], axis=1, errors='ignore')
    sjoin_gdf = sjoin_mu_gdf.drop(['index_left', 'index_right'], axis=1, errors='ignore')

    #sjoin_gdf = gpd.sjoin(sjoin_mu_gdf, rg_gdf, how='left', predicate='within', lsuffix='left', rsuffix='right')
    # drop the index column to prep for the next spatial join
    #sjoin_gdf = sjoin_gdf.drop(['index_left', 'index_right'], axis=1, errors='ignore')

    # If WMU REGION_RESPONSIBLE_NAME = 'Omineca' and REGION_NAME = 'Peace' then change to 'Peace'
    # To overide lagging WMU REGION_RESPONSIBLE_NAME field where the Omineca/Peace was not split out.
    #sjoin_gdf.loc[(sjoin_gdf['REGION_RESPONSIBLE_NAME'] == 'Omineca') & (sjoin_gdf['REGION_NAME'] == 'Peace'), 'REGION_RESPONSIBLE_NAME'] = 'Peace'
    #sjoin_gdf.loc[sjoin_gdf['REGION_RESPONSIBLE_ID'] == '7B', 'REGION_RESPONSIBLE_NAME'] = 'Peace'
    #print(sjoin_gdf.columns)

    
    #TEMP - Compare spatial (ENV REGION)REGION_NAME and (WMU)REGION_RESPONSIBLE_NAME and list discrepancies.  Ignore blank and na values.
    '''# Replace NaN with empty strings for comparison
    sjoin_gdf['REGION_NAME'] = sjoin_gdf['REGION_NAME'].fillna('')
    sjoin_gdf['REGION_RESPONSIBLE_NAME'] = sjoin_gdf['REGION_RESPONSIBLE_NAME'].fillna('')

    # Create the discrepancy column
    sjoin_gdf['REGION_DISCREPANCY'] = np.where(
        (sjoin_gdf['REGION_NAME'] != '') & 
        (sjoin_gdf['REGION_RESPONSIBLE_NAME'] != '') & 
        (sjoin_gdf['REGION_NAME'] != sjoin_gdf['REGION_RESPONSIBLE_NAME']),
        'Mismatch',
        'Match')

    #sjoin_gdf['REGION_DISCREPANCY'] = np.where(sjoin_gdf['REGION_NAME'] != sjoin_gdf['REGION_RESPONSIBLE_NAME'], 'Mismatch', 'Match')
    region_discrepancies = sjoin_gdf[sjoin_gdf['REGION_DISCREPANCY'] == 'Mismatch']
    logging.info(f"\n{len(region_discrepancies)}... records found with REGION_NAME and WMU_REGION_RESPONSIBLE discrepancies\n")
    print(region_discrepancies[['WLH_ID','WILDLIFE_MGMT_UNIT_ID','WMU_REGION_RESPONSIBLE','REGION_NAME','REGION_RESPONSIBLE_NAME']])
    sys.exit()'''

    # compare columns and update flags
    logging.info("..updating flags")

    # Where there are sampling blanks - populate sampling Region/MU w/ values from the spatial join?
    # or just keep in the spatial columns.  WMU_SPATIAL, ENV_REGION_SPATIAL
    """ 
    sjoin_gdf['WMU'] = sjoin_gdf['WMU'].fillna(sjoin_gdf['WILDLIFE_MGMT_UNIT_ID'])
    sjoin_gdf['WMU_REGION_RESPONSIBLE'] = sjoin_gdf['WMU_REGION_RESPONSIBLE'].fillna(sjoin_gdf['REGION_RESPONSIBLE_NAME'])
    """
    

    ##TODO: DOUBLE CHECK - are missing MUs from Survey Ear Card filled in in the spatial column?
    # TODO: Check that Survey Region is filled in, if Survey MU is available.
    
    # Flag records where the sampling Region does not match the spatial WMU Region Responsible
    sjoin_gdf['QA_REG_WMU_CHECK'] = np.where((sjoin_gdf['REGION_RESPONSIBLE_NAME'].notna()) & (sjoin_gdf['WMU_REGION_RESPONSIBLE'].notna()) &
                                                    (sjoin_gdf['REGION_RESPONSIBLE_NAME'] != sjoin_gdf['WMU_REGION_RESPONSIBLE']),
                                                    'REG',
                                                    sjoin_gdf['QA_REG_WMU_CHECK'])
    
    # Flag records where the sampling MU does not match the spatial MU
    sjoin_gdf['QA_REG_WMU_CHECK'] = np.where((sjoin_gdf['WILDLIFE_MGMT_UNIT_ID'].notna()) & (sjoin_gdf['WMU'].notna()) & 
                                                    (sjoin_gdf['WILDLIFE_MGMT_UNIT_ID'] != sjoin_gdf['WMU']),
                                                    sjoin_gdf['QA_REG_WMU_CHECK'].fillna('') + ', WMU',
                                                    sjoin_gdf['QA_REG_WMU_CHECK'])

    # Update QA_REG_WMU_CHECK if there are hanging commas
    def update_error_desc(desc):
        if pd.isna(desc):
            return desc
        return desc.lstrip(", ")
    sjoin_gdf['QA_REG_WMU_CHECK'] = sjoin_gdf['QA_REG_WMU_CHECK'].apply(update_error_desc)

    
    logging.info("..adding municipalities")
    logging.info(f"..buffering municipality boundaries by {buffer_distance} in EPSG:3005")
    municipality_gdf['geometry'] = municipality_gdf.geometry.buffer(buffer_distance)

    logging.info("..projecting Municipality gdf from EPSG:3005 to EPSG:4326")
    municipality_gdf = municipality_gdf.to_crs("EPSG:4326")

    # spatial join the municipality boundaries to the CWD spatial gdf
    logging.info(f"..spatial joining master gdf to municipality boundaries")
    sjoin_gdf = gpd.sjoin(sjoin_gdf, municipality_gdf, how='left', predicate='intersects')

    # change NRRM Municipality name to Northern Rockies clarity
    sjoin_gdf.loc[sjoin_gdf['ADMIN_AREA_ABBREVIATION']=='NRRM', 'ADMIN_AREA_ABBREVIATION'] = 'Northern Rockies'

    # TODO: change NRRM to longer name 
    # TODO: CHECK logic. flag points w/ MUNICIPALITY_NAME from the sampling sheet, that fall outside the muni buffer (50m) (if x/y is not centroid based). 
    # Only if Cranbook, Invermere, Kimberley.  For any mortality cause.
    sjoin_gdf['QA_MUNI_CHECK'] = np.where((sjoin_gdf['MUNICIPALITY_NAME'].isin(['Cranbrook', 'Invermere', 'Kimberley'])) &
                                            ((sjoin_gdf['ADMIN_AREA_ABBREVIATION'].isna()) | (sjoin_gdf['ADMIN_AREA_ABBREVIATION'] == '')),
                                            'CHECK MUNI','')

    muni_checks = sjoin_gdf[(sjoin_gdf['QA_MUNI_CHECK'] == 'CHECK MUNI')]
    logging.info(f"\n{len(muni_checks)}... records found for QA_MUNI_CHECK\n")
    
    # drop joined columns
    # sjoin_df = sjoin_gdf.drop(['geometry', 'index_left', 'index_right', 'WILDLIFE_MGMT_UNIT_ID', 'REGION_NAME'], axis=1, errors='ignore')
    sjoin_df = sjoin_gdf.drop(['geometry', 'geometry_left', 'geometry_right', 'index_left', 'index_right'], axis=1, errors='ignore')

    """
    # append the rows with Unknown locations back to the dataframe
    unknown_df = df[df['UPDATED_SPATIAL_CAPTURE_DESCRIPTOR'].isin(['Region Centroid', 'MU Centroid', 'Unknown'])]
    sjoin_df = pd.concat([sjoin_df, unknown_df], ignore_index=True)
    """
    
    print("Number of rows in sjoin_df:", len(sjoin_gdf))
    #print("Column names in sjoin_df:", sjoin_gdf.columns.tolist())


    # For the select coordinates (from survey or submitter), keep the Region, Mu, and Municipality (plus buffer)
     # based on the spatial join.
     # Otherwise, take the original values from the sampling sheet, where provided (see below).

    # Rename spatial columns
    sjoin_df = sjoin_df.rename(columns={
        'WILDLIFE_MGMT_UNIT_ID': 'UPDATED_WMU',   #'WMU_SPATIAL',
        #'REGION_NAME': 'UPDATED_WMU_REG_RESPONSIBLE',  #'ENV_REGION_SPATIAL'  --USE REGION_RESPONSIBLE_NAME INSTEAD
        'REGION_RESPONSIBLE_NAME': 'UPDATED_WMU_REG_RESPONSIBLE',  #from WMU join
        'ADMIN_AREA_ABBREVIATION': 'UPDATED_MUNICIPALITY'
    })

    #Get select columns
    # TODO: Check if any columns are missing from the new CHEFS survey
    cols = [
        "SAMPLING_LEAD_FULL_NAME",
        "SAMPLING_CITY_LOCATION",
        "SAMPLING_SESSION_ID",
        "CWD_LAB_SUBMISSION_ID",
        "FISCAL_YEAR",
        "WLH_ID",
        "CWD_EAR_CARD_ID",
        "DROPOFF_LOCATION",
        "LATITUDE_DD",
        "LONGITUDE_DD",
        "SPATIAL_CAPTURE_DESCRIPTOR",
        "SURVEY_LATITUDE_DD_CALC",
        "SURVEY_LONGITUDE_DD_CALC",
        "UPDATED_LATITUDE",
        "UPDATED_LONGITUDE",
        "UPDATED_SPATIAL_CAPTURE_DESCRIPTOR",
        "WMU_REGION_RESPONSIBLE",
        "UPDATED_WMU_REG_RESPONSIBLE",
        "WMU",
        "UPDATED_WMU",
        "MUNICIPALITY_NAME",
        "UPDATED_MUNICIPALITY",
        "QA_REG_WMU_CHECK",
        "QA_REG_WMU_CHECK_DATE_TIME",
        "QA_REG_WMU_CHECK_STATUS",
        "QA_REG_WMU_CHECK_COMMENTS",
        "QA_MUNI_CHECK",
        "GIS_LOAD_VERSION_DATE",
        "COLLECTION_DATE",
        "IDENTIFIER_TYPE",
        "IDENTIFIER_ID",
        "SPECIES",
        "SEX",
        "AGE_CLASS",
        "AGE_ESTIMATE",
        "MORTALITY_CAUSE",
        "MORTALITY_DATE",
        "SUBMITTER_SOURCE",   #SUBMITTER_TYPE/SUBMITTER_SOURCE
        "SUBMITTER_FIRST_NAME",
        "SUBMITTER_LAST_NAME",
        "SUBMITTER_PHONE",
        "FWID",
        "SAMPLED_DATE",
        "SAMPLED_IND",
        "CWD_SAMPLED_IND",
        "CWD_TEST_STATUS",
        "CWD_TEST_STATUS_DATE",
        "SURVEY_CWD_EAR_CARD_ID",
        "SURVEY_MORTALITY_DATE",
        "SURVEY_SPECIES",
        "SURVEY_SEX",
        "SURVEY_SUBMIT_DATE_TIME"
        ]

    
    #Re-orders the columns, as listed above. Then sort.
    mu_reg_muni_df = sjoin_df[cols]
    #mu_reg_muni_df = mu_reg_muni_df.sort_values(by=['QA_REG_FLAG','QA_WMU_FLAG'])

    # Create Temporary MERGE_ID.  One of WLH_ID or CWD_EAR_CARD_ID must be present.
    mu_reg_muni_df['MERGE_ID_TEMP'] = mu_reg_muni_df['WLH_ID'].fillna(mu_reg_muni_df['CWD_EAR_CARD_ID'])

    # How many flagged?
    #mu_reg_flagged = mu_reg_flagged[(mu_reg_flagged['QA_REG_FLAG'] == 'Check ENV REGION Survey') | (mu_reg_flagged['QA_WMU_FLAG'] == 'Check WMU')]
    mu_reg_flagged = mu_reg_muni_df[(mu_reg_muni_df['QA_REG_WMU_CHECK'] != '')]
    logging.info(f"\t{len(mu_reg_flagged)}... records found for Flagged REGION or WMU mistmatches from Hunter or Submitter... of a total of {df_length} sampling records\n")

    
    # convert WMU column to string
    mu_reg_flagged['WMU'] = mu_reg_flagged['WMU'].astype(str)
    mu_reg_flagged['UPDATED_WMU'] = mu_reg_flagged['UPDATED_WMU'].astype(str)

    # sort
    mu_reg_flagged = mu_reg_flagged.sort_values(by=['WLH_ID'])

    # TEMP - Export list to XLS for backup.   This is the current list of flagged records vs the master tracking list, which may
    # include old records that have been reviewed and updated and comments added.
    #save_xlsx_to_os(s3_client, s3_bucket_name, mu_reg_flagged, f'qa/mu_reg_checks/bck/cwd_master_mu_region_checks_{current_datetime_str}.xlsx')
    # TEMP - to create inital master tracking xls.
    #save_xlsx_to_os(s3_client, s3_bucket_name, mu_reg_flagged, f'qa/tracking/cwd_sampling_mu_region_checks_tracking_master.xlsx')

    # Add REG, WMU, MUNI and CHECK fields to master sampling df
    # Updated: If WLH_ID is no longer mandatory, may need to merge on WLH_ID or CWD_EAR_CARD_ID, 
    # so create a temp MERGE_ID that fills any blank WLH_ID with the CWD_EAR_CARD_ID ID, or concatenates them.
    #df_wh = df_wh.merge(mu_reg_flagged[['WLH_ID','QA_REG_WMU_CHECK', 'UPDATED_WMU_REG_RESPONSIBLE','UPDATED_WMU','UPDATED_MUNICIPALITY']], on='WLH_ID', how='left')
    df_wh = df_wh.merge(mu_reg_muni_df[['MERGE_ID_TEMP','QA_REG_WMU_CHECK', 'UPDATED_WMU_REG_RESPONSIBLE','UPDATED_WMU','UPDATED_MUNICIPALITY','QA_MUNI_CHECK']], on='MERGE_ID_TEMP', how='left')
    df_wh['GIS_LOAD_VERSION_DATE'] = df_wh.pop('GIS_LOAD_VERSION_DATE')   #Move to last column

    # Fill missing UPDATED_ values from sampling sheet for centroid based location, if sampling info available
    df_wh['UPDATED_WMU_REG_RESPONSIBLE'] = df_wh['UPDATED_WMU_REG_RESPONSIBLE'].fillna(df_wh['WMU_REGION_RESPONSIBLE'])
    df_wh['UPDATED_WMU'] = df_wh['UPDATED_WMU'].fillna(df_wh['WMU'])
    # Calc UPDATED_MUNICIPALITY for remaining centroid based locations not populated above in spatial join
    df_wh['UPDATED_MUNICIPALITY'] = df_wh['UPDATED_MUNICIPALITY'].fillna(df_wh['MUNICIPALITY_NAME'].where(df_wh['MUNICIPALITY_NAME'].notna() & (df_wh['MUNICIPALITY_NAME'].str.strip() != '')))

    return df_wh, mu_reg_flagged

def update_sampling_mu_reg_review_tracking_list(flagged_df):

    """
    Reads the existing master sampling Tracking xls file into a dataFrame, reads flagged QA_REG_WMU_CHECK records , 
    compares the records, and appends new records based on WLH_ID to the master tracking dataFrame.
    TODO:  Merge based on MERGE_ID_TEMP

    This tracking xls preserves the original flags and review status for each record, before any changes may be made in the individual sampling sheets by the WH Team.
    This lets the team see what the original QA flags were, and what changes were made to the records.
    
    Parameters:
    - flagged_df: DataFrame containing the flagged hunter survey records.
    
    Outputs: (none)
    - updated_tracking_df:  Updated static master tracking sheet with newly flagged qa records appended.
    """

    # Read the static master XLS file into a DataFrame
    static_xls_path = 'qa/tracking/cwd_sampling_mu_region_checks_tracking_master.xlsx'
    logging.info(f"...Checking for file:: {static_xls_path}")
    try:
        obj = s3_client.get_object(Bucket=s3_bucket_name, Key=static_xls_path)
        data = obj['Body'].read()
        excel_file = pd.ExcelFile(BytesIO(data))

        logging.info(f"...reading file: {static_xls_path}")

        static_df = pd.read_excel(excel_file, sheet_name='Sheet1')

        # Compare records and append new flagged hunter survey records to the static DataFrame
        # TODO:  Compare based on MERGE_ID_TEMP, as WHL_ID is no longer required.  
        new_records = flagged_df[~flagged_df['WLH_ID'].isin(static_df['WLH_ID'])]
        
        logging.info(f"\n... {len(new_records)} new flagged records found for MU or REGION mismatches... compared to {len(static_df)} existing records\n")
        
        new_records = new_records.reset_index()

        # Ensure both DataFrames have the same columns and data types
        static_df['QA_REG_WMU_CHECK_STATUS'] = static_df['QA_REG_WMU_CHECK_STATUS'].astype(object)
        static_df['QA_REG_WMU_CHECK_COMMENTS'] = static_df['QA_REG_WMU_CHECK_COMMENTS'].astype(object)

        new_records['QA_REG_WMU_CHECK_STATUS'] = new_records['QA_REG_WMU_CHECK_STATUS'].astype(object)
        new_records['QA_REG_WMU_CHECK_COMMENTS'] = new_records['QA_REG_WMU_CHECK_COMMENTS'].astype(object)
        
        for col in static_df.columns:
            if col in new_records.columns:
                #logging.info(f"\tColumn {col} found: {static_df[col].dtype} ... {new_records[col].dtype}")
                new_records[col] = new_records[col].astype(static_df[col].dtype)
            else:
                logging.info(f"Column {col} NOT found in new_records")
        

        # Append new records to the static DataFrame
        tracking_df = pd.concat([static_df, new_records], ignore_index=True)

        # Fill nan values with empty strings
        tracking_df[['QA_REG_WMU_CHECK_STATUS', 'QA_REG_WMU_CHECK_COMMENTS']] = tracking_df[['QA_REG_WMU_CHECK_STATUS', 'QA_REG_WMU_CHECK_COMMENTS']].fillna('')
        #replace Not a Time (NaT) for entire dataframe
        tracking_df = tracking_df.replace(['NaT'], '')

        # Sort the DataFrame
        tracking_df = tracking_df.sort_values(by=['WLH_ID'])

        # Overwrite to xls
        logging.info("..saving the revised tracking list to xls")
        #print(f"\n{len(new_records)}... new flagged records found for MU or REGION mismatches... added to {len(static_df)} existing records\n")
        save_xlsx_to_os(s3_client, s3_bucket_name, tracking_df, f'qa/tracking/cwd_sampling_mu_region_checks_tracking_master.xlsx')

    except Exception as e:
        logging.info(f"\n***WARNING...tracking file not currently found: {static_xls_path}\n...Skipping the update of the xls tracking list.\n")

    return

def sampled_summary_by_unit(df_sampled, df_sum_fld, fl_sum_fld, summ_zones_lyr, summ_zones_features, summ_zones_sdf):
    """
    Summarize a count of  'CWD_SAMPLED' by spatial unit - e.g. MU or MU Region Responsible.
    Add Counts: by fiscal year field, and overall total.
    Export to excel in object storage.
    Update values in existing AGO feature layer.

    (df_sampled,'WMU', 'WILDLIFE_MGMT_UNIT_ID', mu_flayer_lyr, mu_features, mu_flayer_sdf)
    

    Region rollup count should be based on the WMU Region Responsible ID, NOT the ENV Region.

    """ 

    logging.info(f'\nSummarizing Data by...{df_sum_fld} and joining to the AGO Feature Layer' )
    #df_sampled = df_wh[(df_wh['CWD_SAMPLED_IND'] == 'Yes')]
    #logging.info(f'CWD_SAMPLED_IND = Yes:  {len(df_sampled)}')


    # Filter columns to include
    df_unit_sampled = df_sampled[[df_sum_fld,'CWD_SAMPLED_IND', 'FISCAL_YEAR','GIS_LOAD_VERSION_DATE']]
    #print(df_unit_sampled.dtypes)
    #print(df_unit_sampled.head(20))

   #Group by the spatial unit field and count the total number of samples
    df_unit_tot_count = df_unit_sampled.groupby([df_sum_fld,'GIS_LOAD_VERSION_DATE'])['CWD_SAMPLED_IND'].count().reset_index(name='CWD_SAMPLE_COUNT') 
    # Group by FISCAL_YEAR
    df_unit_fiscal_count = df_unit_sampled.groupby([ df_sum_fld,'FISCAL_YEAR' ])['CWD_SAMPLED_IND'].count().reset_index(name='Sample_Count')
    df_pivot = df_unit_fiscal_count.pivot(index=df_sum_fld, columns='FISCAL_YEAR', values='Sample_Count')
    #Rename fiscal column headers with CWD_SAMPLE_COUNT_F prefix.  If Fiscal Year is blank, leave as is.
    df_pivot.columns = [
        f"CWD_SAMPLE_COUNT_F{str(col).replace('-', '_')}" if str(col) and str(col)[0].isdigit() else col
        for col in df_pivot.columns]

    # Merge both DataFrames
    df_unit_count = df_unit_tot_count.merge(df_pivot, on=df_sum_fld, how='outer')  
   
    #df_unit_count = pd.concat([df_unit_tot_count, df_pivot], ignore_index=True)
    df_unit_count = df_unit_count.sort_values(by=[df_sum_fld])
    df_unit_count = df_unit_count.fillna(0)   #Fill any NaN values with 0

    #Ensure these are integer values
    int_cols = [col for col in df_unit_count.columns if str(col).startswith("CWD_SAMPLE_COUNT")]
    df_unit_count[int_cols] = df_unit_count[int_cols].astype(int)

    #print(f'Number of summary units with sampled data for {df_sum_fld}: {df_unit_count}')

    #print(df_unit_count.dtypes)
    #print(df_unit_count.head(20))


    ## Save the summary to a file in object storage (overwrites an existing file)
    summary_file_name = (df_sum_fld.lower()) +'_rollup_summary.xlsx'
    logging.info(f'\nSaving the Rollup summaries...')
    #bucket_name=s3_bucket_name
    save_xlsx_to_os(s3_client, s3_bucket_name, df_unit_count, 'master_dataset/spatial_rollup/'+summary_file_name)
    

    # find rows in sampling summary that don't have a spatial unit match.  These are probably due to data entry errors.
    df_mismatch_errors = df_unit_count[~df_unit_count[df_sum_fld].isin(summ_zones_sdf[fl_sum_fld])]
    logging.info(f'\nCHECK DATA: The following unit(s) do not exist in the spatial data:  {df_mismatch_errors}')
    mismatch_file_name = 'no_spatial_match_'+ (df_sum_fld.lower()) +'.xlsx'
    save_xlsx_to_os(s3_client, s3_bucket_name, df_mismatch_errors, 'qa/mu_reg_checks/'+mismatch_file_name)

    # find overlapping rows
    logging.info(f"\nChecking for intersecting rows based on {df_sum_fld}/{fl_sum_fld}...this could take awhile...")
    overlap_rows = pd.merge(left = summ_zones_sdf, right = df_unit_count, how='inner', left_on = fl_sum_fld , right_on = df_sum_fld)
    rcount = len(overlap_rows)
    #rcount = overlap_rows.count()
    logging.info(f'Number of Intersecting Rows: {rcount}')
    #print(overlap_rows)

    
    #Update cursor using the summary df directly (vs converting it to a hosted table in AGO)
    #First, set default Feature Layer rollup to 0 and the current date/time
    # Note that if fiscal year is beyond 2026-27, you'll have to add a new field in the feature layer for that fiscal year count!
    
    uCount = 0
    logging.info("\nCalculating default values for Feature Layer...this could take awhile...")
    for feature in summ_zones_features:
        try:
            for col in int_cols:
                if col in feature.attributes:
                    feature.attributes[col] = 0
                else:
                    logging.info(f"Warning: {col} not found in feature attributes!!")

            feature.attributes['GIS_LOAD_VERSION_DATE'] = timenow_rounded
            summ_zones_lyr.edit_features(updates=[feature])
            uCount = uCount + 1
        except Exception as e:
            logging.info(f"Could not update Feature Layer with default values. Exception: {e}")
            logging.info(f'\nExiting...\n')
            sys.exit()
            #continue

    
    # Get the list of existing fields in the layer
    existing_fields = [f['name'] for f in summ_zones_lyr.properties.fields]

    # Loop through your target columns and update only if target column exists
    # Note that if fiscal year is beyond 2026-27, you'll have to add a new field in the feature layer for that fiscal year count!
    '''#!!  calc_expression not working for some reason !! - use edit_features instead (above)
    logging.info("\nCalculating default values for Feature Layer...")
    for col in int_cols:
        if col in existing_fields:
            try:
                summ_zones_lyr.calculate(
                    where="1=1",  # Applies to all features
                    calc_expression={"field": col, "value": 0}
                )
            except Exception as e:
                logging.info(f"Could not calculate default values for {col}. Exception: {e}")
                continue
        else:
                logging.info(f"Warning: {col} not found in layer fields!!")
    
    # Update the date field if it exists
    print(current_datetime)
    if 'GIS_LOAD_VERSION_DATE' in existing_fields:
        logging.info(f"Updating !")
        summ_zones_lyr.calculate(
            where="1=1",
            calc_expression={"field": "GIS_LOAD_VERSION_DATE", "value": current_datetime}
        )
    else:
        logging.info("Warning: GIS_LOAD_VERSION_DATE not found in layer fields!!")
    
    '''                  
    logging.info(f"DONE Calculating default values for all records!")

    logging.info(f'\nUpdating the AGO Feature Layer with the summary data...')
    #Then update the Feature Layer with the summary data for matching records
    uCount = 0   #reset counter
    for UNIT_ID in overlap_rows[fl_sum_fld]:
        #logging.info(f"Updating {UNIT_ID}...")
        try:
            summ_feature = [f for f in summ_zones_features if f.attributes[fl_sum_fld] == UNIT_ID][0]  #get the spatial feature
            summary_row = df_unit_count.loc[df_unit_count[df_sum_fld] == UNIT_ID]#,['Sample_Count']] #get the dataframe summary row

            for col in int_cols:
                if col in existing_fields:
                    summary_val = summary_row[col].values[0]  #get the sample count value
                    #logging.info(f"Summary value for {col} in {UNIT_ID}: {summary_val}")
                    summ_feature.attributes[col] = str(summary_val)
            #summ_feature.attributes['GIS_LOAD_VERSION_DATE'] = timenow_rounded #current_datetime_str
            summ_zones_lyr.edit_features(updates=[summ_feature])
            #logging.info(f"Updated {summ_feature.attributes[fl_sum_fld]} sample count to {summary_val} at {timenow_rounded}")
            uCount = uCount + 1
        except Exception as e:
            logging.info(f"Could not update {UNIT_ID}. Exception: {e}")
            continue

    #print("\nDONE updating ",uCount, " records!")
    logging.info(f'DONE updating the AGO Feature Layer with the summary data for:  {uCount} records.')

def backup_master_dataset(s3_client, bucket_name):
    """
    Creates a backup of the Master dataset
    
    """
    pacific_timezone = pytz.timezone('America/Vancouver')
    yesterday = datetime.now(pacific_timezone) - timedelta(days=1)
    dytm = yesterday.strftime("%Y%m%d")
    source_file_path = 'master_dataset/cwd_master_dataset_sampling_w_survey_results.xlsx'
    destination_file_path = f'master_dataset/backups/{dytm}_cwd_master_dataset_sampling_w_survey_results.xlsx'
    
    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_file_path},
            Key=destination_file_path
        )
        logging.info("..old master dataset backed-up successfully")
    except Exception as e:
        logging.info(f"..an error occurred: {e}")

def save_public_db_file(df_wh, s3_client, s3_bucket_name):
    """
    Saves an xls containing columns for the public for PowerBI and/or AGO

    # TODO:  Confirm if the records need to be filtered?  e.g. 'CWD_SAMPLED_IND' = 'Yes' or other criteria?
    """

    # Filter columns to include in the subset, based on the For_Public_Dashboarding column in the Data Dictionary
    df_datadict_keep_for_public = df_datadict[df_datadict['For_Public_Dashboarding'] == 'Yes']
    keep_public_fields = df_datadict_keep_for_public['GIS_FIELD_NAME'].tolist()
    

    df_wh_public = df_wh[[col for col in keep_public_fields if col in df_wh.columns]]

    # Exclude UPDATED_WMU_REG_RESPONSIBLE is 'Out of Province'  (added 9-Dec-2025)
    df_wh_public = df_wh_public[df_wh_public['UPDATED_WMU_REG_RESPONSIBLE'] != 'Out of Province']

    # Save/overwrite a copy to a static file name for PowerBI access
    save_xlsx_to_os(s3_client, s3_bucket_name, df_wh_public, 'share_public/cwd_master_public_fields_only.xlsx')

    #return df_wh_public

def save_web_results (df_wh, s3_client, s3_bucket_name, folder, current_datetime_str):
    """
    Saves an xls containing information for the CWD webpage to publish test results,
    and publish to existing AGO hosted table.  

    Exclude: Non-negative and Positive Statuses

    Aug-2025:   Keep DROPOFF_LOCATION and add CWD_NOT_SAMPLED_REASON

    """

    # delete existing XLS files in the share_Web folder
    try:
        # collect XLS file keys to delete
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=folder)
        if 'Contents' in response:
            xls_keys = [{'Key': obj['Key']} for obj in response['Contents'] if obj['Key'].endswith('.xlsx')]
            #delete the files if there are any to remove
            if xls_keys:
                logging.info(f".. Existing files to be deleted: {xls_keys}")
                s3_client.delete_objects(Bucket=s3_bucket_name, Delete={'Objects': xls_keys})
                logging.info(f"..deleted existing files in folder: {folder}")
            else:
                logging.info("..no existing files found in the folder.")
        else:
            logging.info("..no files found in the folder.")
    except Exception as e:
        logging.error(f"..an error occurred while deleting existing files: {e}")


    #filter rows to include in the webpage  (Exclude Positive samples)
    # Exclude CWD_EAR_CARD_ID if ID is Not recorded (added Sept 2025)
    # Exclude UPDATED_WMU_REG_RESPONSIBLE is 'Out of Province'  (added 9-Dec-2025)
    incld_sts= ['Pending', 'Negative', 'Unsuitable Tissue', 'Not Tested','pending', 'negative', 'Unsuitable tissue', 'Not tested','unsuitable tissue', 'not tested']
    df_wb = df_wh[
        #(df_wh['CWD_SAMPLED_IND'] == 'Yes') & #Discussed with Shari to include all sampling results, even if not tested.
        (pd.to_datetime(df_wh['SAMPLED_DATE'], errors='coerce') >= pd.Timestamp('2025-09-01')) &
        (df_wh['UPDATED_WMU_REG_RESPONSIBLE'] != 'Out of Province') &
        (~df_wh['CWD_EAR_CARD_ID'].isin(['Not recorded', 'Not Recorded','']))  & # ~ means Exclude
        (df_wh['CWD_TEST_STATUS'].isin(incld_sts))
    ]

    logging.info(f"{len(df_wb.index)}... records found for Public Sampling Results")

    # Filter columns to include in the webpage, based on the For_Results_Query column in the Data Dictionary
    df_datadict_filtered = df_datadict[df_datadict['For_Results_Query'] == 'Yes']
    keep_fields = df_datadict_filtered['GIS_FIELD_NAME'].tolist()
    #print("Columns to keep:", keep_fields)
    
    df_wb = df_wb[keep_fields]

    '''df_wb= df_wb[['CWD_EAR_CARD_ID',
                  'DROPOFF_LOCATION',
                  'SPECIES',
                  'SEX',
                  'UPDATED_WMU',  #WMU
                  'MORTALITY_DATE',
                  'SAMPLED_DATE',
                  'CWD_TEST_STATUS',
                  'CWD_NOT_SAMPLED_REASON',
                  'GIS_LOAD_VERSION_DATE']]'''

    # Convert the 'DATE' columns to only show the date part as long  #e.g. September 1, 2024
    # No longer needed.
    #for col in ['MORTALITY_DATE','SAMPLED_DATE']:  #df_wb.columns:  #do not use for GIS_LOAD_VERSION_DATE
    #    df_wb[col] = pd.to_datetime(df_wb[col]).dt.strftime('%B %d, %Y')  #(fyi - this converts the column to a string type vs date!)

    #fill blank values with 'Not Recorded'
    #df_wb = df_wb.fillna('Not recorded')
    #df_wb = df_wb.replace('', 'Not recorded')

    #Sort by CWD_EAR_CARD_ID
    df_wb = df_wb.sort_values(by=['CWD_EAR_CARD_ID'])

    # Rename the columns
    # No longer needed, if data is going to AGO instead of to web table.  May use if going to PowerBI
    '''df_wb = df_wb.rename(columns={
        'CWD_EAR_CARD_ID': 'CWD Ear Card',
        'DROPOFF_LOCATION': 'Drop-off Location',
        'SPECIES': 'Species',
        'SEX': 'Sex',
        'UPDATED_WMU': 'Management Unit',
        'MORTALITY_DATE': 'Mortality Date',
        'SAMPLED_DATE': 'Sample Date',
        'CWD_TEST_STATUS': 'CWD Status',
        'CWD_NOT_SAMPLED_REASON': 'Reason for Not Sampling',
        'GIS_LOAD_VERSION_DATE': 'GIS Load Version Date'
    })'''

    
    #File is named with the GIS_LOAD_VERSION_DATE (aka current_datetime string)
    file_key = f"{folder}cwd_sampling_results_for_public_web_{current_datetime_str}.xlsx"

    #convert to xls
    try:
        save_xlsx_to_os(s3_client, s3_bucket_name, df_wb, file_key)
        logging.info(f'..public xlsx successfully saved to bucket {s3_bucket_name}')
    except Exception as e:
        logging.info(f"..an error occurred: {e}")

    # Also save/overwrite a copy to a static file name for PowerBI access
    save_xlsx_to_os(s3_client, s3_bucket_name, df_wb, 'share_public/cwd_sampling_results_for_public_web.xlsx')

    # -----------------------------------------------
    # Load records to AGO - CWD_Public_Sampling_Results  hosted table (non-spatial)
    web_table_item_id = '358b088b619c4283958ed4506f91f069'
    web_item = gis.content.get(web_table_item_id)
    print(f"\nEXISTING SERVICE:  Title: {web_item.title}, ID: {web_item.id}, Type: {web_item.type}")

    feature_table = web_item.tables[0]
    ago_field_names = [field['name'] for field in feature_table.properties.fields]
    #logging.info(f"\tExisting fields in hosted {web_item.type}: {ago_field_names}")

    field_definitions = {key: fprop_dict[key] for key in keep_fields if key in fprop_dict}

    # Add the data dictionary field schema to the feature service, if fields do not already exist
    # This is used in the INITIAL hosted table creation, after the hosted table is created manually.
    # Manual hosted table creation ensures that the OBJECT ID field is created and managed properly.
    # In this case, the hosted table has already been created.
    if 'CWD_TEST_STATUS' not in ago_field_names:
        feature_table.manager.add_to_definition({"fields": field_definitions})
        logging.info(f"Fields added successfully.")
        
        # Check updated fields
        updated_fields = [field['name'] for field in feature_table.properties.fields]
        print(updated_fields)

        print("\n-----------------")
    else:
        logging.info(f"AGO Fields already exist.  Ready to load data :)\n")

    # Truncate (delete) records and overwrite with new records
    if df_wb.empty:
        logging.info("..No Records found for Public Sampling Results to load to AGO!")
    else:
        truncate_and_load_to_ago(web_table_item_id, df_wb, field_definitions, 'table')



def save_lab_submission (df_wh, s3_client, s3_bucket_name, folder):
    """
    Saves multiple xlss, each containing information for a specific lab submission.
    """

    # delete existing XLS files in the to_Lab folder
    try:
        # collect XLS file keys to delete
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=folder)
        if 'Contents' in response:
            xls_keys = [{'Key': obj['Key']} for obj in response['Contents'] if obj['Key'].endswith('.xlsx')]
            #delete the files if there are any to remove
            if xls_keys:
                logging.info(f".. Existing files to be deleted: {xls_keys}")
                s3_client.delete_objects(Bucket=s3_bucket_name, Delete={'Objects': xls_keys})
                logging.info(f"..deleted existing files in folder: {folder}")
            else:
                logging.info("..no existing files found in the folder.")
        else:
            logging.info("..no files found in the folder.")
    except Exception as e:
        logging.error(f"..an error occurred while deleting existing files: {e}")

    logging.info(f'..Filtering rows...')
    # Filter rows to include in the lab submission
    df_lb = df_wh[(df_wh['CWD_SAMPLED_IND'] == 'Yes') &
                  (df_wh['CWD_TEST_STATUS'] == 'Pending') &
                  (df_wh['REPORTING_LAB_DATE_RECEIVED'].isnull()) &
                  (df_wh['REPORTING_LAB_ID'].isnull())
    ]

    # IF statment to notify if there are no returned records.
    if df_lb.empty:
        logging.info("..No Records found for Pending Lab Submissions!")
    else:
        logging.info(f"{len(df_lb.index)}... records found for Pending Lab Submissions.  Processing...")

        # Filter columns to include in the CSV
        df_lb = df_lb[['CWD_SAMPLED_IND',
                    'CWD_LAB_SUBMISSION_ID',
                    'WLH_ID',
                    'CWD_EAR_CARD_ID',
                    'SPECIES',
                    'SAMPLED_DATE',
                    'SAMPLE_CONDITION',
                    'SAMPLE_CWD_TONSIL_NUM',
                    'SAMPLE_CWD_RPLN_NUM',
                    'SAMPLE_CWD_OBEX_IND',
                    'SAMPLE_DATE_SENT_TO_LAB',
                    'REPORTING_LAB',
                    'REPORTING_LAB_DATE_RECEIVED',
                    'REPORTING_LAB_ID',
                    'REPORTING_LAB_COMMENT',
                    'CWD_TEST_STATUS',
                    'CWD_TEST_STATUS_DATE',
                    'GIS_LOAD_VERSION_DATE']]


        #convert the 'DATE' columns to only show the date part excluding time
        for col in ['SAMPLED_DATE','SAMPLE_DATE_SENT_TO_LAB']:  
            df_lb[col] = pd.to_datetime(df_lb[col]).dt.date   #e.g.2024-09-08

        #iterate over each unique CWD_LAB_SUBMISSION_ID and save a separate file for each
        for submission_id, group_df in df_lb.groupby('CWD_LAB_SUBMISSION_ID'):
            submission_id_lowr = submission_id.lower()
            file_key = f"{folder}cwd_sampling_lab_submission_{submission_id_lowr}.xlsx"

            #logging.info(f'..Trying to save {submission_id} to file {file_key}')

            #Sort by WLH_ID
            group_df = group_df.sort_values(by=['WLH_ID'])

            #Upload the XLSX to S3
            try:
                save_xlsx_to_os(s3_client, s3_bucket_name, group_df, file_key)
                logging.info(f'..XLSX for submission {submission_id} successfully saved to {file_key}')
            except Exception as e:
                logging.error(f"..an error occurred while saving {file_key}: {e}")

def save_tongue_sample_data(df_wh,df_tng_new):
    """
    df_tng_new         from tongue sheets in XLSXs in object storage - merged for all species
    df_tng_sampled     Select out SAMPLE_TONGUE_IND = 'Yes' from master sampling data
    df_tng_bck         temp df to hold fields from the original tongue tracking data, if needed
    
    
    """
    # Join the latest merged tongue file with selected fields from the master sampling data. 
    # (Optional) Read the existing static merged tongue tracking XLS file into a DataFrame to temporarily hold the data.
    # if for some reason the xls file is not found in OS (sometimes from syncing issues), then skip the overwrite of the merged tongue file.
    static_xls_path = 'master_dataset/tongue_samples/cwd_tongue_samples_merged.xlsx'
    logging.info(f"...Checking for file: {static_xls_path}")

    #print(df_tng_new.dtypes)
    #print(df_tng_new['Date Samples Shipped'].dtype)
    #print(df_tng_new[['Genomics ID','Date Samples Shipped']])
    #print(df_tng_new[['Genomics ID','Date Samples Shipped']].head(20))
    #print(df_tng_new[['Genomics ID','Date Samples Shipped']].tail(20))


    if s3_file_exists_check(s3_client, s3_bucket_name, static_xls_path):
        obj = s3_client.get_object(Bucket=s3_bucket_name, Key=static_xls_path)
        data = obj['Body'].read()
        excel_file = pd.ExcelFile(BytesIO(data))

        logging.info(f"...reading file: {static_xls_path}")
        static_df = pd.read_excel(excel_file, sheet_name='Sheet1')

        # Temp dataframe to hold original file, if needed.  e.g. to backup existing master tongue tracking file.
        # df_tng_bck = static_df[~static_df['LOCATION_OF_SAMPLE'].isna()]
        df_tng_bck = static_df
    else:
        logging.info(f"\n***WARNING...original xls file not currently found: {static_xls_path}\n...Skipping the backup of the tongue sampling xls tracking list.\n")

    
    try:
        # Get new merged tongue xls sheet and convert incoming tongue tracking column names to uppercase and replace spaces with underscores
        columns = df_tng_new.columns
        df_tng_new.columns = [col.upper().replace(' ', '_') for col in columns]

        # Create Temporary MERGE_ID.  One of WLH_ID or CWD_EAR_CARD_ID must be present.
        df_tng_new['MERGE_ID_TEMP'] = df_tng_new['WLH_ID'].fillna(df_tng_new['CWD_EAR_CARD_ID'])
        
        logging.info(f"\n..{len(df_tng_new)}...records found in the submitted tongue tracking data")
        
        # Filter for records with SAMPLE_TONGUE_IND = 'Y' from the master dataframe
        df_tng_sampled = df_wh[df_wh['SAMPLE_TONGUE_IND'] == 'Yes']
        df_tng_sampled['MERGE_ID_TEMP'] = df_tng_sampled['WLH_ID'].fillna(df_tng_sampled['CWD_EAR_CARD_ID'])
        logging.info(f"..{len(df_tng_sampled)}... records found with SAMPLE_TONGUE_IND = 'Yes' in the master dataset")
        
        # Tongue tracking column list
        #tng_column_list = ['Genomics ID','MU_NUMBER','PreFrozen', 'Still Frozen', 'Shipment Dec 2024', 'Box Number', 'Box Location Number', 'Date Samples Shipped']
        #tng_column_list = [col.upper().replace(' ', '_') for col in tng_column_list]
        tng_column_list = ['GENOMICS_ID', 'MU_NUMBER','PREFROZEN', 'STILL_FROZEN', 'SHIPMENT_DEC_2024', 'BOX_NUMBER', 'BOX_LOCATION_NUMBER', 'DATE_SAMPLES_SHIPPED']

        # Join the tongue tracking fields to df_tng_sampled on WH_ID
        # TODO: Revise this to use the MERGE_ID_TEMP
        # Confirm what the primary key column will be for tongue sampling sheet: 
        #df_tng_sampled = df_tng_sampled.merge(df_tng_new[tng_column_list + ['WLH_ID']], how='left', on='WLH_ID')
        df_tng_sampled = df_tng_sampled.merge(df_tng_new[tng_column_list + ['MERGE_ID_TEMP']], how='left', on='MERGE_ID_TEMP')
        logging.info(f"{len(df_tng_sampled)}... total merged tongue sample records")

        # Strip down/re-order final fields
        # TODO: Confirm if we want to use the UPDATED WMU /REGION instead or in addition?
        fldList = ['GENOMICS_ID','CWD_LAB_SUBMISSION_ID','SAMPLING_SESSION_ID','WLH_ID','CWD_EAR_CARD_ID','DROPOFF_LOCATION','COLLECTION_DATE','SPECIES','SEX','AGE_CLASS','AGE_ESTIMATE','MORTALITY_CAUSE', 'MORTALITY_DATE', 'SAMPLED_DATE', 'SAMPLE_CONDITION', 'SAMPLE_TONGUE_IND','SAMPLE_EAR_TIP_IND','CWD_TEST_STATUS','WMU_REGION_RESPONSIBLE', 'WMU', 'MU_NUMBER','UPDATED_LATITUDE', 'UPDATED_LONGITUDE', 'UPDATED_SPATIAL_CAPTURE_DESCRIPTOR', 'UPDATED_WMU_REG_RESPONSIBLE','UPDATED_WMU','GIS_LOAD_VERSION_DATE','PREFROZEN', 'STILL_FROZEN', 'SHIPMENT_DEC_2024', 'BOX_NUMBER', 'BOX_LOCATION_NUMBER', 'DATE_SAMPLES_SHIPPED']
        df_tng_sampled = df_tng_sampled[fldList]

        # Sort the dataframe
        df_tng_sampled = df_tng_sampled.sort_values(by=['GENOMICS_ID','CWD_LAB_SUBMISSION_ID','WLH_ID'])

        # Strip 0:0 time out of time fields
        # Note: 'DATE_SAMPLES_SHIPPED' may contain text values!  e.g. Not shipped
        for col in ['MORTALITY_DATE','SAMPLED_DATE']:  #,'DATE_SAMPLES_SHIPPED']:  
            df_tng_sampled[col] = pd.to_datetime(df_tng_sampled[col]).dt.date   #e.g.2024-09-08

        # Remove ' 00:00:00' from all strings in the column
        df_tng_sampled['DATE_SAMPLES_SHIPPED'] = df_tng_sampled['DATE_SAMPLES_SHIPPED'].astype(str).str.replace(' 00:00:00', '', regex=False)
        
        # Fill NaN values with empty strings
        df_tng_sampled['DATE_SAMPLES_SHIPPED'] = df_tng_sampled['DATE_SAMPLES_SHIPPED'].fillna('') #Not working?

        # SKIP.  DEPRECATED .if there are existing records with LOCATION_OF_SAMPLE populated, then re-join these to the new tongue sample dataframe.
        # Join the LOCATION_OF_SAMPLE field back to df_tng_sampled on WLH_ID
        '''
        if len(df_tng_bck) > 0:
            df_tng_sampled = df_tng_sampled.merge(df_tng_bck[['WLH_ID', 'LOCATION_OF_SAMPLE']], how='left', on='WLH_ID')
            logging.info(f"..{len(df_tng_bck)}... original LOCATION_OF_SAMPLE records added back to latest tongue sample records")
        elif len(df_tng_bck) == 0:
            # Add blank field
            df_tng_sampled[['LOCATION_OF_SAMPLE']] = ''
            logging.info(f"...no existing LOCATION_OF_SAMPLE values to add to cwd_tongue_sample_tracking ")
        '''
        

        # Export to XLS
        logging.info(f"..saving the Tongue Tracking File to xls")
        #save_xlsx_to_os(s3_client, s3_bucket_name, df_tng_sampled, f'master_dataset/tongue_samples/cwd_tongue_sample_tracking_{current_datetime_str}.xlsx')
        save_xlsx_to_os(s3_client, s3_bucket_name, df_tng_sampled, f'master_dataset/tongue_samples/cwd_tongue_samples_merged.xlsx')

    except Exception as e:
        logging.info(f"\n***WARNING...an error occurred in the tongue sampling function: {e}\n...Skipping the update of the tongue sampling xls tracking list.\n")

    return


def publish_feature_layer(gis, df, latcol, longcol, title, folder):
    """
    Publishes the master dataset to AGO, overwriting data if it already exists.

    CHECK:  why are numeric types in data dictionary being replaced with string types? Is
    this related to filling with length 25 by default?  Or does a blank feature template have to 
    be created first if string types cannot be converted to numeric types?

    CHECK:  why is feature layer always getting newly created instead of updating the existing one, according to the message?
    """
    # Cleanup the master dataset df before publishing
    df = df.dropna(subset=[latcol, longcol])
    df = df.astype(str)

    # Drop personal info fields from the dataset
    # TODO: Confirm if any other fields should be removed.  This could be controlled from the datadictionary 'Skip_for_AGO' field value (i.e. use a filter)
     # Filter columns to include in the ago dataset, based on the Skip_for_AGO column in the Data Dictionary ( Skip_for_AGO = Yes  means exclude the field)
    df_datadict_skip_for_ago = df_datadict[df_datadict['Skip_for_AGO'] == 'Yes']
    drop_cols = df_datadict_skip_for_ago['GIS_FIELD_NAME'].tolist()
    #drop_cols = ['SUBMITTER_FIRST_NAME', 'SUBMITTER_LAST_NAME', 'SUBMITTER_PHONE', 'FWID','STATUS_ID']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])


    # Define Pacific timezone
    pacific_timezone = pytz.timezone('America/Vancouver')

    # Convert DATE (with date and time) fields to datetime, ensure they are timezone-aware
    for col in df.columns:
        #if 'DATE' in col:
        if 'GIS_LOAD_VERSION_DATE' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(pacific_timezone, 
                                                                              ambiguous='NaT', 
                                                                              nonexistent='shift_forward')

    '''# DO NOT USE - this converts to string types, which are not compatible with AGO (and PowerBI?)
    date_columns = df.columns[df.columns.str.contains('DATE')]
    date_columns_convert = ['COLLECTION_DATE','MORTALITY_DATE','SAMPLED_DATE','SAMPLE_DATE_SENT_TO_LAB','REPORTING_LAB_DATE_RECEIVED','CWD_TEST_STATUS_DATE','PREP_LAB_LAB_DATE_RECEIVED','PREP_LAB_DATE_FORWARDED']
    
    for col in date_columns_convert:
        df[col] = pd.to_datetime(df[col]).dt.date   #e.g.2024-09-08                                                                       
    '''

    #Convert numeric column types and fill empty numeric values with zero
    #Note:  These are the fields with numeric domain values (see domains_dict)
    float_cols = ['SAMPLE_CWD_TONSIL_NUM', 'SAMPLE_CWD_RPLN_NUM', 'SAMPLE_PLN_NUM', 'SAMPLE_MLN_NUM']
    int_cols = ['SAMPLE_COVID_SWAB_NUM','SAMPLE_NOBUTO_NUM']
    zero_cols = float_cols + int_cols
    
    #df[zero_cols] = df[zero_cols].fillna(0).astype(float)
    df[zero_cols] = df[zero_cols].astype(float)
    df[zero_cols] = df[zero_cols].fillna(0)  #must fill with 0 after converting to float

    #print(df[zero_cols].dtypes)
    #print(df[zero_cols])

    #df[float_cols] = df[float_cols].astype(float)
    #df[int_cols] = df[int_cols].astype(int)  #won't work if there are decimal values in df

    # Fill NaN and NaT values in text and date strings
    df = df.fillna('')

    # Create a spatial dataframe
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[longcol], df[latcol]), crs="EPSG:4326")

    # Convert Timestamp columns to string format with timezone info
    for col in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[col]):
            gdf[col] = gdf[col].apply(lambda x: x.isoformat() if not pd.isna(x) else '')
    
    gdf = gdf.replace(['nan', '<NA>'], '')

    def gdf_to_geojson(gdf):
        features = []
        for _, row in gdf.iterrows():
            feature = {
                "type": "Feature",
                "properties": {},
                "geometry": row['geometry'].__geo_interface__
            }
            for column, value in row.items():
                if column != 'geometry':
                    if isinstance(value, (datetime, pd.Timestamp)):
                        feature['properties'][column] = value.isoformat() if not pd.isna(value) else ''
                    else:
                        feature['properties'][column] = value
            features.append(feature)
        
        geojson_dict = {
            "type": "FeatureCollection",
            "features": features
        }
        return geojson_dict

    # Convert GeoDataFrame to GeoJSON
    geojson_dict = gdf_to_geojson(gdf)
    geojson = json.dumps(geojson_dict)

    try:
        # Search for existing items (including the GeoJSON file and feature layer)
        existing_items = gis.content.search(f"(title:{title} OR title:data.geojson) AND owner:{gis.users.me.username}")

        # Delete the existing GeoJSON file
        for item in existing_items:
            if item.type == 'GeoJson':
                item.delete(force=True, permanent=True)
                logging.info(f"..existing GeoJSON item '{item.title}' permanently deleted.")

        # Find the existing feature layer
        feature_layer_item = None
        for item in existing_items:
            if item.type == 'Feature Layer':
                feature_layer_item = item
                break   # Exit loop after finding the first feature layer

        print(f"\nFound {len(existing_items)} existing items with title '{title}'")
        print(feature_layer_item)

        sys.exit()

        # Create a new GeoJSON item
        geojson_item_properties = {
            'title': title,
            'type': 'GeoJson',
            'tags': 'sampling points,geojson',
            'description': 'CWD master dataset containing lab sampling and hunter information',
            'fileName': 'data.geojson'
        }
        geojson_file = BytesIO(json.dumps(geojson_dict).encode('utf-8'))
        new_geojson_item = gis.content.add(item_properties=geojson_item_properties, data=geojson_file, folder=folder)

        # Update the existing feature layer or create a new one if it doesn't exist
        #  CHECK!   This seems to always use the second method using overwrite, instead of using .update() method.
        if feature_layer_item: #and feature_layer_item.type == 'Feature Layer':
            logging.info(f"..found existing feature layer '{feature_layer_item.title}'")
        if feature_layer_item and feature_layer_item.type == 'Feature Layer':
            logging.info(f"..found existing feature layer '{feature_layer_item.title}'")

        if feature_layer_item:
            feature_layer_item.update(data=new_geojson_item, folder=folder)
            logging.info(f"..existing feature layer '{title}' updated successfully.")
        else:
            published_item = new_geojson_item.publish(overwrite=True)
            logging.info(f"..new feature layer '{title}' published successfully.")
            return published_item

    except Exception as e:
        error_message = f"..error publishing/updating feature layer: {str(e)}"
        raise RuntimeError(error_message)



def retrieve_field_properties (s3_client, s3_bucket_name):
    """
    Constructs dictionaries containing field properties.
    """
    prefix= 'incoming_from_idir/data_dictionary/'

    objects = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix)['Contents']

    # Filter to only include the latest Data dictionnary file
    xlsx_files = [obj for obj in objects if obj['Key'].endswith('.xlsx')]
    latest_file = max(xlsx_files, key=lambda x: x['LastModified'])

    latest_file_key = latest_file['Key']
    obj = s3_client.get_object(Bucket=s3_bucket_name, Key=latest_file_key)
    data = obj['Body'].read()
    excel_file = pd.ExcelFile(BytesIO(data))

    df_datadict = pd.read_excel(excel_file, sheet_name='Data Dictionary')
    df_pcklists = pd.read_excel(excel_file, sheet_name='Picklists')

    #Creating domains dictionary
    df_domfields = df_datadict[['Domain Name','GIS_FIELD_NAME']].dropna(subset=['Domain Name'])
    field_match_dict = df_domfields.set_index('Domain Name')['GIS_FIELD_NAME'].to_dict()

    df_pcklists = df_pcklists.rename(columns=field_match_dict)

    domains_dict = {} 
    # Iterate through each column (field) in the DataFrame
    for column in df_pcklists.columns:
        # Extract the field name and values
        field_name = column
        values = df_pcklists[field_name].dropna().tolist()  # Drop NaN values and convert to list

        #TEMP - check syntax - this doesn't seem to do anything.
        """# Ensure coded values are integers for esriFieldTypeSmallInteger fields
        if field_name in field_match_dict and df_datadict.loc[df_datadict['GIS_FIELD_NAME'] == field_name, 'Type'].values[0] == 'SHORT':
            print(f"Converting {field_name} values to integers")
            values = [int(value) for value in values]"""

        # Create the domain dictionary for the field
        domain_values = {str(value): str(value) for value in values}
        domains_dict[field_name] = domain_values

    #print(f"\nDomains Dictionary: {domains_dict}")
    #TEMP - empty the dictionary for testing
    #domains_dict = {}

    #Creating field length and type dictionnary
    df_fprop= df_datadict[['GIS_FIELD_NAME', 'Type', 'Length', 'Alias']]
    df_fprop = df_fprop.replace(["n/a", "N/A", ""], None)
    df_fprop['Length'] = df_fprop['Length'].fillna(25)
    df_fprop['Length'] = df_fprop['Length'].astype(int)

    # Mapping from custom types to ArcGIS field types
    type_mapping = {
        'TEXT': 'esriFieldTypeString',
        'DATEONLY': 'esriFieldTypeDateOnly',  # Date Only
        'DATE': 'esriFieldTypeDate',      # Date and Time
        'LONG': 'esriFieldTypeInteger',
        'SHORT': 'esriFieldTypeSmallInteger',  #does not work if coded domain values are not integers e.g. 1 vs 1.0.
        'FLOAT': 'esriFieldTypeSingle',
        'DOUBLE': 'esriFieldTypeDouble'
    }
    df_fprop['Type'] = df_fprop['Type'].map(type_mapping)

    # T. Transpose the dataframe
    fprop_dict = df_fprop.set_index('GIS_FIELD_NAME').T.to_dict()

    #print(f"\nDomains Dictionary: {domains_dict}")
    #print(f"\nField Properties Dictionary: {fprop_dict}")

    return df_datadict, domains_dict, fprop_dict


def apply_field_properties(gis, title, domains_dict, fprop_dict):
    """Applies Field properities to the published Feature Layer

        NOTE:  
        28-Feb-2025     This function is not working as expected.  The field properties are not being updated, e.g. to change the field type from string to integer.
                        A recent AGO update also messed with esriFieldTypeSmallInteger fields and domain values and issues with decimal vs integer values?

    """
    # Retrieve the published feature layer
    feature_layer_item = gis.content.search(query=title, item_type="Feature Layer")[0]
    feature_layer = feature_layer_item.layers[0]

    # Apply Domains
    for field, domain_values in domains_dict.items():
        domain = {
            "type": "codedValue",
            "name": f"{field}_domain",
            "codedValues": [{"name": k, "code": v} for k, v in domain_values.items()]
        }
        field_info = {
            "name": field,
            "domain": domain
        }
        feature_layer.manager.update_definition({"fields": [field_info]})

    # Apply Field Lengths and Types
    fields = feature_layer.properties['fields']
    # Update the fields based on the dictionary
    for field in fields:
        field_name = field['name']
        if field_name in fprop_dict:
            field['length'] = fprop_dict[field_name]['Length']
            field['type'] = fprop_dict[field_name]['Type']
            field['alias'] = fprop_dict[field_name]['Alias']

    # Update the field definitions
    response = feature_layer.manager.update_definition({
        "fields": fields
    })
    
    # Check and print the response
    if 'success' in response and response['success']:
        logging.info("..field properties updated successfully!")
    else:
        logging.info("..failed to update field properties. Response:", response)

def truncate_and_load_to_ago(item_id, df,field_definitions, create_type):
    """
    For hosted TABLE
    Truncate (delete) all existing records and replace them

    item_id of the target table
    df for the records to load
    field_defintions dictionary (may be filtered for the particular ago service e.g. for sampling results)
    create_type = 'table'  or 'feature_layer'.  Only 'table' is implemented currently.
    """
             
    #Truncate Table
    hosted_table_item = gis.content.get(item_id)
    table_layer = hosted_table_item.tables[0]
    hosted_table_manager = table_layer.manager

    logging.info(f"\nTruncating and Loading new records to existing hosted table: {hosted_table_item.title}")

    try:
        hosted_table_manager.truncate()
        logging.info(f"Data truncated")
    except:
        logging.warning("Truncate failed")



    # -----------------------Load Table records (non-spatial)
    # Preprocess the DataFrame for esriFieldTypeDateOnly fields
    # NOTE:  MAY NEED TO FILL any null or Not Recorded dates as blanks or an obscure date, then fill with None later.
    # It seems AGO accepts None for empty date fields, but the dataframe type can be Object, vs a date type.
    
    #print(field_definitions)
    #dateonly_fields = [field['name'] for field in field_definitions if field['type'] == 'esriFieldTypeDateOnly']
    dateonly_fields = [name for name, props in field_definitions.items() if props.get('Type') == 'esriFieldTypeDateOnly']

    for field in dateonly_fields:
        if field in df.columns:
            #print(f"Processing 'esriFieldTypeDateOnly' field: {field} and values: {df[field].values}  ")
            df[field] = pd.to_datetime(df[field], errors='coerce').dt.date  # Convert to date only - should be already done
            df[field] = df[field].replace('Not Recorded', pd.Timestamp('1900-01-01'))
            df[field] = df[field].where(df[field].notna(), pd.Timestamp('1900-01-01'))

            # Replace default date with None
            #df[field] = df[field].apply(lambda x: None if str(x) == '1900-01-01 00:00:00' else x)

            #logging.info(f"Processed 'esriFieldTypeDateOnly' field: {field}")
            #print(f"Fixed? 'esriFieldTypeDateOnly' field: {field} and values: {df[field].values}  ")
    
    for col in df.columns:
        #if '_DATE' not in col:
        df[col] = df[col].fillna('').astype(str)
    
    # Convert DATE field to datetime, ensure it is timezone-aware
    for col in df.columns:
        if 'GIS_LOAD_VERSION_DATE' in col:

            print(f"Converting column '{col}' to datetime with timezone")
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(pacific_timezone, 
                                                                            ambiguous='NaT', 
                                                                            nonexistent='shift_forward')
            df[col] = pd.to_datetime(df[col]).dt.tz_convert('UTC')
    

    #for col in df.columns:
        #if '_DATE' not in col:
    df[col] = df[col].fillna('')
    
    # Replace default date with None
    for field in dateonly_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: None if str(x) == '1900-01-01 00:00:00' else x)

    #Replace Not Recorded with None in Integer fields
    #df['CWD_EAR_CARD_ID'] = df['CWD_EAR_CARD_ID'].apply(lambda x: None if str(x).strip() == 'Not Recorded' else x)
    #df['CWD_EAR_CARD_ID'] = df['CWD_EAR_CARD_ID'].apply(lambda x: None if str(x).strip() == 'Not recorded' else x)

    #print("\nDataFrame dtypes after manipulation loading:")
    #print(df.dtypes)
    #print(df[['CWD_EAR_CARD_ID','MORTALITY_DATE']].head(40))
    #print(df[['CWD_EAR_CARD_ID','MORTALITY_DATE']].tail(40))

    if create_type == "table":
        # Load new Table from dataframe
        logging.info(f"\nLoading new records to hosted table from dataframe...")
        features = []
        
        #print(df.dtypes)
        #print(df[['CWD_EAR_CARD_ID','MORTALITY_DATE','SAMPLED_DATE','GIS_LOAD_VERSION_DATE']].head(20))
        #print(df[['CWD_EAR_CARD_ID','MORTALITY_DATE','SAMPLED_DATE','GIS_LOAD_VERSION_DATE']].tail(20))

        # Get valid field names and types from the table schema
        field_info = {field['name']: field['type'] for field in table_layer.properties.fields}
        #print(f"\nFeature Layer Field Info:")
        #for field in field_info:
            #print(f"Field: {field}, Type: {field_info[field]}")

        #logging.info(f"\nField names in hosted table: {[field['name'] for field in table_layer.properties.fields]}")

        for _, row in df.iterrows():
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

    #Or Load Feature Records (spatial) - NOT YET TESTED
    if create_type == "feature_layer":
        logging.info(f"\nLoading new records to hosted feature layer from dataframe...")
        features = []
        for _, row in df.iterrows():
            attributes = row.to_dict()
            geometry = {"x": row["Longitude"], "y": row["Latitude"]}  # Replace with your geometry fields
            features.append({"attributes": attributes, "geometry": geometry})


    # Add features to the table
    logging.info(f"\nAdding {len(features)} records to the hosted table...\n")
    #logging.info(f"\nFeatures to add (after preprocessing): {features}")

    result = table_layer.edit_features(adds=features)

    #result = table_layer.append(features) #?? Append does not work
    print("\n-----------------")
    #print(result)

    if result['addResults'][0]['success']:
        logging.info("Records added successfully.")
    else:
        # Extract the error message or reason for failure
        error_message = result['addResults'][0].get('error', {}).get('message', 'Unknown error')
        logging.warning(f"Failed to add records. Reason: {error_message}")

    print("\n-----------------")
    '''
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
    '''


    # Final Delete of xls, as it is no longer needed
    #delete_existing_item(gis, item_title, item_type="Microsoft Excel")

    logging.info(f"Done Truncate and Load")

def master_dataframe_edits(df_wh, fprop_dict, domains_dict): 
    """
    Add remaining calculated fields and re-order all dataframe fields according to the Data Dictionary
    """

    # Categorize MORTALITY_CAUSE and SUBMITTER_SOURCE
    # First remove any leading or trailing spaces from the strings
    df_wh['MORTALITY_CAUSE'] = df_wh['MORTALITY_CAUSE'].str.strip()
    df_wh['SUBMITTER_SOURCE'] = df_wh['SUBMITTER_SOURCE'].str.strip()

    # Supply dictionaries 
    # Future - instead of hard-coding these dictionaries, build it from the Data Dictionary picklist columns
    mortality_cause_dict = {
        'Euthanized - Clinical': 'Other',
        'Euthanized - Not Clinical': 'Other',
        'Harvest Kill': 'Harvest',
        'Mortality Investigation': 'Other',
        'Not Recorded': 'Not Recorded',
        'Road Kill': 'Motor Vehicle Collision',
        'Seized': 'Other',
        'Targeted Removal - Contracted': 'Other',
        'Targeted Removal - Special Permitted Hunt': 'Other',
        'Targeted Removal - Urban Animal': 'Other',
        'Train Kill': 'Motor Vehicle Collision'
    }

    submitter_source_dict = {
        'Conservation Officer': 'Conservation Officer',
        'Contractor': 'Other',
        'First Nation': 'Other',
        'Government Biologist': 'Government Biologist',
        'Highway Crew': 'Highway Crew',
        'Hunter': 'Hunter',
        'Municipality': 'Municipality',
        'Public': 'Other',
        'RCMP': 'Other',
        'Trapper': 'Trapper',
        'WSRO': 'Other',
        'Unknown': 'Unknown'
    }

    # Calculate the category based on the dictionary
    df_wh['MORTALITY_CAUSE_CATEGORY'] = df_wh['MORTALITY_CAUSE'].map(mortality_cause_dict)
    df_wh['SUBMITTER_SOURCE_CATEGORY'] = df_wh['SUBMITTER_SOURCE'].map(submitter_source_dict)


    #print(df_wh.dtypes)
    #print(df_wh[['CWD_TEST_STATUS_DATE','SAMPLE_DATE_SENT_TO_LAB']].dtypes)
    #print(df_wh[['CWD_TEST_STATUS_DATE','SAMPLE_DATE_SENT_TO_LAB']])
    #print(df_wh[['CWD_TEST_STATUS_DATE','SAMPLE_DATE_SENT_TO_LAB']].head(20))
    
    # Add Time difference fields - calculate the number of days between key date fields where values are not null
    # TODO: Check - if CWD_TEST_STATUS is 'Negative', but missing dates, mark as complete?
    logging.info("\n..Calculating Time Difference fields...")
    #--
    # Defaults
    df_wh['LAB_TURNAROUND_DAYS'] = 0
    df_wh['LAB_TURNAROUND_STATUS'] = 'N/A'

    # Condition: Both dates are present → 'Complete'    
    mask_complete = (
        df_wh['CWD_TEST_STATUS_DATE'].notna() & (df_wh['CWD_TEST_STATUS_DATE'] != '') &
        df_wh['SAMPLE_DATE_SENT_TO_LAB'].notna() & (df_wh['SAMPLE_DATE_SENT_TO_LAB'] != ''))
    df_filtered1 = df_wh[mask_complete]
    logging.info(f"\t{len(df_filtered1)}... Testing COMPLETE")

    df_wh.loc[mask_complete, 'LAB_TURNAROUND_DAYS'] = (
        pd.to_datetime(df_wh.loc[mask_complete, 'CWD_TEST_STATUS_DATE'], errors='coerce') -
        pd.to_datetime(df_wh.loc[mask_complete, 'SAMPLE_DATE_SENT_TO_LAB'], errors='coerce')
    ).dt.days

    df_wh.loc[mask_complete, 'LAB_TURNAROUND_STATUS'] = 'Complete'

    # Condition: Sample date present, test status date missing → 'In Progress'  (Some of these may be Not Tested - see below)
    #mask_in_progress = df_wh['SAMPLE_DATE_SENT_TO_LAB'].notna() & df_wh['CWD_TEST_STATUS_DATE'].isna()
    
    mask_in_progress = (
        (df_wh['CWD_TEST_STATUS_DATE'].isna() | (df_wh['CWD_TEST_STATUS_DATE'] == '')) &
        df_wh['SAMPLE_DATE_SENT_TO_LAB'].notna() & (df_wh['SAMPLE_DATE_SENT_TO_LAB'] != '')
    )
    
    df_filtered2 = df_wh[mask_in_progress]
    logging.info(f"\t{len(df_filtered2)}... Testing IN PROGRESS")

    df_wh.loc[mask_in_progress, 'LAB_TURNAROUND_DAYS'] = 0
    df_wh.loc[mask_in_progress, 'LAB_TURNAROUND_STATUS'] = 'In Progress'

    # Condition: CWD_TEST_STATUS = 'Not Tested' → 'Not Tested' - this may overwrite some of teh 'In Progress' records.
    mask_not_tested = df_wh['CWD_TEST_STATUS'] == 'Not Tested'
    df_filtered3 = df_wh[mask_not_tested]
    logging.info(f"\t{len(df_filtered3)}... Not Tested")

    df_wh.loc[mask_not_tested, 'LAB_TURNAROUND_DAYS'] = 0
    df_wh.loc[mask_not_tested, 'LAB_TURNAROUND_STATUS'] = 'Not Tested'

    # # Ensure all columns in fprop_dict are present in df_wh
    # for field in fprop_dict.keys():
    #     if field not in df_wh.columns:
    #         df_wh[field] = None  # Add missing field with None values

    # Reorder columns based on fprop_dict keys
    ordered_columns = [field for field in fprop_dict.keys() if field in df_wh.columns]
    df_wh = df_wh[ordered_columns]

    return df_wh

def test_save_spatial_files(df, s3_client, s3_bucket_name):
    """
    TEST Saves spatial files of the master datasets in object storage  e.g. KML, Shapefile.
    Not yet implemented in the main workflow.
    """
    latcol='UPDATED_LATITUDE'
    loncol= 'UPDATED_LONGITUDE'

    df = df.dropna(subset=[latcol, loncol])
    df = df.astype(str)

    pacific_timezone = pytz.timezone('America/Vancouver')
    dytm = datetime.now(pacific_timezone).strftime("%Y%m%d")
    
    # Create a GeoDataFrame from the DataFrame
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[loncol], df[latcol]),
        crs="EPSG:4326"
    )
    
    #save the KML
    try:
        import fiona
        fiona.supported_drivers['KML'] = 'rw'

        # Convert GeoDataFrame to KML
        kml_buffer = BytesIO()
        gdf.to_file(kml_buffer, driver='KML')
        kml_buffer.seek(0)

        # Define the S3 object key
        s3_kml_key = f"spatial/{dytm}_cwd_sampling_points.kml"
        
        # Upload the KML file to S3
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_kml_key, Body=kml_buffer, 
                             ContentType='application/vnd.google-earth.kml+xml')
        
        logging.info(f"..KML file saved to {s3_bucket_name}/{s3_kml_key}")
        
    except Exception as e:
        logging.error(f"..failed to save or upload KML file: {e}")

    #save the shapefile
    try:
        import zipfile
        import tempfile

        shapefile_buffer = BytesIO()
        with zipfile.ZipFile(shapefile_buffer, 'w') as zf:
            with tempfile.TemporaryDirectory() as tmpdir:
                shapefile_path = os.path.join(tmpdir, "{dytm}_cwd_sampling_points.shp")
                gdf.to_file(shapefile_path, driver='ESRI Shapefile')

                # Add all shapefile components to the zip archive
                for filename in os.listdir(tmpdir):
                    file_path = os.path.join(tmpdir, filename)
                    zf.write(file_path, os.path.basename(file_path))

        shapefile_buffer.seek(0)

        # Define the S3 object key for Shapefile
        s3_shapefile_key = f"spatial/{dytm}_cwd_sampling_points.zip"

        # Upload the Shapefile (zipped) to S3
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_shapefile_key, Body=shapefile_buffer, 
                             ContentType='application/zip')

        logging.info(f"..shapefile saved to {s3_bucket_name}/{s3_shapefile_key}")

    except Exception as e:
        logging.error(f"..failed to save or upload the shapefile: {e}")       


#---------------------------------------------------------------------------------------------------
#  MAIN FUNCTION CALLS START
#---------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    start_t = timeit.default_timer() #start time

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    
    # The GitHub repository contains the secrets file with the environment variables. 
    # Or if running locally, set the sys environment variables in your IDE or terminal.

    logging.info('Connecting to Object Storage')
    # The secrets/passwords were updated from the Dev to the Prod environment on 29-Oct-2025.
    S3_ENDPOINT = os.getenv('S3_ENDPOINT')
    S3_CWD_ACCESS_KEY = os.getenv('S3_CWD_ACCESS_KEY')   #or S3_CWD_ACCESS_KEY_DEV for the Dev Environment, _TEST for Test Environment
    S3_CWD_SECRET_KEY = os.getenv('S3_CWD_SECRET_KEY')   #or S3_CWD_SECRET_KEY_DEV for the Dev Environment, _TEST for Test Environment
    s3_client = connect_to_os(S3_ENDPOINT, S3_CWD_ACCESS_KEY, S3_CWD_SECRET_KEY)
    # Ensure that the s3_bucket_name variable below is set correctly for the environment (prod, dev, test).
    s3_bucket_name = 'whcwdp' #'whcwdp' Production bucket, or 'whcwdd' Development bucket,  or 'whcwdt'  Test bucket

    logging.info('\nConnecting to AGO')
    AGO_HOST = os.getenv('AGO_HOST')
    AGO_USERNAME = os.getenv('AGO_USERNAME')
    AGO_PASSWORD = os.getenv('AGO_PASSWORD')
    gis = connect_to_AGO(AGO_HOST, AGO_USERNAME, AGO_PASSWORD)
    
    #Set current date/time and as a string value to use later in file names.
    pacific_timezone = pytz.timezone('America/Vancouver')
    current_datetime = datetime.now(pacific_timezone).strftime('%Y-%m-%d %H:%M:%S')
    current_datetime_str = datetime.now(pacific_timezone).strftime('%Y%m%d_%H%M%S%p')
    current_datetime_str = current_datetime_str.lower()
    timenow_rounded = datetime.now().astimezone(pacific_timezone)
    today = datetime.today().strftime('%Y%m%d')

    # Get field properties from XLS Data Dictionary
    df_datadict, domains_dict, fprop_dict = retrieve_field_properties(s3_client, s3_bucket_name)


    if s3_client:
        logging.info('\nRetrieving Incoming Sampling Data from Object Storage')
        #df = get_incoming_data_from_os(s3_client)
        #bucket_name = s3_bucket_name
        folder = 'incoming_from_idir/cwd_lab_submissions'
        required_headers = ['CWD_LAB_SUBMISSION_ID','WLH_ID','CWD_EAR_CARD_ID']  #provide either the required headers or the header index number
        df = append_xls_files_from_os(s3_client, s3_bucket_name, folder,'cwd_sample_collection', 2, required_headers=None,sheet_name='Sampling Sheet')
        logging.info(f"Number of Sampling Records from Object Storage:  {len(df)}")

        logging.info('\nGetting Email Submission file with Verified lat/longs from Object Storage') 
        filepathname = 'incoming_from_idir/cwd_lab_submissions/email_submissions/harvestcoordinates2025_receivedbyemail.xlsx'
        df_email_submissions = get_email_data_from_os(s3_client, bucket_name=s3_bucket_name, filepathname=filepathname.lower())
        logging.info(f"Number of Email Submission Records from Object Storage:  {len(df_email_submissions)}")
        
        logging.info('\nRetrieving Region and MU Centroid CSV Lookup Tables from Object Storage')
        df_rg, df_mu= get_lookup_tables_from_os(s3_client, bucket_name=s3_bucket_name)
        
    logging.info('\nGetting Legacy Hunter Survey Data from AGOL')
    # Using Feature Layer ID is more reliable than the Name, which can be ambiguous.
    #AGO_HUNTER_ITEM='CWD_Hunter_Survey_Responses'
    AGO_HUNTER_ITEM_ID = '107b4486f0674f0e8837f0745e49c194'  
    hunter_df = get_hunter_data_from_ago(AGO_HUNTER_ITEM_ID)

    logging.info('\nGetting Hunter Survey Data from CHEFS')
    BASE_URL = "https://submit.digital.gov.bc.ca/app/api/v1/forms"
    #FORM_ID =  os.getenv("CHEFS_FORM_ID")
    #API_KEY =  os.getenv("CHEFS_API_KEY")
    FORM_ID =  os.environ["CHEFS_FORM_ID"]
    API_KEY =  os.environ["CHEFS_API_KEY"]
    CHEFS_HIDDEN_FIELDS = ["LATITUDE_DD_CALC","LONGITUDE_DD_CALC", "SAMPLE_FROM_SUBMITTER"]
    chefs_df = get_hunter_data_from_chefs(BASE_URL, FORM_ID, API_KEY, CHEFS_HIDDEN_FIELDS)
    logging.info(f"Number of active records from CHEFS:  {len(chefs_df)}")


    surveys_df = combine_survey_info(hunter_df, chefs_df, df)
    logging.info(f"Total number of records from Hunter Survey and CHEFS:  {len(surveys_df)}")


    #logging.info('\nGetting MU, Region, and Municipality data from AGOL Feature Layers')
    logging.info('\nGetting MU and Municipality data from AGOL Feature Layers')
    # This points to the project specific MU layer which has REGION_RESPONSIBLE_ID 7B updated to REGION_RESPONSIBLE_NAME = 'Peace'.
    # Request to DataBC to update the WMU REGION_RESPONSIBLE_NAME was made Nov.4, 2025 to update the BCGW layer attributes. Stay tuned.
    mu_flayer_lyr, mu_flayer_fset, mu_features, mu_flayer_sdf = get_ago_flayer('0b81e46151184058a88c701853e09577')
    #Use the WMU REGION_RESPONSIBLE field to validate the entered data in the master dataset (NOT the spatial overlap with BC Env Region)
    rg_flayer_lyr, rg_flayer_fset, rg_features, rg_flayer_sdf = get_ago_flayer('118379ce29f94faeaa724d2055ea235c')
    muni_flayer_lyr, muni_flayer_fset, muni_features, muni_flayer_sdf = get_ago_flayer('e02cefad37f344008f0696663630e134')


    logging.info('\nProcessing the Master Sampling dataset')
    df, current_datetime_str, timenow_rounded = process_master_dataset (df)

    logging.info('\nAdding Survey data to Master sampling dataset')
    # UPDATED to incorporate CHEFS data.
    df_wh, hs_merged_df, flagged_hs_df = hunter_qa_and_updates_to_master(df, surveys_df)

    

    logging.info('\nAdding new hunter survey QA flags to master xls tracking list')
    # TODO: Turn back on once CHEFS data is incorporated and confirm if needed and revise as necessary.
    #updated_tracking_df = update_hs_review_tracking_list(flagged_hs_df)

    logging.info('\nUpdating the AGO Hunter Survey Feature Layer with the latest QA flags')
    # TODO: No longer needed, as AGO S123 is no longer being used. Determine if a new workflow is needed for CHEFS. Pubish new FL for combined survey data?
    #update_hunter_flags_to_ago(hs_merged_df, updated_tracking_df, AGO_HUNTER_ITEM_ID)

    logging.info('\nFinding and saving duplicate CWD_EAR_CARD_IDs and WLH_IDs in Hunter data and Sampling data. ')
    find_and_save_duplicates(hs_merged_df,'SURVEY_CWD_EAR_CARD_ID', f'qa/duplicate_ids/cwd_survey_ear_card_id_duplicates.xlsx')
    find_and_save_duplicates(df_wh,'CWD_EAR_CARD_ID', f'qa/duplicate_ids/cwd_master_sampling_ear_card_id_duplicates.xlsx')
    find_and_save_duplicates(df_wh,'WLH_ID', f'qa/duplicate_ids/cwd_master_sampling_wh_id_duplicates.xlsx')
    
    logging.info('\nAdding REG, MU, MUNI spatial overlap. Checking for mismatches between entered MU, Region, and Municipality and spatial location')
    df_wh, mu_reg_flagged = check_point_within_poly(df_wh, mu_flayer_sdf, muni_flayer_sdf, buffer_distance=50, longcol='UPDATED_LONGITUDE', latcol='UPDATED_LATITUDE')

    logging.info('\nAdding new MU REGION QA flags to master xls tracking list')
    # TODO: Determine if a new workflow is needed for CHEFS.  
    #update_sampling_mu_reg_review_tracking_list(mu_reg_flagged)

    # Get field properties from Data Dictionary
    # Add remaining calculated fields and re-order all fields according to the Data Dictionary
    df_wh = master_dataframe_edits(df_wh, fprop_dict, domains_dict)

    logging.info('\nSaving the Master Dataset xlsx to Object Storage')
    #bucket_name=s3_bucket_name   # this points to GeoDrive
    backup_master_dataset(s3_client, s3_bucket_name) #backup  
    ##save_xlsx_to_os(s3_client, s3_bucket_name, df, 'master_dataset/cwd_master_dataset_sampling.xlsx') #just lab data - used for initial testing
    ##save_xlsx_to_os(s3_client, s3_bucket_name, df_wh, 'master_dataset/cwd_master_dataset_sampling_w_hunter.xlsx') #lab + Old S123 hunter data
    save_xlsx_to_os(s3_client, s3_bucket_name, df_wh, 'master_dataset/cwd_master_dataset_sampling_w_survey_results.xlsx') #Newest Master

    #print("DONE TO HERE ")
    #sys.exit()

    logging.info('\nInitiating .... Saving a subset of the dataset for public dashboarding')
    folder= 'share_public/' # this points to GeoDrive
    save_public_db_file (df_wh, s3_client, s3_bucket_name)

    #'''
    logging.info('\nInitiating .... Saving an XLSX of sampling results for the webpage')
    # #bucket_name_bbx = 'whcwddbcbox' # this points to BCBOX Dev
    # #folder = 'Web/'       # this points to BCBOX
    # #save_web_results (df_wh, s3_client, bucket_name_bbx, folder, current_datetime_str)
    #bucket_name= s3_bucket_name # this points to GeoDrive
    folder= 'share_web/' # this points to GeoDrive
    save_web_results (df_wh, s3_client, s3_bucket_name, folder, current_datetime_str)
    

    logging.info('\nInitiating .... Saving an XLSX for lab testing')
    # bucket_name_bbx= 'whcwddbcbox' # this points to BCBOX Dev
    # folder_bbx= 'Lab/to_Lab/' # this points to BCBOX
    # save_lab_submission (df_wh, s3_client, bucket_name_bbx, folder_bbx)
    #bucket_name= s3_bucket_name # this points to GeoDrive
    folder= 'share_labs/for_lab/' # this points to GeoDrive
    save_lab_submission (df_wh, s3_client, s3_bucket_name, folder)
    
    logging.info('\nInitiating .... Saving an XLSX for tongue sample data')
    folder = 'incoming_from_idir/cwd_tongue_samples'
    required_headers = ['CWD_LAB_SUBMISSION_ID','WLH_ID','CWD_EAR_CARD_ID']  #provide either the required headers or the header index number
    df_tng_new = append_xls_files_from_os(s3_client, s3_bucket_name, folder,'genomebc_tongues', header_index_num=None, required_headers=required_headers, sheet_name=0)
    save_tongue_sample_data(df_wh,df_tng_new)

    #print("DONE TO HERE ")
    #sys.exit()

    # TESTING EXPORTING SPATIAL DATA - not yet implemented in main workflow
    #logging.info('\nSaving spatial data')
    #test_save_spatial_files(df_wh, s3_client, s3_bucket_name)
    
    logging.info(f'\nSummarizing sampling data by spatial units WMU and Environment Region.' )
    df_sampled = df_wh[(df_wh['CWD_SAMPLED_IND'] == 'Yes')]
    logging.info(f'CWD_SAMPLED_IND = Yes:  {len(df_sampled)}')
    sampled_summary_by_unit(df_sampled,'UPDATED_WMU_REG_RESPONSIBLE', 'REGION_RESPONSIBLE_NAME', rg_flayer_lyr, rg_features, rg_flayer_sdf)
    sampled_summary_by_unit(df_sampled,'UPDATED_WMU', 'WILDLIFE_MGMT_UNIT_ID', mu_flayer_lyr, mu_features, mu_flayer_sdf)


    '''
    #15-Sept-2025  Publishing to AGO is currently on hold while we revise the master template and field properties.
    # TODO: Revise this to point to the new master template, with new CHEFS/Survey Fields and test loading.  Potentially use Truncate and Load function instead, with FL option.
    logging.info('\nPublishing the Master Dataset to AGO')
    title='CWD_Master_dataset_Template'
    master_item_id = '555d7896b8914ac993264adcd9d9a547'  
    folder='2024_CWD'
    latcol='UPDATED_LATITUDE'
    longcol= 'UPDATED_LONGITUDE'
    published_item= publish_feature_layer(gis, df_wh, latcol, longcol, title, folder)
    
    logging.info('\nApplying field properties to the Feature Layer')
    #bucket_name= s3_bucket_name
    #domains_dict, fprop_dict= retrieve_field_properties(s3_client, s3_bucket_name)
    apply_field_properties (gis, title, domains_dict, fprop_dict)
    
    '''

    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    logging.info('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))