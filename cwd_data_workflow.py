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
# Updates ongoing - see GitHub for details.
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os, sys
import re
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


def get_incoming_data_from_os(s3_client):
    """
    Returns a df of incoming data from Object Storage
    """
    logging.info("..listing buckets")
    buckets = s3_client.list_buckets()
    df_list = []
    
    for bucket in buckets['Buckets']:
        bucket_name = bucket['Name']
        logging.info(f"..processing bucket: {bucket_name}")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.xlsx') and 'cwd_lab_submissions' in key.lower():
                    try:
                        logging.info(f"...reading file: {key}")
                        obj_data = s3_client.get_object(Bucket=bucket_name, Key=key)
                        df = pd.read_excel(BytesIO(obj_data['Body'].read()),
                                           header=2,
                                           sheet_name='Sampling Sheet')
                        df = df[df['WLH_ID'].notna()] #remove empty rows
                        
                        df_list.append(df)
                    except botocore.exceptions.BotoCoreError as e:
                        logging.error(f"...failed to retrieve file: {e}")
    if df_list:
        listlen = len(df_list)
        logging.info(f"..appending dataframes for {listlen} submission files")
        return pd.concat(df_list, ignore_index=True)
    else:
        logging.info("..no dataframes to append")
        return pd.DataFrame()
    

def get_lookup_tables_from_os(s3_client, bucket_name='whcwdd'):
    """
    Returns dataframes of lookup tables
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
    Returns a df containing hunter survey data from AGO and manipulate time columns to pacific time vs UTC.
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
    save_xlsx_to_os(s3_client, 'whcwdd', hunter_df, f'hunter_survey/ago_backups/cwd_hunter_survey_data_from_ago_backup_{current_datetime_str}.xlsx')


    #Convert UTC Date Times from AGO to Pacific Time
    for col in ['CreationDate','EditDate','HUNTER_MORTALITY_DATE','HUNTER_SUBMIT_DATE_TIME']:  
        # Convert to datetime and localize to UTC
        hunter_df[col] = pd.to_datetime(hunter_df[col], errors='coerce').dt.tz_localize('UTC')
        # Convert to Pacific time
        hunter_df[col] = hunter_df[col].dt.tz_convert(pacific_timezone)
        #Then remove time aware, as this is required below.
        hunter_df[col] = hunter_df[col].dt.tz_localize(None)
            
    print(f"Number of Hunter Survey Records from AGO:  {len(hunter_df)}")

    return hunter_df

def get_ago_flayer(ago_flayer_id):
    """
    Returns AGO feature layers features and spatial datafram

    Input:
    -ago_flayer_id:  AGO item ID for the feature layer

    """
    ago_flayer_item = gis.content.get(ago_flayer_id)
    ago_flayer_lyr = ago_flayer_item.layers[0]
    ago_flayer_fset = ago_flayer_lyr.query()
    ago_features = ago_flayer_fset.features
    ago_flayer_sdf = ago_flayer_fset.sdf

    return ago_flayer_lyr, ago_flayer_fset, ago_features, ago_flayer_sdf

def save_xlsx_to_os(s3_client, bucket_name, df, file_name):
    """
    Saves an xlsx to Object Storage bucket.
    """
    xlsx_buffer = BytesIO()
    df.to_excel(xlsx_buffer, index=False)
    xlsx_buffer.seek(0)

    #logging.info(f'..Trying to export {file_name} to XLS')

    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=xlsx_buffer.getvalue())
        logging.info(f'..data successfully saved {file_name} to bucket {bucket_name}')
    except botocore.exceptions.ClientError as e:
        logging.error(f'..failed to save data to Object Storage: {e.response["Error"]["Message"]}')

def process_master_dataset(df):
    """
    Populates missing Latitude and Longitude values
    Format Datetime columns
    """
    logging.info("..cleaning-up data")
    df['LATITUDE_DD'] = pd.to_numeric(df['LATITUDE_DD'], errors='coerce')
    df['LONGITUDE_DD'] = pd.to_numeric(df['LONGITUDE_DD'], errors='coerce')
    
    def set_source_value(row):
        if pd.notna(row['LATITUDE_DD']) and pd.notna(row['LONGITUDE_DD']):
            if 47.0 <= row['LATITUDE_DD'] <= 60.0 and -145.0 <= row['LONGITUDE_DD'] <= -113.0:
                return 'From Submitter'
            else:
                return 'Incorrectly entered'
        return np.nan

    df['SPATIAL_CAPTURE_DESCRIPTOR'] = df.apply(set_source_value, axis=1)
    
    columns = list(df.columns)
    long_index = columns.index('LONGITUDE_DD')
    columns.remove('SPATIAL_CAPTURE_DESCRIPTOR')
    columns.insert(long_index + 1, 'SPATIAL_CAPTURE_DESCRIPTOR')

    df = df[columns]
    
    #correct errrors in MU column
    def correct_mu_value(mu):
        # Remove any letters and spaces
        mu = re.sub(r'[a-zA-Z\s]', '', mu)
        
        # Remove leading zeros
        parts = mu.split('-')
        if len(parts) == 2 and parts[1].startswith('0'):
            parts[1] = parts[1][1:]
        return '-'.join(parts)
    
    df['WMU'] = df['WMU'].apply(correct_mu_value)
    
    
    logging.info("..populating missing latlon from MU and Region centroids")
    def latlong_from_MU(row, df_mu):
        if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR']) or row['SPATIAL_CAPTURE_DESCRIPTOR'] == 'Incorrectly entered by user') and row['WMU'] != 'Not Recorded':
            mu_value = row['WMU']
            match = df_mu[df_mu['MU'] == mu_value]
            if not match.empty:
                row['LATITUDE_DD'] = match['CENTER_LAT'].values[0]
                row['LONGITUDE_DD'] = match['CENTER_LONG'].values[0]
                row['SPATIAL_CAPTURE_DESCRIPTOR'] = 'MU Centroid'
        return row
    
    df = df.apply(lambda row: latlong_from_MU(row, df_mu), axis=1)
    
    #populate lat/long Region centroid centroid
    def latlong_from_Region(row, df_rg):
        if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR']) or row['SPATIAL_CAPTURE_DESCRIPTOR'] == 'Incorrectly entered by user') and row['ENV_REGION_NAME'] != 'Not Recorded':
            rg_value = row['ENV_REGION_NAME']
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
    date_columns = df.columns[df.columns.str.contains('DATE')]
    df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')


    #ADD THIS IN AGAIN to make POwerBI work?? 2024-10-10
    #print(df.MORTALITY_DATE)
    ##convert the 'DATE' columns to only show the date part as short - don't include 00:00:00 time
    ## DO IN Web and Lab exports instead.  Need full dates for AGO and PowerBI(?)
    ##  Warning:  This converts to a string vs a date type!
    for col in date_columns:
        if 'DATE' in col and col != 'GIS_LOAD_VERSION_DATE':  #exclude GIS_LOAD_VERSION_DATE
            df[col] = pd.to_datetime(df[col]).dt.date   #e.g.2024-09-08
    
    #print(df.MORTALITY_DATE)

    #Sort
    #df = df.sort_values(by=['SAMPLING_SESSION_ID','WLH_ID'])
    df = df.sort_values(by=['WLH_ID'])

    return df, current_datetime_str, timenow_rounded


def hunter_qa_and_updates_to_master(df, hunter_df):
    """
    Inputs:
        - df: Master Sampling dataset - all combined sampling records
        - hunter_df: Hunter Survey data from AGO - all records

    Outputs:  
        1. All Hunter Survey (hs) records from AGO with QA flags for where there are discrepancies between the Hunter Survey data and the Master Sampling dataset.
           Flagged discrepancies are exported as an xls file (cwd_hunter_survey_flags_for_review_<datetime>.xlsx) to Object Storage for WH Team review, and QA flags are updated back to the Hunter data AGO.
           The WH Team will review the discrepancies and update the individual Sampling sheets as needed. They will
           update the two Review fields as appropriate, which will subsequently be added to the Hunter Survey data in AGO by this script.
        2. The Master Sampling dataset for which the lat/long and map source descriptor is corrected based on the hunter survey data, where there is a matching hs record.
           This does not include Hunter Survey records where there is no matching Ear Tag ID and therefore there is no sampling data yet.

        - hs_merged_df:   all hunter survey records with new qa columns and duplicates identified
        - flagged_hs_df:  flagged hunter survey records for qa review
        - df_wh:          updated master sampling dataset with hunter (wh) survey matches
    """
    #logging.info("..calculating Hunter Survey QA flags")

    # Length of initial dataframes
    print(f"\n\t{len(df.index)}... Total Sampling Records in df")
    print(f"\t{len(hunter_df.index)}... Total Hunter Survey Records in hunter_df\n")

    cols = [
    "HUNTER_CWD_EAR_CARD_ID",
    "HUNTER_CWD_EAR_CARD_ID_TEXT",
    "HUNTER_SUBMIT_DATE_TIME",
    "HUNTER_MORTALITY_DATE",
    "HUNTER_SPECIES",
    "HUNTER_SEX",
    "HUNTER_LATITUDE_DD",
    "HUNTER_LONGITUDE_DD",
    "GEO_CHECK_PROV",
    "QA_HUNTER_SURVEY_FLAG",
    "QA_HUNTER_SURVEY_FLAG_DESC",
    "QA_HUNTER_EARCARD_DUPLICATE",
    "QA_HUNTER_SURVEY_REVIEW_STATUS",
    "QA_HUNTER_SURVEY_REVIEW_COMMENTS"
    ]

    hunter_df = hunter_df[cols]

    logging.info("..calculating Hunter Survey QA field defaults")
    # calc/reset default values for qa hunter columns. This will be re-calculated below.
    #hunter_df[['QA_HUNTER_SURVEY_FLAG','QA_HUNTER_SURVEY_FLAG_DESC']] = None
    hunter_df[['QA_HUNTER_SURVEY_FLAG','QA_HUNTER_SURVEY_FLAG_DESC','QA_HUNTER_EARCARD_DUPLICATE']] = ''
    hunter_df['QA_HUNTER_CHECK_DATE_TIME'] = current_datetime

    logging.info("..manipulating CWD_EAR_CARD_ID column")
    # convert CWD Ear Card values from df to  integer to string
    df['CWD_EAR_CARD_ID'] = df['CWD_EAR_CARD_ID'].apply(lambda x: str(int(x)) if pd.notnull(x) and x != 'None' else None)
 
 
    # NOTE!  There may be duplicate Ear Card IDs in both the sampling data and the Hunter Survey data.  
    # May need to drop duplicates in df before joining to avoid addition of cross rows?
    # This should be checked and resolved by the Wildlife Health Team.
    logging.info("..joining matching sampling records to hunter survey data")
    hs_merged_df = pd.merge(left=hunter_df,
                         right=df, 
                         left_on='HUNTER_CWD_EAR_CARD_ID_TEXT', 
                         right_on='CWD_EAR_CARD_ID', 
                         how='left', 
                         indicator=True)

    #print(f"\n\t{len(hs_merged_df.index)}... total records in hs_merged_df\n")
    

    logging.info("..calculating Hunter Survey QA flags")
        
    # Flag records that DO NOT have an ear card id in the master sampling dataset, Otherwise, Matched
    # Note again, that there may be duplicate Ear Card IDs in both the sampling data and the Hunter Survey data.
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where(hs_merged_df['_merge'] == 'left_only',
                                                'No Sampling Match', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])  #'Matched')

    #Flag records that DO have an ear card id in the master sampling dataset
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where(hs_merged_df['_merge'] == 'both',
                                                'Matched', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    # Update 'QA_HUNTER_SURVEY_FLAG' to 'Check' for records where the species do not match
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where((hs_merged_df['SPECIES'] != hs_merged_df['HUNTER_SPECIES']) &
                                               (hs_merged_df['SPECIES'].notna() & hs_merged_df['HUNTER_SPECIES'].notna()), 
                                               'Check', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'] = np.where((hs_merged_df['SPECIES'] != hs_merged_df['HUNTER_SPECIES']) &
                                                        (hs_merged_df['SPECIES'].notna() & hs_merged_df['HUNTER_SPECIES'].notna()), 
                                                        #hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('Mismatched value(s):') + ' Species;',
                                                        'Species',
                                                        hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'])

    # Update 'QA_HUNTER_SURVEY_FLAG' to 'Check' for records where the sex does not match
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where((hs_merged_df['SEX'] != hs_merged_df['HUNTER_SEX']) &
                                               (hs_merged_df['SEX'].notna() & hs_merged_df['HUNTER_SEX'].notna()), 
                                               'Check', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'] = np.where((hs_merged_df['SEX'] != hs_merged_df['HUNTER_SEX']) &
                                                     (hs_merged_df['SEX'].notna() & hs_merged_df['HUNTER_SEX'].notna()), 
                                                     #hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('Mismatched value(s):') + ' Sex;',
                                                     hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'].fillna('') + ', Sex',
                                                     #', Sex',
                                                     hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'])

    # Update 'QA_HUNTER_SURVEY_FLAG' to 'Check' for records where the mortality dates do not match
    # Ensure date formats match - Converted to Pacific Time (above) and then to date part only
    hs_merged_df['MORTALITY_DATE'] = pd.to_datetime(hs_merged_df['MORTALITY_DATE'], errors='coerce').dt.date
    hs_merged_df['HUNTER_MORTALITY_DATE'] = pd.to_datetime(hs_merged_df['HUNTER_MORTALITY_DATE'], errors='coerce').dt.date
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG'] = np.where((hs_merged_df['MORTALITY_DATE'] != hs_merged_df['HUNTER_MORTALITY_DATE']) &
                                               (hs_merged_df['MORTALITY_DATE'].notna() & hs_merged_df['HUNTER_MORTALITY_DATE'].notna()), 
                                               'Check', hs_merged_df['QA_HUNTER_SURVEY_FLAG'])
    
    hs_merged_df['QA_HUNTER_SURVEY_FLAG_DESC'] = np.where((hs_merged_df['MORTALITY_DATE'] != hs_merged_df['HUNTER_MORTALITY_DATE']) &
                                                     (hs_merged_df['MORTALITY_DATE'].notna() & hs_merged_df['HUNTER_MORTALITY_DATE'].notna()), 
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
    hs_merged_df['QA_HUNTER_EARCARD_DUPLICATE'] = np.where(hs_merged_df.duplicated(subset='HUNTER_CWD_EAR_CARD_ID', keep=False), 'DUPLICATE', '')
    

    # Convert time aware datetime columns to string, otherwise cannot export to xls
    for col in ['MORTALITY_DATE','HUNTER_MORTALITY_DATE','HUNTER_SUBMIT_DATE_TIME']: 
        #hs_merged_df[col] = hs_merged_df[col].dt.tz_convert(None)  #remove timezone info (but it's still time aware)
        hs_merged_df[col] = hs_merged_df[col].astype(str)
        #hs_merged_df[col] = hs_merged_df[col].dt.tz_localize(None)
    

    # Fill nan values with empty strings
    hs_merged_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']] = hs_merged_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']].fillna('')
    
    #Export the Hunter Survey QA flags and associated sampling data to an XLS file for review by the WH Team
    export_cols = [
        "HUNTER_CWD_EAR_CARD_ID",
        "HUNTER_SUBMIT_DATE_TIME",
        "HUNTER_MORTALITY_DATE",
        "MORTALITY_DATE",
        "HUNTER_SPECIES",
        "SPECIES",
        "HUNTER_SEX",
        "SEX",
        "HUNTER_LATITUDE_DD",
        "HUNTER_LONGITUDE_DD",
        "GEO_CHECK_PROV",
        "QA_HUNTER_SURVEY_FLAG",
        "QA_HUNTER_SURVEY_FLAG_DESC",
        "QA_HUNTER_EARCARD_DUPLICATE",
        "QA_HUNTER_SURVEY_REVIEW_STATUS",
        "QA_HUNTER_SURVEY_REVIEW_COMMENTS",
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
        "ENV_REGION_NAME",
        "WMU",
        "SUBMITTER_TYPE",
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
    
    print(f"\n\t{len(flagged_hs_df.index)}... flagged hunter survey qa records found ... of a total of {len(hs_merged_df.index)} hs_merged_df records\n")
    
    logging.info("..saving the Hunter Survey QA flags to xls")
    save_xlsx_to_os(s3_client, 'whcwdd', flagged_hs_df, f'qa/hunter_survey_mismatches/bck/cwd_hunter_survey_1_flags_for_review_{current_datetime_str}.xlsx')
    # TEMP
    save_xlsx_to_os(s3_client, 'whcwdd', hs_merged_df, f'qa/hunter_survey_mismatches/bck/cwd_hunter_survey_2_all_records_and_flags_{current_datetime_str}.xlsx')
    

    #--------------------------------------------- end of the QA checks ----------------------------------------------
    #  Merge the Hunter Survey data with the Master Sampling data:
    
    cols = [
    "HUNTER_MORTALITY_DATE",
    "HUNTER_SPECIES",
    "HUNTER_SEX",
    "HUNTER_LATITUDE_DD",
    "HUNTER_LONGITUDE_DD",
    'HUNTER_CWD_EAR_CARD_ID_TEXT',
    "HUNTER_CWD_EAR_CARD_ID",
    "HUNTER_SUBMIT_DATE_TIME"
    ]   

    hunter_df = hunter_df[cols]
    hunter_df_no_dups = hunter_df.drop_duplicates(subset="HUNTER_CWD_EAR_CARD_ID_TEXT", keep="last")

    logging.info("\n..merging hunter survey results to sampling dataframe")
    # merge the dataframes
    # note that if there are duplicate Ear Card IDs in the hunter survey data, 
    # then there will be multiple records in the merged dataframe, if drop_duplicates is not used, above.
    # This could be a problem if the last record is not the 'best' but will be resolved once the hunter survey spatial data is corrected.
    # if drop_duplicates is used, then only the 'last' hunter survey record will be kept.  

    # Length of initial dataframes
    #print(f"\n\t{len(df.index)}...  Sampling Records in df")
    #print(f"\t{len(hunter_df.index)}...  Hunter Survey Records in hunter_df")
    #print(f"\t{len(hunter_df_no_dups.index)}...  Hunter Survey Records in hunter_df_no_dups\n")

    combined_df = pd.merge(left=df,
                       right=hunter_df_no_dups,
                       how="left",
                       left_on="CWD_EAR_CARD_ID",
                       right_on="HUNTER_CWD_EAR_CARD_ID_TEXT")
                       #right_on="HUNTER_CWD_EAR_CARD_ID_TEXT").drop_duplicates(subset="HUNTER_CWD_EAR_CARD_ID_TEXT", keep="last")
    
    #print(f"\t{len(combined_df.index)}... records in combined_df")
    
    logging.info("..cleaning dataframes")
    # filter df for where hunters have updated data.  i.e. sex cannot be null.
    hunter_matches_df = combined_df[combined_df.HUNTER_SEX.notnull()].copy()
    
    # filter df for where hunters have not updated data
    xls_df = combined_df[combined_df.HUNTER_SEX.isnull()].copy()

    
    # for hunter_matches_df - update MAP_SOURCE_DESCRIPTOR w/ value = Hunter Survey
    xls_df['MAP_SOURCE_DESCRIPTOR'] =  xls_df['SPATIAL_CAPTURE_DESCRIPTOR']
    hunter_matches_df['MAP_SOURCE_DESCRIPTOR'] = "Hunter Survey"
    
    # clean up xls_df to conform with ago field requirements 
    xls_df[['HUNTER_SPECIES', 'HUNTER_SEX', 'HUNTER_MORTALITY_DATE']] = None

    # populate MAP_LATITUDE and MAP_LONGITUDE columns
    hunter_matches_df[['MAP_LATITUDE', 'MAP_LONGITUDE']] = hunter_matches_df[['HUNTER_LATITUDE_DD', 'HUNTER_LONGITUDE_DD']]
    xls_df[['MAP_LATITUDE', 'MAP_LONGITUDE']] = xls_df[['LATITUDE_DD', 'LONGITUDE_DD']]

    #print(f"\n\t{len(hunter_matches_df.index)}... records in hunter_matches_df")
    #print(f"\t{len(xls_df.index)}... records in xls_df")

    # re-combine dataframes
    df_wh = pd.concat([hunter_matches_df, xls_df], ignore_index=True)
    
    # Move columns to the last position
    df_wh['MAP_SOURCE_DESCRIPTOR'] = df_wh.pop('MAP_SOURCE_DESCRIPTOR')
    df_wh['GIS_LOAD_VERSION_DATE'] = df_wh.pop('GIS_LOAD_VERSION_DATE')

    # Convert all columns containing 'DATE' in their names to datetime
    date_columns = df_wh.columns[df_wh.columns.str.contains('DATE')]
    df_wh[date_columns] = df_wh[date_columns].apply(pd.to_datetime, errors='coerce')

    #ADD This back in?
    #strip time portion of date/time
    ##Beware! This converts to String type! May not be compatible if using in PowerBI,etc.
    #date_columns_convert = ['COLLECTION_DATE','MORTALITY_DATE','SAMPLED_DATE','SAMPLE_DATE_SENT_TO_LAB','REPORTING_LAB_DATE_RECEIVED','CWD_TEST_STATUS_DATE','PREP_LAB_LAB_DATE_RECEIVED','PREP_LAB_DATE_FORWARDED']
    #for col in date_columns_convert:
    #    df_wh[col] = pd.to_datetime(df_wh[col]).dt.date   #e.g.2024-09-08  

    #COLLECTION_DATE to string (for PowerBI?)
    df_wh['COLLECTION_DATE']= df_wh['COLLECTION_DATE'].astype(str)

    #replace Not a Time (NaT) for entire dataframe
    df_wh = df_wh.replace(['NaT'], '')

    #Sort
    df_wh = df_wh.sort_values(by=['WLH_ID'])

    print(f"\n\t{len(df_wh.index)}... records in df_wh")
    print(f"\t{len(hs_merged_df.index)}... records in hs_merged_df")
    print(f"\t{len(flagged_hs_df.index)}... records in flagged_hs_df\n")

    return df_wh, hs_merged_df, flagged_hs_df

def update_hs_review_tracking_list(flagged_hs_df):

    """
    Reads the existing master hunter survey QA tracking xls file into a dataFrame, reads new hs data from AGO, 
    compares the records, and appends new records based on HUNTER_CWD_EAR_CARD_ID to the master tracking dataFrame.

    This tracking xls preserves the original flags and review status for each record, before any changes may be made in the individual sampling sheets by the WH Team.
    This lets the team see what the original QA flags were, and what changes were made to the records.
    
    NOTE:  28-Feb-2025 - there is a glitch in S3 storage where the file is disappearing.  Trying to resolve this with the help desk.

    Parameters:
    - flagged_hs_df: DataFrame containing the flagged hunter survey records.
    
    Outputs:
    - updated_tracking_df:  Updated static master hunter survey with newly flagged qa records appended.
    """

    # Read the static master XLS file into a DataFrame
    static_xls_path = 'qa/tracking/cwd_hunter_survey_qa_flag_status_tracking_master.xlsx'
    logging.info(f"...Checking for file: {static_xls_path}")
    try:
        logging.info(f"...reading file: {static_xls_path}")
        obj = s3_client.get_object(Bucket='whcwdd', Key=static_xls_path)
        data = obj['Body'].read()
        excel_file = pd.ExcelFile(BytesIO(data))

        static_df = pd.read_excel(excel_file, sheet_name='Sheet1')

        # Compare records and append new flagged hunter survey records to the static DataFrame
        new_records = flagged_hs_df[~flagged_hs_df['HUNTER_CWD_EAR_CARD_ID'].isin(static_df['HUNTER_CWD_EAR_CARD_ID'])]
        
        #if there are new records to add, then append them to the static DataFrame
        if len(new_records) == 0:
            updated_tracking_df = static_df
            logging.info(f"...no new records to add to the Hunter QA tracking list")
        else:
            # Append new records to the static DataFrame
            updated_tracking_df = pd.concat([static_df, new_records], ignore_index=True)

            # Fill nan values with empty strings
            updated_tracking_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']] = updated_tracking_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']].fillna('')
            #replace Not a Time (NaT) for entire dataframe
            updated_tracking_df = updated_tracking_df.replace(['NaT'], '')

            # Sort the DataFrame
            updated_tracking_df = updated_tracking_df.sort_values(by=['QA_HUNTER_SURVEY_FLAG','QA_HUNTER_SURVEY_FLAG_DESC','HUNTER_CWD_EAR_CARD_ID'])

            # Overwrite to xls
            logging.info("..saving the revised tracking list to xls")
            print(f"\n{len(new_records)}... new flagged records found for Hunter Survey... added to {len(static_df)} existing records\n")
            #save_xlsx_to_os(s3_client, 'whcwdd', updated_tracking_df, f'qa/hunter_survey_mismatches/cwd_hunter_survey_qa_flag_status_tracking_master_{current_datetime_str}.xlsx')
            save_xlsx_to_os(s3_client, 'whcwdd', updated_tracking_df, f'qa/tracking/cwd_hunter_survey_qa_flag_status_tracking_master.xlsx')

    except Exception as e:
        logging.info(f"\n***WARNING...tracking file not currently found: {static_xls_path}\n...Skipping the update of the xls tracking list.\n")
        # Create an empty dataframe as a place holder until xls is back online
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
    "HUNTER_CWD_EAR_CARD_ID_TEXT",
    "HUNTER_CWD_EAR_CARD_ID",
    "QA_HUNTER_EARCARD_DUPLICATE",
    "QA_HUNTER_SURVEY_FLAG",
    "QA_HUNTER_SURVEY_FLAG_DESC",
    "QA_HUNTER_SURVEY_REVIEW_STATUS",
    "QA_HUNTER_SURVEY_REVIEW_COMMENTS",
    ]   
    updated_df = df_hs[cols]
    print('Hunter Survey Records to Revise :  ',len(updated_df))
    
    
    ##### Use Pandas Dataframes to update AGO Layers
    ## get spatial features from AGO
    hunter_survey_item = gis.content.get(spatial_fl_id)
    hs_lyr = hunter_survey_item.layers[0]
    hs_fset = hs_lyr.query()
    hs_features = hs_fset.features
    hs_sdf = hs_fset.sdf
    #print(hs_sdf)
    print('Records in Hunter Survey Layer:  ',len(hs_sdf))
    

    uCount = 0   #initiate counter

    logging.info('Updating the AGO Hunter Survey Feature Layer with the latest QA Flags...please be patient')
    # Update the Feature Layer with the QA flags for matching records.  
    # This may overwrite existing values in the FL with the new qa values.  That is ok.
    uCount = 0   #initiate counter
    fl_sum_fld = 'HUNTER_CWD_EAR_CARD_ID'
    df_sum_fld = 'HUNTER_CWD_EAR_CARD_ID'
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
            print(f"Could not update {ROW_ID}. Exception: {e}.  Ear Card ID may have been updated.")
            continue
    logging.info(f'DONE updating the Hunter Survey AGO Feature Layer with the new QA Flags for:  {uCount} records.')

    #if updated_tracking_df is not empty (i.e. xls file is available and has records), then update the AGO FL with the review status and comments  
    if not updated_tracking_df.empty:
        logging.info('Updating the AGO Hunter Survey Feature Layer with the latest QA tracking review status and comments ...')

        # Fill nan values with empty strings
        updated_tracking_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']] = updated_tracking_df[['QA_HUNTER_SURVEY_REVIEW_STATUS', 'QA_HUNTER_SURVEY_REVIEW_COMMENTS']].fillna('')
    
        """# Testing: filter df for where there are review status /comments
        updated_tracking_df_select = updated_tracking_df[
            (updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_STATUS'].notna() & updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_STATUS'].str.strip() != '') |
            (updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_COMMENTS'].notna() & updated_tracking_df['QA_HUNTER_SURVEY_REVIEW_COMMENTS'].str.strip() != '')
            ]
        
        print(f"{len(updated_tracking_df_select)}... records found for Hunter Survey review tracking... of a total of {updated_tracking_df} flagged records\n")
        """
        
        # Update the Feature Layer with the QA flags for matching records, if there are any available records.
        # Use all records in the updated_tracking_df, without any filter, in order to update with any revised review status or comments.
        
       
        uCount = 0   #initiate counter
        for ROW_ID in updated_tracking_df[fl_sum_fld]:
            try:
                upd_feature = [f for f in hs_features if f.attributes[fl_sum_fld] == ROW_ID][0]  #get the spatial feature
                upd_row = updated_tracking_df.loc[updated_tracking_df[df_sum_fld] == ROW_ID]   #get the dataframe row
                upd_val_1 = upd_row['QA_HUNTER_SURVEY_REVIEW_STATUS'].values[0]
                upd_val_2 = upd_row['QA_HUNTER_SURVEY_REVIEW_COMMENTS'].values[0]
                upd_feature.attributes['QA_HUNTER_SURVEY_REVIEW_STATUS'] = str(upd_val_1)
                upd_feature.attributes['QA_HUNTER_SURVEY_REVIEW_COMMENTS'] = str(upd_val_2)
                hs_lyr.edit_features(updates=[upd_feature])
                uCount = uCount + 1
            except Exception as e:
                print(f"Could not update {ROW_ID}. Exception: {e}")
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
    df_non_null = df[df[fld].notna()]

    # Find duplicate values
    duplicate_mask = df_non_null.duplicated(subset= fld, keep=False)
    duplicate_df = df_non_null[duplicate_mask]
    
    # Sort by fld
    duplicate_df = duplicate_df.sort_values(by=fld)

    # Save the duplicate values to an Excel report
    if not duplicate_df.empty:
        save_xlsx_to_os(s3_client, 'whcwdd', duplicate_df, output_path)
        #print(f"Duplicate values saved to {output_path}")
    else:
        print(f"No duplicate values found for {fld}")
    
    return duplicate_df



def check_point_within_mu_region(df_wh, mu_flayer_sdf, rg_flayer_sdf, longcol, latcol):
    """
    Flags points where the sampling MU or Region does not match the spatial MU or Region.
    Note that there may be cases where the point is very close to a Region or MU border.
    
    This is only be for records where the MAP_SOURCE_DESCRIPTOR is Hunter Survey or From Submitter i.e. not for centroid based locations.

    Add new fields to the master xls and FL for QA checks - QA_REG_WMU_CHECK, (QA_REG_WMU_CHECK_DATE_TIME)
    """
    # convert MU and Region feature layers to geodataframes
    mu_gdf = gpd.GeoDataFrame(mu_flayer_sdf, geometry='SHAPE', crs="EPSG:4326")[['WILDLIFE_MGMT_UNIT_ID', 'SHAPE']]
    rg_gdf = gpd.GeoDataFrame(rg_flayer_sdf, geometry='SHAPE', crs="EPSG:4326")[['REGION_NAME', 'SHAPE']]

    
    df_length = len(df_wh)
    df = df_wh.dropna(subset=[latcol, longcol])
    # filter for specific records from Hunter Survey or Submitter - i.e. based on specific lat/longs, not centroids.
    df_select = df[(df['MAP_SOURCE_DESCRIPTOR'] == 'Hunter Survey') | (df['MAP_SOURCE_DESCRIPTOR'] == 'From Submitter')]
    
    print(f"\t{len(df_select)}... records found for Hunter or Submitter locations... of a total of {df_length} sampling records")
    
    # Calc / Recalc default values for QA Checks - overwrites previous values.
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
    sjoin_mu_gdf = sjoin_mu_gdf.drop(['index_left', 'index_right'], axis=1, errors='ignore')

    sjoin_gdf = gpd.sjoin(sjoin_mu_gdf, rg_gdf, how='left', predicate='within', lsuffix='left', rsuffix='right')


    # compare columns and update flags
    logging.info("..updating flags")

     # Where there are sampling blanks - populate sampling Region/MU w/ values from the spatial join?
     # or just keep in the spatial columns.  WMU_SPATIAL, ENV_REGION_SPATIAL
    """ 
    sjoin_gdf['WMU'] = sjoin_gdf['WMU'].fillna(sjoin_gdf['WILDLIFE_MGMT_UNIT_ID'])
    sjoin_gdf['ENV_REGION_NAME'] = sjoin_gdf['ENV_REGION_NAME'].fillna(sjoin_gdf['REGION_NAME'])
    """

    ##DOUBLE CHECK - are missing MUs from Ear Card filled in in the spatial column?
    
    # Flag records where the Region does not match the spatial Region
    sjoin_gdf['QA_REG_WMU_CHECK'] = np.where((sjoin_gdf['REGION_NAME'].notna()) & (sjoin_gdf['ENV_REGION_NAME'].notna()) &
                                                    (sjoin_gdf['REGION_NAME'] != sjoin_gdf['ENV_REGION_NAME']),
                                                    'REG',
                                                    sjoin_gdf['QA_REG_WMU_CHECK'])
    
    # Flag records where the MU does not match the spatial MU
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

    
    # drop joined columns
    # sjoin_df = sjoin_gdf.drop(['geometry', 'index_left', 'index_right', 'WILDLIFE_MGMT_UNIT_ID', 'REGION_NAME'], axis=1, errors='ignore')
    sjoin_df = sjoin_gdf.drop(['geometry', 'index_left', 'index_right'], axis=1, errors='ignore')

    """
    # append the rows with Unknown locations back to the dataframe
    unknown_df = df[df['MAP_SOURCE_DESCRIPTOR'].isin(['Region Centroid', 'MU Centroid', 'Unknown'])]
    sjoin_df = pd.concat([sjoin_df, unknown_df], ignore_index=True)
    """

    # Rename spatial columns
    sjoin_df = sjoin_df.rename(columns={
        'WILDLIFE_MGMT_UNIT_ID': 'WMU_SPATIAL',
        'REGION_NAME': 'ENV_REGION_SPATIAL'
    })

    #Get select columns
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
        "HUNTER_LATITUDE_DD",
        "HUNTER_LONGITUDE_DD",
        "MAP_LATITUDE",
        "MAP_LONGITUDE",
        "MAP_SOURCE_DESCRIPTOR",
        "ENV_REGION_NAME",
        "ENV_REGION_SPATIAL",
        "WMU",
        "WMU_SPATIAL",
        "QA_REG_WMU_CHECK",
        "QA_REG_WMU_CHECK_DATE_TIME",
        "QA_REG_WMU_CHECK_STATUS",
        "QA_REG_WMU_CHECK_COMMENTS",
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
        "SUBMITTER_TYPE",
        "SUBMITTER_FIRST_NAME",
        "SUBMITTER_LAST_NAME",
        "SUBMITTER_PHONE",
        "FWID",
        "SAMPLED_DATE",
        "SAMPLED_IND",
        "CWD_SAMPLED_IND",
        "CWD_TEST_STATUS",
        "CWD_TEST_STATUS_DATE",
        "HUNTER_CWD_EAR_CARD_ID",
        "HUNTER_MORTALITY_DATE",
        "HUNTER_SPECIES",
        "HUNTER_SEX",
        "HUNTER_SUBMIT_DATE_TIME"
        ]    
  
    
    #Re-orders the columns, as listed above. Then sort.
    mu_reg_flagged = sjoin_df[cols]
    #mu_reg_flagged = mu_reg_flagged.sort_values(by=['QA_REG_FLAG','QA_WMU_FLAG'])

    # How many flagged?
    #mu_reg_flagged = mu_reg_flagged[(mu_reg_flagged['QA_REG_FLAG'] == 'Check ENV REGION Survey') | (mu_reg_flagged['QA_WMU_FLAG'] == 'Check WMU')]
    mu_reg_flagged = mu_reg_flagged[(mu_reg_flagged['QA_REG_WMU_CHECK'] != '')]
    print(f"\t{len(mu_reg_flagged)}... records found for Flagged REGION or WMU mistmatches from Hunter or Submitter... of a total of {df_length} sampling records\n")

    
    # convert WMU column to string
    mu_reg_flagged['WMU'] = mu_reg_flagged['WMU'].astype(str)
    mu_reg_flagged['WMU_SPATIAL'] = mu_reg_flagged['WMU_SPATIAL'].astype(str)

    # sort
    mu_reg_flagged = mu_reg_flagged.sort_values(by=['WLH_ID'])

    # Export list to XLS to overwrite master tracking xls
    save_xlsx_to_os(s3_client, 'whcwdd', mu_reg_flagged, f'qa/mu_reg_checks/bck/cwd_master_mu_region_checks_{current_datetime_str}.xlsx')
    # TEMP - to create inital master tracking xls.
    #save_xlsx_to_os(s3_client, 'whcwdd', mu_reg_flagged, f'qa/tracking/cwd_sampling_mu_region_checks_tracking_master.xlsx')

    # Add QA_REG_WMU_CHECK to master sampling df
    df_wh = df_wh.merge(mu_reg_flagged[['WLH_ID','QA_REG_WMU_CHECK']], on='WLH_ID', how='left')
    df_wh['GIS_LOAD_VERSION_DATE'] = df_wh.pop('GIS_LOAD_VERSION_DATE')   #Move to last column
    
    return df_wh, mu_reg_flagged

def update_sampling_mu_reg_review_tracking_list(flagged_df):

    """
    Reads the existing master sampling tracking xls file into a dataFrame, reads flagged QA_REG_WMU_CHECK records , 
    compares the records, and appends new records based on WLH_ID to the master tracking dataFrame.

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
        logging.info(f"...reading file: {static_xls_path}")
        obj = s3_client.get_object(Bucket='whcwdd', Key=static_xls_path)
        data = obj['Body'].read()
        excel_file = pd.ExcelFile(BytesIO(data))

        static_df = pd.read_excel(excel_file, sheet_name='Sheet1')

        # Compare records and append new flagged hunter survey records to the static DataFrame
        new_records = flagged_df[~flagged_df['WLH_ID'].isin(static_df['WLH_ID'])]
        
        print(f"\n{len(new_records)}... new flagged records found for MU or REGION mismatches... compared to {len(static_df)} existing records\n")
        
        new_records = new_records.reset_index()

        # Ensure both DataFrames have the same columns and data types
        static_df['QA_REG_WMU_CHECK_STATUS'] = static_df['QA_REG_WMU_CHECK_STATUS'].astype(object)
        static_df['QA_REG_WMU_CHECK_COMMENTS'] = static_df['QA_REG_WMU_CHECK_COMMENTS'].astype(object)

        new_records['QA_REG_WMU_CHECK_STATUS'] = new_records['QA_REG_WMU_CHECK_STATUS'].astype(object)
        new_records['QA_REG_WMU_CHECK_COMMENTS'] = new_records['QA_REG_WMU_CHECK_COMMENTS'].astype(object)
        
        for col in static_df.columns:
            if col in new_records.columns:
                #print(f"\tColumn {col} found: {static_df[col].dtype} ... {new_records[col].dtype}")
                new_records[col] = new_records[col].astype(static_df[col].dtype)
            else:
                print(f"Column {col} NOT found in new_records")
        

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
        save_xlsx_to_os(s3_client, 'whcwdd', tracking_df, f'qa/tracking/cwd_sampling_mu_region_checks_tracking_master.xlsx')

    except Exception as e:
        logging.info(f"\n***WARNING...tracking file not currently found: {static_xls_path}\n...Skipping the update of the xls tracking list.\n")

    return

def sampled_summary_by_unit(df_sum_fld, fl_sum_fld, summ_zones_lyr, summ_zones_features, summ_zones_sdf):
    """
    Summarize a count of  'CWD_SAMPLED' by spatial unit - e.g. MU or MOE Region
    Export to excel.
    Update values in AGO.
    """ 

    logging.info(f'\nSummarizing Data by...{df_sum_fld} and joining to the AGO Feature Layer' )
    df_sampled = df_wh[(df_wh['CWD_SAMPLED_IND'] == 'Yes')]
    print('CWD_SAMPLED_IND = Yes:  ',len(df_sampled))


    # Filter columns to include
    df_unit_sampled = df_sampled[[df_sum_fld,'CWD_SAMPLED_IND']]
    #Group by the spatial unit field and count the number of samples
    df_unit_count = df_unit_sampled.groupby(df_sum_fld)['CWD_SAMPLED_IND'].count().reset_index(name='Sample_Count')
    df_unit_count = df_unit_count.sort_values(by=[df_sum_fld])
    #Add a field to store the GIS_LOAD_VERSION_DATE
    df_unit_count['GIS_LOAD_VERSION_DATE'] = current_datetime
    #print(f'Number of summary units with sampled data for {df_sum_fld}: {df_unit_count}')

    ## Save the summary to a file in object storage
    logging.info('\nSaving the TEST summary')
    bucket_name='whcwdd'
    summary_file_name = (df_sum_fld.lower()) +'_rollup_summary.xlsx'
    save_xlsx_to_os(s3_client, 'whcwdd', df_unit_count, 'master_dataset/spatial_rollup/'+summary_file_name)
    

    # find rows in sampling summary that don't have a spatial unit match.  These are probably due to data entry errors.
    df_mismatch_errors = df_unit_count[~df_unit_count[df_sum_fld].isin(summ_zones_sdf[fl_sum_fld])]
    print('\nCHECK DATA: The following unit(s) do not exist in the spatial data:  \n',df_mismatch_errors)
    mismatch_file_name = 'no_spatial_match_'+ (df_sum_fld.lower()) +'.xlsx'
    save_xlsx_to_os(s3_client, 'whcwdd', df_mismatch_errors, 'qa/mu_reg_checks/'+mismatch_file_name)

    # find overlapping rows
    print(f"\nChecking for intersecting rows based on {df_sum_fld}/{fl_sum_fld}...this could take awhile...")
    overlap_rows = pd.merge(left = summ_zones_sdf, right = df_unit_count, how='inner', left_on = fl_sum_fld , right_on = df_sum_fld)
    rcount = len(overlap_rows)
    #rcount = overlap_rows.count()
    print(f'Number of Intersecting Rows: {rcount}')
    #print(overlap_rows)

    
    #Update cursor using the summary df directly (vs converting it to a hosted table in AGO)
    #First, set default Feature Layer rollup to 0 and the current date/time
    uCount = 0
    print("\nCalculating default values for Feature Layer...this could take awhile...")
    for feature in summ_zones_features:
        try:
            feature.attributes['CWD_SAMPLE_COUNT'] = 0
            feature.attributes['GIS_LOAD_VERSION_DATE'] = timenow_rounded
            summ_zones_lyr.edit_features(updates=[feature])
            uCount = uCount + 1
        except Exception as e:
            print(f"Could not update Feature Layer with default values. Exception: {e}")
            print('\nExiting...\n')
            sys.exit()
            #continue

    print("DONE Calculating default values for ",uCount, " records!")
    

    logging.info('Updating the AGO Feature Layer with the summary data...')
    #Then update the Feature Layer with the summary data for matching records
    uCount = 0   #reset counter
    for UNIT_ID in overlap_rows[fl_sum_fld]:
        try:
            summ_feature = [f for f in summ_zones_features if f.attributes[fl_sum_fld] == UNIT_ID][0]  #get the spatial feature
            summary_row = df_unit_count.loc[df_unit_count[df_sum_fld] == UNIT_ID]#,['Sample_Count']] #get the dataframe summary row
            summary_val = summary_row['Sample_Count'].values[0]  #get the sample count value
            #print(summary_val)
            summ_feature.attributes['CWD_SAMPLE_COUNT'] = str(summary_val)
            #summ_feature.attributes['GIS_LOAD_VERSION_DATE'] = timenow_rounded #current_datetime_str
            summ_zones_lyr.edit_features(updates=[summ_feature])
            #print(f"Updated {summ_feature.attributes[fl_sum_fld]} sample count to {summary_val} at {timenow_rounded}", flush=True)
            uCount = uCount + 1
        except Exception as e:
            print(f"Could not update {UNIT_ID}. Exception: {e}")
            continue

    #print("\nDONE updating ",uCount, " records!")
    logging.info(f'DONE updating the AGO Feature Layer with the summary data for:  {uCount} records.')



def save_web_results (df_wh, s3_client, bucket_name, folder, current_datetime_str):
    """
    Saves an xls containing information for the CWD webpage to publish test results.
    """

    # delete existing XLS files in the to_Web folder
    try:
        # collect XLS file keys to delete
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        if 'Contents' in response:
            xls_keys = [{'Key': obj['Key']} for obj in response['Contents'] if obj['Key'].endswith('.xlsx')]
            #delete the files if there are any to remove
            if xls_keys:
                logging.info(f".. Existing files to be deleted: {xls_keys}")
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': xls_keys})
                logging.info(f"..deleted existing files in folder: {folder}")
            else:
                logging.info("..no existing files found in the folder.")
        else:
            logging.info("..no files found in the folder.")
    except Exception as e:
        logging.error(f"..an error occurred while deleting existing files: {e}")


    #filter rows to include in the webpage
    incld_sts= ['Pending', 'Negative', 'Unsuitable Tissue', 'Not Tested','pending', 'negative', 'Unsuitable tissue', 'Not tested','unsuitable tissue', 'not tested']
    df_wb = df_wh[
        #(df_wh['CWD_SAMPLED_IND'] == 'Yes') & #Discussed with Shari to include all sampling results, even if not tested.
        (df_wh['SAMPLED_DATE'] >= pd.Timestamp('2024-08-01')) & 
        (df_wh['CWD_TEST_STATUS'].isin(incld_sts))
    ]

    #filter columns to include in the webpage
    df_wb= df_wb[['CWD_EAR_CARD_ID',
                  'DROPOFF_LOCATION',
                  'SPECIES',
                  'SEX',
                  'WMU',
                  'MORTALITY_DATE',
                  'SAMPLED_DATE',
                  'CWD_TEST_STATUS']]
                  #'GIS_LOAD_VERSION_DATE']]  
    
    #print(df_wb.dtypes)
    #print (df_wb)

    #convert the 'DATE' columns to only show the date part as long  #e.g. September 1, 2024
    for col in ['MORTALITY_DATE','SAMPLED_DATE']:  #df_wb.columns:  #do not use for GIS_LOAD_VERSION_DATE
        df_wb[col] = pd.to_datetime(df_wb[col]).dt.strftime('%B %d, %Y')  #(fyi - this converts the column to a string type vs date!)

    #fill blank values with 'Not Recorded'
    df_wb = df_wb.fillna('Not recorded')
    df_wb = df_wb.replace('', 'Not recorded')

    #Sort by CWD_EAR_CARD_ID
    df_wb = df_wb.sort_values(by=['CWD_EAR_CARD_ID'])

    #rename the columns
    df_wb = df_wb.rename(columns={
        'CWD_EAR_CARD_ID': 'CWD Ear Card',
        'DROPOFF_LOCATION': 'Drop-off Location',
        'SPECIES': 'Species',
        'SEX': 'Sex',
        'WMU': 'Management Unit',
        'MORTALITY_DATE': 'Mortality Date',
        'SAMPLED_DATE': 'Sample Date',
        'CWD_TEST_STATUS': 'CWD Status'
    })

    

    #File is named with the GIS_LOAD_VERSION_DATE (aka current_datetime string)
    file_key = f"{folder}cwd_sampling_results_for_public_web_{current_datetime_str}.xlsx"

    #convert to xls
    try:
        save_xlsx_to_os(s3_client, bucket_name, df_wb, file_key)
        logging.info(f'..public xlsx successfully saved to bucket {bucket_name}')
    except Exception as e:
        logging.info(f"..an error occurred: {e}")



def save_lab_submission (df_wh, s3_client, bucket_name, folder):
    """
    Saves multiple xlss, each containing information for a specific lab submission.
    """

    # delete existing XLS files in the to_Lab folder
    try:
        # collect XLS file keys to delete
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        if 'Contents' in response:
            xls_keys = [{'Key': obj['Key']} for obj in response['Contents'] if obj['Key'].endswith('.xlsx')]
            #delete the files if there are any to remove
            if xls_keys:
                logging.info(f".. Existing files to be deleted: {xls_keys}")
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': xls_keys})
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
                save_xlsx_to_os(s3_client, bucket_name, group_df, file_key)
                logging.info(f'..XLSX for submission {submission_id} successfully saved to {file_key}')
            except Exception as e:
                logging.error(f"..an error occurred while saving {file_key}: {e}")

def backup_master_dataset(s3_client, bucket_name):
    """
    Creates a backup of the Master dataset
    """
    pacific_timezone = pytz.timezone('America/Vancouver')
    yesterday = datetime.now(pacific_timezone) - timedelta(days=1)
    dytm = yesterday.strftime("%Y%m%d")
    source_file_path = 'master_dataset/cwd_master_dataset_sampling_w_hunter.xlsx'
    destination_file_path = f'master_dataset/backups/{dytm}_cwd_master_dataset_sampling_w_hunter.xlsx'
    
    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_file_path},
            Key=destination_file_path
        )
        logging.info("..old master dataset backed-up successfully")
    except Exception as e:
        logging.info(f"..an error occurred: {e}")


def publish_feature_layer(gis, df, latcol, longcol, title, folder):
    """
    Publishes the master dataset to AGO, overwriting if it already exists.

    CHECK:  why are numeric types in data dictionary being replaced with string types? Is
    this related to filling with length 25 by default?  Or does a blank feature template have to 
    be created first if string types cannot be converted to numeric types?
    """
    # Cleanup the master dataset before publishing
    df = df.dropna(subset=[latcol, longcol])
    df = df.astype(str)

    # Drop personal info fields from the dataset
    drop_cols = ['SUBMITTER_FIRST_NAME', 'SUBMITTER_LAST_NAME', 'SUBMITTER_PHONE', 'FWID']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])


    # Define Pacific timezone
    pacific_timezone = pytz.timezone('America/Vancouver')

    # Convert DATE fields to datetime, ensure they are timezone-aware
    for col in df.columns:
        if 'DATE' in col:
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
                break

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


def retrieve_field_properties (s3_client, bucket_name):
    """
    Constructs dictionaries containing field properties.
    """
    prefix= 'incoming_from_idir/data_dictionary/'

    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']

    # Filter to only include the latest Data dictionnary file
    xlsx_files = [obj for obj in objects if obj['Key'].endswith('.xlsx')]
    latest_file = max(xlsx_files, key=lambda x: x['LastModified'])

    latest_file_key = latest_file['Key']
    obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
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

    fprop_dict = df_fprop.set_index('GIS_FIELD_NAME').T.to_dict()

    #print(f"\nDomains Dictionary: {domains_dict}")
    #print(f"\nField Properties Dictionary: {fprop_dict}")

    return domains_dict, fprop_dict


def apply_field_properties(gis, title, domains_dict, fprop_dict):
    """Applies Field properities to the published Feature Layer

        NOTE:  
        28-Feb-2025     This function is not working as expected.  The field properties are not being updated.
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


def test_save_spatial_files(df, s3_client, bucket_name):
    """
    TEST Saves spatial files of the master datasets in object storage
    """
    latcol='MAP_LATITUDE'
    loncol= 'MAP_LONGITUDE'

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
        s3_client.put_object(Bucket=bucket_name, Key=s3_kml_key, Body=kml_buffer, 
                             ContentType='application/vnd.google-earth.kml+xml')
        
        logging.info(f"..KML file saved to {bucket_name}/{s3_kml_key}")
        
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
        s3_client.put_object(Bucket=bucket_name, Key=s3_shapefile_key, Body=shapefile_buffer, 
                             ContentType='application/zip')

        logging.info(f"..shapefile saved to {bucket_name}/{s3_shapefile_key}")

    except Exception as e:
        logging.error(f"..failed to save or upload the shapefile: {e}")       


#---------------------------------------------------------------------------------------------------
#  MAIN
#---------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    start_t = timeit.default_timer() #start time

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    logging.info('Connecting to Object Storage')
    S3_ENDPOINT = os.getenv('S3_ENDPOINT')
    S3_CWD_ACCESS_KEY = os.getenv('S3_CWD_ACCESS_KEY')
    S3_CWD_SECRET_KEY = os.getenv('S3_CWD_SECRET_KEY')
    s3_client = connect_to_os(S3_ENDPOINT, S3_CWD_ACCESS_KEY, S3_CWD_SECRET_KEY)

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

    
    if s3_client:
        logging.info('\nRetrieving Incoming Data from Object Storage')
        df = get_incoming_data_from_os(s3_client)
        
        logging.info('\nRetrieving Centroid CSV Lookup Tables from Object Storage')
        df_rg, df_mu= get_lookup_tables_from_os(s3_client, bucket_name='whcwdd')

    print(f"\n---------------- Print Statements  ----------------\n")
    
    logging.info('\nGetting Hunter Survey Data from AGOL')
    # Using Featire Layer ID is more reliable than the Name, which can be amiguous.
    #AGO_HUNTER_ITEM='CWD_Hunter_Survey_Responses'
    AGO_HUNTER_ITEM_ID = '107b4486f0674f0e8837f0745e49c194'  
    hunter_df = get_hunter_data_from_ago(AGO_HUNTER_ITEM_ID)
    
    logging.info('\nGetting MU and Region data from AGOL Feature Layers')
    mu_flayer_lyr, mu_flayer_fset, mu_features, mu_flayer_sdf = get_ago_flayer('0b81e46151184058a88c701853e09577')
    rg_flayer_lyr, rg_flayer_fset, rg_features, rg_flayer_sdf = get_ago_flayer('118379ce29f94faeaa724d2055ea235c')

    
    logging.info('\nProcessing the Master Sampling dataset')
    df, current_datetime_str, timenow_rounded = process_master_dataset (df)

    logging.info('\nAdding hunter survey (hs) data to Master sampling dataset')
    df_wh, hs_merged_df, flagged_hs_df = hunter_qa_and_updates_to_master(df, hunter_df)

    
    logging.info('\nAdding new hunter survey QA flags to master xls tracking list')
    updated_tracking_df = update_hs_review_tracking_list(flagged_hs_df)

    logging.info('\nUpdating the AGO Hunter Survey Feature Layer with the latest QA flags')
    update_hunter_flags_to_ago(hs_merged_df, updated_tracking_df, AGO_HUNTER_ITEM_ID)


    logging.info('\nFinding and saving duplicate CWD_EAR_CARD_IDs and WLH_IDs in Hunter data and Sampling data. ')
    find_and_save_duplicates(hs_merged_df,'HUNTER_CWD_EAR_CARD_ID', f'qa/duplicate_ids/cwd_hunter_survey_ear_card_id_duplicates.xlsx')
    find_and_save_duplicates(df_wh,'CWD_EAR_CARD_ID', f'qa/duplicate_ids/cwd_master_sampling_ear_card_id_duplicates.xlsx')
    find_and_save_duplicates(df_wh,'WLH_ID', f'qa/duplicate_ids/cwd_master_sampling_wh_id_duplicates.xlsx')


    logging.info('\nChecking for mismatches between entered MU and Region and spatial location')
    df_wh, mu_reg_flagged = check_point_within_mu_region(df_wh, mu_flayer_sdf, rg_flayer_sdf, 'MAP_LONGITUDE', 'MAP_LATITUDE')

    logging.info('\nAdding new MU REGION QA flags to master xls tracking list')
    update_sampling_mu_reg_review_tracking_list(mu_reg_flagged)

    logging.info(f'\nSummarizing sampling data by spatial units WMU and Environment Region.' )
    sampled_summary_by_unit('WMU', 'WILDLIFE_MGMT_UNIT_ID', mu_flayer_lyr, mu_features, mu_flayer_sdf)
    sampled_summary_by_unit('ENV_REGION_NAME', 'REGION_NAME', rg_flayer_lyr, rg_features, rg_flayer_sdf)


    logging.info('\nInitiating .... Saving an XLSX for the webpage')
    # #bucket_name_bbx = 'whcwddbcbox' # this points to BCBO#
    # #folder = 'Web/'       # this points to BCBOX
    # #save_web_results (df_wh, s3_client, bucket_name_bbx, folder, current_datetime_str)

    bucket_name= 'whcwdd' # this points to BGeoDrive
    folder= 'share_web/' # this points to GeoDrive
    save_web_results (df_wh, s3_client, bucket_name, folder, current_datetime_str)
    

    logging.info('\nInitiating .... Saving an XLSX for lab testing')
    # bucket_name_bbx= 'whcwddbcbox' # this points to BCBOX
    # folder_bbx= 'Lab/to_Lab/' # this points to BCBOX
    # save_lab_submission (df_wh, s3_client, bucket_name_bbx, folder_bbx)

    bucket_name= 'whcwdd' # this points to GeoDrive
    folder= 'share_labs/for_lab/' # this points to GeoDrive
    save_lab_submission (df_wh, s3_client, bucket_name, folder)
    
    logging.info('\nSaving the Master Dataset')
    bucket_name='whcwdd'
    backup_master_dataset(s3_client, bucket_name) #backup
    ##save_xlsx_to_os(s3_client, 'whcwdd', df, 'master_dataset/cwd_master_dataset_sampling.xlsx') #just lab data - used for initial testing
    save_xlsx_to_os(s3_client, 'whcwdd', df_wh, 'master_dataset/cwd_master_dataset_sampling_w_hunter.xlsx') #lab + hunter data

    
    #TESTING
    #logging.info('\nSaving spatial data')
    #test_save_spatial_files(df_wh, s3_client, bucket_name)
    
    
    logging.info('\nPublishing the Master Dataset to AGO')
    title='CWD_Master_dataset'
    folder='2024_CWD'
    latcol='MAP_LATITUDE'
    longcol= 'MAP_LONGITUDE'
    published_item= publish_feature_layer(gis, df_wh, latcol, longcol, title, folder)
    
    logging.info('\nApplying field properties to the Feature Layer')
    bucket_name= 'whcwdd'
    domains_dict, fprop_dict= retrieve_field_properties(s3_client, bucket_name)
    apply_field_properties (gis, title, domains_dict, fprop_dict)
    

    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    logging.info('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))