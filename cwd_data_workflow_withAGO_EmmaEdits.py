#-------------------------------------------------------------------------------
# Name:        Chronic Wasting Disease (CWD) Data Workflow
#
# Purpose:     This script automates the CWD data workflow. It combines
#              incoming data saved in Object Storage and exports a master dataset.
#              The master datset is then published to AGOL.
#              
# Input(s):    (1) Object Storage credentials.
#              (1) AGO credentials.           
#
# Author:      Moez Labiadh - GeoBC
#
# Created:     2024-08-12
# Updated:     
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import re
import boto3
import botocore
import pandas as pd
import numpy as np
from io import BytesIO

from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from arcgis.features import FeatureLayerCollection

from datetime import datetime
import logging


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
        logging.info("..appending dataframes")
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
    
def get_hunter_data_from_ago(gis, HUNTER_ITEM_ID):
    logging.info("..getting hunter survey data from AGO")
    # get the ago item
    hunter_survey_item = gis.content.get(HUNTER_ITEM_ID)

    # get the ago feature layer
    hunter_flayer = hunter_survey_item.layers[0]

    # query the feature layer to get its data
    hunter_data = hunter_flayer.query().sdf

    # convert the feature layer data to pandas dataframe
    hunter_df = pd.DataFrame(hunter_data)

    return hunter_df

def process_master_dataset(df):
    """
    Populates missing Latitude and Longitude values
    Fromat Datetime columns
    """
    logging.info("..formatting columns ")
    df['LATITUDE_DD'] = pd.to_numeric(df['LATITUDE_DD'], errors='coerce')
    df['LONGITUDE_DD'] = pd.to_numeric(df['LONGITUDE_DD'], errors='coerce')
    
    def set_source_value(row):
        if pd.notna(row['LATITUDE_DD']) and pd.notna(row['LONGITUDE_DD']):
            if 47.0 <= row['LATITUDE_DD'] <= 60.0 and -145.0 <= row['LONGITUDE_DD'] <= -113.0:
                return 'Entered by User'
            else:
                return 'Incorrectly entered by user'
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
    
    logging.info("..retrieving latlon from MU centroid")
    #populate lat/long MU centroid centroid
    def latlong_from_MU(row, df_mu):
        if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR']) or row['SPATIAL_CAPTURE_DESCRIPTOR'] == 'Incorrectly entered by user') and row['WMU'] != 'Not Recorded':
            mu_value = row['WMU']
            match = df_mu[df_mu['MU'] == mu_value]
            if not match.empty:
                row['LATITUDE_DD'] = match['CENTER_LAT'].values[0]
                row['LONGITUDE_DD'] = match['CENTER_LONG'].values[0]
                row['SPATIAL_CAPTURE_DESCRIPTOR'] = 'MU centroid'
        return row
    
    df = df.apply(lambda row: latlong_from_MU(row, df_mu), axis=1)
    
    logging.info("..retrieving latlon from Region centroids")
    #populate lat/long Region centroid centroid
    def latlong_from_Region(row, df_rg):
        if (pd.isnull(row['SPATIAL_CAPTURE_DESCRIPTOR']) or row['SPATIAL_CAPTURE_DESCRIPTOR'] == 'Incorrectly entered by user') and row['ENV_REGION_NAME'] != 'Not Recorded':
            rg_value = row['ENV_REGION_NAME']
            match = df_rg[df_rg['REGION'] == rg_value]
            if not match.empty:
                row['LATITUDE_DD'] = match['CENTER_LAT'].values[0]
                row['LONGITUDE_DD'] = match['CENTER_LONG'].values[0]
                row['SPATIAL_CAPTURE_DESCRIPTOR'] = 'Region centroid'
        return row
    
    df = df.apply(lambda row: latlong_from_Region(row, df_rg), axis=1)
      
    # Add the 'GIS_LOAD_VERSION_DATE' column with the current date and timestamp
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['GIS_LOAD_VERSION_DATE'] = current_datetime
    
    # format datetime columns. STILL WORKING ON THIS. 
    '''
    for col in df.columns:
        if 'date' in col.lower():
            try:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%d %B %Y')
            except ValueError:
                pass  # Skip columns that can't be converted to datetime

    '''
    
    return df

def join_xls_and_hunter_df(df, hunter_df):
    """
    Returns the final dataframe including hunter survey responses
    """
    # convert CWD Ear Card from df to type string
    df['CWD_EAR_CARD_ID'] = df['CWD_EAR_CARD_ID'].astype('string')
    
    logging.info("..merging dataframes")
    # merge the dataframes
    combined_df = pd.merge(left=df,
                           right=hunter_df,
                           how="left",
                           left_on="CWD_EAR_CARD_ID",
                           right_on="HUNTER_CWD_EAR_CARD_ID_TEXT")
    
    # drop unnecessary columns
    logging.info("..dropping unnecessary columns")
    columns_to_drop = ['OBJECTID', 'GlobalID', 'CreationDate', 'Creator', 'EditDate', 'bc_gov_header', 'disclaimer_text', 'email_message', 'SHAPE']
    combined_df = combined_df.drop(columns_to_drop, axis=1)
    
    logging.info("..cleaning dataframes")
    # filter df for where hunters have updated data
    hunter_matches_df = combined_df[combined_df.HUNTER_SEX.notnull()].copy()

    # filter df for where hunters have not updated data
    xls_df = combined_df[combined_df.HUNTER_SEX.isnull()].copy()

    # for hunter_matches_df - update MAP_SOURCE_DESCRIPTOR w/ value = Hunter Survey
    hunter_matches_df['MAP_SOURCE_DESCRIPTOR'] = "Hunter Survey"
    # for xls_df - update MAP_SOURCE_DESCRIPTOR w/ value = Ear Card
    xls_df['MAP_SOURCE_DESCRIPTOR'] = "Ear Card"
    
    # clean up xls_df to comform with ago field requirements 
    xls_df[['HUNTER_SPECIES', 'HUNTER_SEX', 'HUNTER_MORTALITY_DATE']] = None

    # populate MAP_LATITUDE and MAP_LONGITUDE columns
    hunter_matches_df[['MAP_LATITUDE', 'MAP_LONGITUDE']] = hunter_matches_df[['HUNTER_LATITUDE_DD', 'HUNTER_LONGITUDE_DD']]
    xls_df[['MAP_LATITUDE', 'MAP_LONGITUDE']] = xls_df[['LATITUDE_DD', 'LONGITUDE_DD']]

    # re-combine dataframes
    final_df = pd.concat([hunter_matches_df, xls_df], ignore_index=True)

    return final_df
      

def save_xlsx_to_os(s3_client, bucket_name, df, file_name):
    """
    Saves an xlsx to Object Storage bucket.
    """
    xlsx_buffer = BytesIO()
    df.to_excel(xlsx_buffer, index=False)
    xlsx_buffer.seek(0)

    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=xlsx_buffer.getvalue())
        logging.info(f'..data successfully saved {file_name}to bucket {bucket_name}')
    except botocore.exceptions.ClientError as e:
        logging.error(f'..failed to save data to Object Storage: {e.response["Error"]["Message"]}')
        

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


def construct_domains_dict(s3_client, bucket_name='whcwdd'):
    """
    Constructs a dictionnary containing Domains data based on Picklists
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

    df_domfields = df_datadict[['Domain Name', 'GIS Field_Name (does not have to match WHIS)\n(in progress for use in GIS/GDB, when implemented)']].dropna(subset=['Domain Name'])
    df_domfields = df_domfields.rename(columns={'GIS Field_Name (does not have to match WHIS)\n(in progress for use in GIS/GDB, when implemented)': 'GIS Field Name'})
    field_match_dict = df_domfields.set_index('Domain Name')['GIS Field Name'].to_dict()

    df_pcklists = df_pcklists.rename(columns=field_match_dict)

    domains_dict = {} 
    # Iterate through each column (field) in the DataFrame
    for column in df_pcklists.columns:
        # Extract the field name and values
        field_name = column
        values = df_pcklists[field_name].dropna().tolist()  # Drop NaN values and convert to list

        # Create the domain dictionary for the field
        domain_values = {str(value): str(value) for value in values}
        domains_dict[field_name] = domain_values
    
    return domains_dict


def publish_feature_layer(df, domains_dict, title='TEST_FL', folder='2024_CWD'):
    """
    Publishes the master dataset to AGO and applies domains (value lists)
    """
    # Create a Spatial DataFrame
    sdf = pd.DataFrame.spatial.from_xy(df, x_column='MAP_LONGITUDE', y_column='MAP_LATITUDE')

    # Publish a feature layer
    sdf.spatial.to_featurelayer(title=title, gis=gis, folder=folder)

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
        
        
        
        
        
        
        
        
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    logging.info('Connecting to Object Storage')
    S3_ENDPOINT = os.getenv('S3_ENDPOINT')
    S3_CWD_ACCESS_KEY = os.getenv('S3_CWD_ACCESS_KEY')
    S3_CWD_SECRET_KEY = os.getenv('S3_CWD_SECRET_KEY')
    s3_client = connect_to_os(S3_ENDPOINT, S3_CWD_ACCESS_KEY, S3_CWD_SECRET_KEY)

    logging.info('\nConnecting to AGOL')
    AGO_HOST = os.getenv('AGO_HOST')
    AGO_USERNAME = os.getenv('AGO_USERNAME')
    AGO_PASSWORD = os.getenv('AGO_PASSWORD')
    gis = connect_to_AGO(AGO_HOST, AGO_USERNAME, AGO_PASSWORD)
    
    if s3_client:
        logging.info('\nRetrieving Incoming Data from Object Storage')
        df = get_incoming_data_from_os(s3_client)
        
        logging.info('\nRetrieving Lookup Tables from Object Storage')
        df_rg, df_mu= get_lookup_tables_from_os(s3_client, bucket_name='whcwdd')

    logging.info('\nGetting Hunter Survey Data from AGOL')
    HUNTER_AGO_ITEM = os.getenv('HUNTER_AGO_ITEM')
    hunter_df = get_hunter_data_from_ago(gis, HUNTER_AGO_ITEM)
        
    logging.info('\nProcessing the master dataset')
    df= process_master_dataset (df)

    logging.info('\nMerging master df w/ hunter survey df')
    final_df = join_xls_and_hunter_df(df, hunter_df)
    
    logging.info('\nSaving a Master Dataset')
    dytm = datetime.now().strftime("%Y%m%d_%H%M")
    #save_xlsx_to_os(s3_client, 'whcwdd', final_df, 'master_dataset/cwd_master_dataset.xlsx') #main dataset
    #save_xlsx_to_os(s3_client, 'whcwdd', final_df, f'master_dataset/backups/{dytm}_cwd_master_dataset.xlsx') #backup

    logging.info('\nCreating domains and field proprities')
    domains_dict= construct_domains_dict(s3_client, bucket_name='whcwdd')

    logging.info('\nPublishing the Mater Dataset to AGO')
    publish_feature_layer(final_df, domains_dict, title='TEST_FL', folder='2024_CWD')

