#-------------------------------------------------------------------------------
# Name:        Chronic Wasting Disease (CWD) Data Workflow
#
# Purpose:     This script automates the CWD data workflow. It combines
#              incoming data saved in Object Storage and exports a master dataset.
#              
# Input(s):    (1) Object Storage credentials.         
#
# Author:      Moez Labiadh - GeoBC
#
# Created:     2024-07-05
# Updated:     
#-------------------------------------------------------------------------------

import os
import re
import boto3
import botocore
import pandas as pd
import numpy as np
from io import BytesIO
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
                if key.endswith('.xlsx') and 'cwd_lab_submissions' in key.lower() and 'sample_collection' in key.lower():
                    try:
                        logging.info(f"...reading file: {key}")
                        obj_data = s3_client.get_object(Bucket=bucket_name, Key=key)
                        df = pd.read_excel(BytesIO(obj_data['Body'].read()), 
                                           sheet_name='Sampling Sheet')
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
    

def process_master_dataset(df):
    """
    Populates missing Latitude and Longitude values
    Fromat Datetime columns
    """
    logging.info("..formatting columns ")
    df['Latitude (DD)'] = pd.to_numeric(df['Latitude (DD)'], errors='coerce')
    df['Longitude (DD)'] = pd.to_numeric(df['Longitude (DD)'], errors='coerce')
    
    def set_source_value(row):
        if pd.notna(row['Longitude (DD)']) and pd.notna(row['Latitude (DD)']):
            if 47.0 <= row['Latitude (DD)'] <= 60.0 and -145.0 <= row['Longitude (DD)'] <= -113.0:
                return 'Entered by User'
            else:
                return 'Incorrectly entered by user'
        return np.nan

    df['LatLong Source'] = df.apply(set_source_value, axis=1)
    
    df['LatLong Accuracy'] = None
    
    columns = list(df.columns)
    longitude_index = columns.index('Longitude (DD)')
    columns.remove('LatLong Source')
    columns.remove('LatLong Accuracy')
    columns.insert(longitude_index + 1, 'LatLong Source')
    columns.insert(longitude_index + 2, 'LatLong Accuracy')
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
    
    df['MU'] = df['MU'].apply(correct_mu_value)
    
    logging.info("..retrieving latlon from MU centroids")
    #populate lat/long from MU centroid
    def latlong_from_MU(row, df_mu):
        if (pd.isnull(row['LatLong Source']) or row['LatLong Source'] == 'Incorrectly entered by user') and row['MU'] != 'Not Recorded':
            mu_value = row['MU']
            match = df_mu[df_mu['MU'] == mu_value]
            if not match.empty:
                row['Latitude (DD)'] = match['CENTER_LAT'].values[0]
                row['Longitude (DD)'] = match['CENTER_LONG'].values[0]
                row['LatLong Source'] = 'From MU'
        return row
    
    df = df.apply(lambda row: latlong_from_MU(row, df_mu), axis=1)
    
    logging.info("..retrieving latlon from Region centroids")
    #populate lat/long from Region centroid
    def latlong_from_Region(row, df_rg):
        if (pd.isnull(row['LatLong Source']) or row['LatLong Source'] == 'Incorrectly entered by user') and row['Region'] != 'Not Recorded':
            rg_value = row['Region']
            match = df_rg[df_rg['REGION'] == rg_value]
            if not match.empty:
                row['Latitude (DD)'] = match['CENTER_LAT'].values[0]
                row['Longitude (DD)'] = match['CENTER_LONG'].values[0]
                row['LatLong Source'] = 'From Region'
        return row
    
    df = df.apply(lambda row: latlong_from_Region(row, df_rg), axis=1)
    
    df.loc[df['LatLong Source'] == 'Entered by User', 'LatLong Accuracy'] = 'Exact'
    df.loc[df['LatLong Source'].isin(['From MU', 'From Region']), 'LatLong Accuracy'] = 'Estimate'
    
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
        
            
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    logging.info('Connecting to Object Storage')
    S3_ENDPOINT = os.getenv('S3_ENDPOINT')
    S3_CWD_ACCESS_KEY = os.getenv('S3_CWD_ACCESS_KEY')
    S3_CWD_SECRET_KEY = os.getenv('S3_CWD_SECRET_KEY')
    s3_client = connect_to_os(S3_ENDPOINT, S3_CWD_ACCESS_KEY, S3_CWD_SECRET_KEY)
    
    if s3_client:
        logging.info('\nRetrieving Incoming Data from Object Storage')
        df = get_incoming_data_from_os(s3_client)
        
        logging.info('\nRetrieving Lookup Tables from Object Storage')
        df_rg, df_mu= get_lookup_tables_from_os(s3_client, bucket_name='whcwdd')
    
        '''
    logging.info('\nProcessing the master dataset')
    df= process_master_dataset (df)
    
    df_test= df.loc[df['CWD Ear Card']==19296]
    
    logging.info('\nSaving a Master Dataset')
    dytm = datetime.now().strftime("%Y%m%d_%H%M")
    save_xlsx_to_os(s3_client, 'whcwdd', df, 'master_dataset/cwd_master_dataset.xlsx') #main dataset
    save_xlsx_to_os(s3_client, 'whcwdd', df, f'master_dataset/backups/{dytm}_cwd_master_dataset.xlsx') #backup
    '''