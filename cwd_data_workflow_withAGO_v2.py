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
import json
import pandas as pd
import geopandas as gpd
import numpy as np
from io import BytesIO
from arcgis.gis import GIS

import logging
import timeit
from datetime import datetime


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
    
    logging.info("..retrieving latlon from MU centroid")
    #populate lat/long MU centroid centroid
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
    
    logging.info("..retrieving latlon from Region centroids")
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

    # Add the 'GIS_LOAD_VERSION_DATE' column with the current date and timestamp
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['GIS_LOAD_VERSION_DATE'] = current_datetime
    
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
        logging.info(f'..data successfully saved {file_name} to bucket {bucket_name}')
    except botocore.exceptions.ClientError as e:
        logging.error(f'..failed to save data to Object Storage: {e.response["Error"]["Message"]}')
        

def backup_master_dataset(s3_client, bucket_name):
    """
    Creates a backup of the Master dataset
    """
    dytm = datetime.now().strftime("%Y%m%d_%H%M")
    source_file_path = 'master_dataset/cwd_master_dataset.xlsx'
    destination_file_path = f'master_dataset/backups/{dytm}_cwd_master_dataset.xlsx'
    
    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_file_path},
            Key=destination_file_path
        )
        print(f"..old master dataset backed-up successfully")
    except Exception as e:
        print(f"..an error occurred: {e}")


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


def publish_feature_layer(df, title='TEST_FL', folder='2024_CWD'):
    """
    Publishes the master dataset to AGO, overwriting if it already exists.
    """
    # Cleanup the master dataset before publishing
    df = df.dropna(subset=['LATITUDE_DD', 'LONGITUDE_DD'])

    # Drop personal info fields from the dataset
    drop_cols = ['SUBMITTER_FIRST_NAME', 'SUBMITTER_LAST_NAME', 'SUBMITTER_PHONE', 'FWID']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    # Convert DATE fields to datetime and coerce errors to NaT
    for col in df.columns:
        if 'DATE' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Fill NaN and NaT values
    df = df.fillna('')  

    # Create a spatial dataframe
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LONGITUDE_DD, df.LATITUDE_DD), crs="EPSG:4326")

    # Convert Timestamp columns to string format
    for col in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[col]):
            gdf[col] = gdf[col].apply(lambda x: x.isoformat() if not pd.isna(x) else '')

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
    geojson_file = BytesIO(json.dumps(geojson_dict).encode('utf-8'))

    # Create a dictionary representing the GeoJSON item properties
    geojson_item_properties = {
        'title': title,
        'type': 'GeoJson',
        'tags': 'sampling points,geojson',
        'description': 'CWD sampling data',
        'fileName': 'data.geojson'
    }

    try:
        # Search for existing items (including the GeoJSON file)
        existing_items = gis.content.search(f"(title:{title} OR title:data.geojson) AND owner:{gis.users.me.username}")

        # Delete all existing items
        for item in existing_items:
            item.delete(force=True, permanent=True)
            print(f"..existing item '{item.title}' permanently deleted.")
        
        # Wait for a short time to ensure deletion is processed
        import time
        time.sleep(10)
 
        # Create a new item
        new_item = gis.content.add(item_properties=geojson_item_properties, data=geojson_file, folder=folder)
        published_item = new_item.publish()
        print(f"..new feature layer '{title}' published successfully.")
        return published_item

    except Exception as e:
        error_message = f"..error publishing/updating feature layer: {str(e)}"
        print(error_message)
        raise RuntimeError(error_message)


def retrieve_field_properties (s3_client, bucket_name='whcwdd'):
    """
    Constructs a dictionnaries containing field properties (domains, length and value type)
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

    #Creating domains dictionnary
    df_domfields = df_datadict[['Domain Name','GIS_FIELD_NAME']].dropna(subset=['Domain Name'])
    field_match_dict = df_domfields.set_index('Domain Name')['GIS_FIELD_NAME'].to_dict()

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

    #Creating field length and type dictionnary
    df_fprop= df_datadict[['GIS_FIELD_NAME', 'Type', 'Length']]
    df_fprop = df_fprop.replace(["n/a", "N/A", ""], None)
    df_fprop['Length'] = df_fprop['Length'].fillna(25)
    df_fprop['Length'] = df_fprop['Length'].astype(int)

    # Mapping from custom types to ArcGIS field types
    type_mapping = {
        'TEXT': 'esriFieldTypeString',
        'DATEONLY': 'esriFieldTypeDate',  # Date and Time
        'DATE': 'esriFieldTypeDate',      # Date and Time
        'LONG': 'esriFieldTypeInteger',
        'SHORT': 'esriFieldTypeSmallInteger',
        'FLOAT': 'esriFieldTypeSingle',
        'DOUBLE': 'esriFieldTypeDouble'
    }
    df_fprop['Type'] = df_fprop['Type'].map(type_mapping)

    fprop_dict = df_fprop.set_index('GIS_FIELD_NAME').T.to_dict()

    return domains_dict, fprop_dict


def apply_field_properties(domains_dict, fprop_dict, title='TEST_FL'):
    """Applies Domains, Field Lengths and Field Types to the published Feature Layer"""
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

    # Update the field definitions
    response = feature_layer.manager.update_definition({
        "fields": fields
    })


if __name__ == "__main__":
    start_t = timeit.default_timer() #start time

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
      
    logging.info('\nProcessing the master dataset')
    df= process_master_dataset (df)
    
    logging.info('\nSaving a Master Dataset')
    backup_master_dataset(s3_client, bucket_name='whcwdd')
    save_xlsx_to_os(s3_client, 'whcwdd', df, 'master_dataset/cwd_master_dataset.xlsx') #main dataset

    logging.info('\nConnecting to AGO')
    AGO_HOST = os.getenv('AGO_HOST')
    AGO_USERNAME = os.getenv('AGO_USERNAME')
    AGO_PASSWORD = os.getenv('AGO_PASSWORD')
    gis = connect_to_AGO(AGO_HOST, AGO_USERNAME, AGO_PASSWORD)

    logging.info('\nPublishing the Mater Dataset to AGO')
    published_item = publish_feature_layer(df, title='TEST_FL', folder='2024_CWD')

    logging.info('\nApplying field proprities to the Feature Layer')
    domains_dict, fprop_dict= retrieve_field_properties(s3_client, bucket_name='whcwdd')
    apply_field_properties (domains_dict, fprop_dict, title='TEST_FL')

    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    logging.info('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs)) 