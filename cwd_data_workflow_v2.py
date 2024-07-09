#-------------------------------------------------------------------------------
# Name:        Chronic Wasting Disease (CWD) Data Workflow
#
# Purpose:     This script automates the CWD data workflow. It combines
#              incoming data saved in Object Storage and exports a master dataset.
#              
# Input(s):    (1) S3 Object Storage credentials. 
#              (2) AGO ArcGIS Online credentials.          
#
# Author:      Moez Labiadh - GeoBC
#
# Created:     2024-07-09
# Updated:     
#-------------------------------------------------------------------------------

import os
import re
import json
import requests
import boto3
import botocore
import pandas as pd
import numpy as np
from io import BytesIO
import geopandas as gpd
from shapely.geometry import Point

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
                if key.startswith('incoming_from_idir') and key.endswith('.xlsx') and 'test_incoming' in key.lower():
                    try:
                        logging.info(f"...reading file: {key}")
                        obj_data = s3_client.get_object(Bucket=bucket_name, Key=key)
                        df = pd.read_excel(BytesIO(obj_data['Body'].read()), 
                                           sheet_name='Sampling')
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
    

def populate_missing_latlong(df):
    """
    Populates missing Latitude and Longitude values
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
        

def df_to_gdf(df, lat_col, lon_col, crs=4326):
    """
    Converts a DataFrame with latitude and longitude columns to a GeoDataFrame
    """
    geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf.set_crs(epsg=crs, inplace=True)  
        
    #convert datetime cols to str: needed to publish to AGO later on
    for column in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[column]):
            gdf[column] = gdf[column].astype(str)
    
    return gdf

def export_gdf_to_shapefile (gdf, s3_client, shapefile_name, bucket_name='whcwdd', subfolder='spatial'):
    """
    Exports a GeoDataFrame to a shapefile and uploads it to object storage.
    """
    try:
        # Write GeoDataFrame to a temporary file-like object
        with gpd.io.file.fiona_env():
            with BytesIO() as fileobj:
                gdf.to_file(fileobj, driver='ESRI Shapefile')
                fileobj.seek(0)
                
                # Upload shapefile to object storage
                s3_key = f'{subfolder}/{shapefile_name}.zip'
                s3_client.upload_fileobj(fileobj, bucket_name, s3_key)
        
        logging.info(f'..shapefile uploaded successfully to {bucket_name}/{subfolder}')
        return True
    
    except Exception as e:
        logging.error(f'...failed to export or upload shapefile: {e}')
        return False


def prepare_df_for_ago(df):
    """
    Cleans up df column names and coordinates for AGO.
    """
    df_ago= df.copy()
    
    def clean_name(name):
        # Remove specified special characters
        for char in ['(', ')', '#', '-', ' ', "'"]:
            name = name.replace(char, '')
        return name
    
    # Apply the cleaning function to all column names
    df_ago.columns = [clean_name(col) for col in df_ago.columns]
    
    # Drop rows with NaN values in LatLongSource column
    df_ago = df_ago.dropna(subset=['LatLongSource'])
    
    return df_ago


def get_ago_token(TOKEN_URL, HOST, USERNAME, PASSWORD):
    """
    Returns an access token to AGOL account
    """
    params = {
        'username': USERNAME,
        'password': PASSWORD,
        'referer': HOST,
        'f': 'json'
    }
    
    try:
        # Send request to get token
        response = requests.post(TOKEN_URL, data=params, verify=True) # Enable SSL verification

        # Check response status
        response.raise_for_status()

        logging.info("...successfully obtained AGO access token.")
        
        return response.json().get('token')
    
    except requests.exceptions.RequestException as e:
        logging.error(f"...failed to obtain access token: {e}")
        raise
        
        
def get_ago_folderID(token, username, folder_name):
    """
    Get or create a folder in ArcGIS Online.
    """
    folders_url = f"https://www.arcgis.com/sharing/rest/content/users/{username}"
    params = {
        'f': 'json',
        'token': token,
    }

    try:
        # Check if the folder exists
        response = requests.post(folders_url, data=params, verify=True)
        response.raise_for_status()
        folders = response.json().get('folders', [])
        
        for folder in folders:
            if folder['title'] == folder_name:
                logging.info(f"...folder '{folder_name}' already exists.")
                return folder['id']
        
        # Create the folder if it does not exist
        create_folder_url = f"{folders_url}/createFolder"
        create_params = {
            'f': 'json',
            'title': folder_name,
            'token': token
        }
        
        create_response = requests.post(create_folder_url, data=create_params, verify=True)
        create_response.raise_for_status()
        
        logging.info(f"...folder '{folder_name}' created successfully.")
        return create_response.json().get('folder').get('id')
    
    except requests.exceptions.RequestException as e:
        logging.error(f"...failed to create or get folder: {e}")
        raise
            

def create_feature_service(token, username, folder_id, service_name):
    """
    Creates a Feature Service in ArcGIS online
    """
    params = {
        'f': 'json',
        'token': token,
        'createParameters': json.dumps({
            'name': service_name,
            'serviceDescription': '',
            'hasStaticData': False,
            'maxRecordCount': 1000,
            'supportedQueryFormats': 'JSON',
            'capabilities': 'Create,Delete,Query,Update,Editing',
            'initialExtent': {
                'xmin': -180,
                'ymin': -90,
                'xmax': 180,
                'ymax': 90,
                'spatialReference': {'wkid': 4326}
            },
            'allowGeometryUpdates': True,
            'units': 'esriDecimalDegrees',
            'xssPreventionInfo': {'xssPreventionEnabled': True, 'xssPreventionRule': 'InputOnly', 'xssInputRule': 'rejectInvalid'}
        }),
        'tags': 'feature service',
        'title': service_name
    }
    CREATE_SERVICE_URL = f'https://www.arcgis.com/sharing/rest/content/users/{username}/{folder_id}/createService'
    response = requests.post(CREATE_SERVICE_URL, data=params)
    response_json = response.json()
    if 'serviceItemId' in response_json and 'encodedServiceURL' in response_json:
        service_id = response_json['serviceItemId']
        service_url = response_json['encodedServiceURL']
        admin_url = service_url.replace('/rest/', '/rest/admin/')
        logging.info(f"...feature service created successfully with ID: {service_id}")
        return service_id, admin_url
    else:
        raise Exception(f"Error creating feature service: {response_json}")


def add_layer_to_service(token, admin_url, df, latcol, longcol):
    """
    Adds a Point Layer to the Feature Service
    """
    add_to_definition_url = f"{admin_url}/addToDefinition"

    fields = [{
        "name": "ObjectID",
        "type": "esriFieldTypeOID",
        "alias": "ObjectID",
        "sqlType": "sqlTypeOther",
        "nullable": False,
        "editable": False,
        "domain": None,
        "defaultValue": None
    }]
    
    for col in df.columns:
        fields.append({
            "name": col,
            "type": "esriFieldTypeString",  # Assuming all fields are string
            "alias": col,
            "sqlType": "sqlTypeOther",
            "nullable": True,
            "editable": True,
            "domain": None,
            "defaultValue": None
        })

    # Calculate extent from the lat/long columns
    xmin = df[longcol].min()
    ymin = df[latcol].min()
    xmax = df[longcol].max()
    ymax = df[latcol].max()

    layer_definition = {
        "layers": [
            {
                "name": "Points",
                "type": "Feature Layer",
                "geometryType": "esriGeometryPoint",
                "fields": fields,
                "extent": {
                    "xmin": xmin,
                    "ymin": ymin,
                    "xmax": xmax,
                    "ymax": ymax,
                    "spatialReference": {"wkid": 4326}
                }
            }
        ]
    }

    params = {
        'f': 'json',
        'token': token,
        'addToDefinition': json.dumps(layer_definition)
    }
    response = requests.post(add_to_definition_url, data=params)

    response_json = response.json()
    if 'success' in response_json and response_json['success']:
        logging.info("...layer added to the feature service successfully")
        return True
    else:
        raise Exception(f"Error adding layer to feature service: {response_json}")
        
        
def add_features(token, service_url, df, latcol, longcol):
    """
    Adds data (features) to the Feature Service Layer
    """
    add_features_url = f"{service_url}/0/addFeatures"
    features = []
    for index, row in df.iterrows():
        try:
            attributes = {}
            for col in df.columns:
                value = row[col]
                if isinstance(value, datetime):
                    value = value.isoformat()
                elif pd.isna(value):
                    value = None
                attributes[col] = value

            feature = {
                'geometry': {
                    'x': float(row[longcol]),
                    'y': float(row[latcol]),
                    'spatialReference': {'wkid': 4326}
                },
                'attributes': attributes
            }
            features.append(feature)
        except Exception as e:
            logging.error(f"Error processing row {index}: {e}")

    if not features:
        logging.error("No valid features to add")
        return []

    params = {
        'f': 'json',
        'token': token,
        'features': json.dumps(features)
    }
    
    try:
        response = requests.post(add_features_url, data=params)
        response.raise_for_status()
        response_json = response.json()
        
        if 'addResults' in response_json:
            successful_adds = sum(1 for result in response_json['addResults'] if result.get('success', False))
            logging.info(f"...{successful_adds} out of {len(features)} features added successfully")
            return response_json['addResults']
        else:
            logging.error(f"Error adding features: {response_json}")
            return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return []
        
        
        
        
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
        
    logging.info('\nPopulating missing latlon values')
    df= populate_missing_latlong (df)
    
    logging.info('\nSaving a Master Dataset')
    dytm = datetime.now().strftime("%Y%m%d_%H%M")
    save_xlsx_to_os(s3_client, 'whcwdd', df, 'master_dataset/cwd_master_dataset.xlsx') #main dataset
    save_xlsx_to_os(s3_client, 'whcwdd', df, f'master_dataset/backups/{dytm}_cwd_master_dataset.xlsx') #backup

    logging.info('\nPrepare the Dataframe for AGO publishing')
    df_ago= prepare_df_for_ago(df)
    

    logging.info('\nPublishing the Master Dataset to AGO')
    logging.info('..connecting to AGO')
    AGO_TOKEN_URL = os.getenv('AGO_TOKEN_URL')
    AGO_HOST = os.getenv('AGO_HOST')
    AGO_USERNAME = os.getenv('AGO_USERNAME')
    AGO_PASSWORD = os.getenv('AGO_PASSWORD')
    AGO_ACCOUNT_ID = os.getenv('AGO_ACCOUNT_ID')
    
    token = get_ago_token(AGO_TOKEN_URL, AGO_HOST, AGO_USERNAME, AGO_PASSWORD)
    
    logging.info('..getting AGO folder ID')
    folder_name= "Hackathon_2024_Survey123"
    folderID= get_ago_folderID(token, AGO_USERNAME, folder_name)
    

    logging.info('..creating a Feature Service')
    service_name= "CWD_Master_dataset"
    service_id, admin_url = create_feature_service(token, AGO_USERNAME, folderID, service_name)
    
    logging.info('..adding a layer to the Feature Service')
    layer_added = add_layer_to_service(token, admin_url, df_ago, 'LatitudeDD', 'LongitudeDD')
    
    logging.info('..loading data to the Feature Service Layer')
    service_url = admin_url.replace('/admin/', '/')
    add_features_response = add_features(token, service_url, df_ago, 'LatitudeDD', 'LongitudeDD')
    
    logging.info('\nCreating a GeoDataframe')
    gdf= df_to_gdf(df_ago, 'LatitudeDD', 'LongitudeDD', crs=4326)  
    
    logging.info('\nExporting to shapefile')
    export_gdf_to_shapefile (gdf, s3_client, 'CWD_Master_dataset', bucket_name='whcwdd', subfolder='spatial')
