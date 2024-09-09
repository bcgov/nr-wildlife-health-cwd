#-------------------------------------------------------------------------------
# Name:        Update the CWD Drop-off location Feature Layer
#
# Purpose:     This script updates the drop-off locations  
#              feature layer on ArcGis Online.
#              
# Input(s):    (1) Object Storage credentials.
#              (1) AGO credentials.           
#
# Author:      Moez Labiadh - GeoBC 
#
# Created:     2024-08-20
# Updated:     
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import boto3
import botocore
import json
import pandas as pd
import numpy as np
import geopandas as gpd
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


def get_dropoff_locations(s3_client, bucket_name):
    """
    Returns a df of drop-off locations from Object Storage.
    """
    prefix = 'incoming_from_idir/point_locations/'

    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']

    # Filter to only include files that end with '.xlsx' and contain 'freezerlocations'
    xlsx_files = [obj for obj in objects if obj['Key'].endswith('.xlsx') and 'freezerlocations' in obj['Key']]
    
    # Check if there are any matching files
    if not xlsx_files:
        raise ValueError("..no files matching the criteria were found.")
    
    # Select the latest file based on the LastModified date
    latest_file = max(xlsx_files, key=lambda x: x['LastModified'])

    latest_file_key = latest_file['Key']
    obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
    data = obj['Body'].read()
    excel_file = pd.ExcelFile(BytesIO(data))

    # Read the excel into df
    df= pd.read_excel(excel_file, sheet_name='DropOff_Locations', header=2)

    # Keep only confirmed location rows
    df= df[df['CONFIRMED']=='Yes']

    # Set 'CONTACT_NAME' and 'CONTACT_INFO' to None if 'PUBLIC_CONTACT_INFO_IND' is 'No'
    df.loc[df['PUBLIC_CONTACT_INFO_IND'] == 'No', ['CONTACT_NAME', 'CONTACT_INFO']] = None

    # Drop columns
    df = df.drop(columns=['PUBLIC_CONTACT_INFO_IND', 'NOTES', 'CONFIRMED', 'ON_WEBSITE'])

    # Create a dict of field aliases and Lengths
    fprop_dict= {
            'REGION': {'Alias': 'Region', 'Length': 50},
            'LOCATION_NAME': {'Alias': 'Location Name', 'Length': 120},
            'CIVIC_ADDRESS': {'Alias': 'Civic Address', 'Length': 50},
            'CITY': {'Alias': 'City', 'Length': 40},
            'POSTAL_CODE': {'Alias': 'Postal Code', 'Length': 7},
            'Lat': {'Alias': 'Latitude'},
            'Long': {'Alias': 'Longitude'},
            'F24_7_ACCESS': {'Alias': '24/7 Access', 'Length': 5},
            'MORE_INFO': {'Alias': 'Details', 'Length': 200},
            'CONTACT_NAME': {'Alias': 'Contact Name', 'Length': 50},
            'CONTACT_INFO': {'Alias': 'Contact Information', 'Length': 40}
        }

    return df, fprop_dict


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


def publish_feature_layer(gis, df, latcol, longcol, title, folder):
    """
    Publishes the master dataset to AGO, overwriting if it already exists.
    """
    # Cleanup missing latlon
    df = df.dropna(subset=[latcol, longcol])

    # Create a spatial dataframe
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[longcol], df[latcol]), crs="EPSG:4326")

    gdf = gdf.replace(['NaN', np.nan], '')
    gdf = gdf.drop(columns=['Lat', 'Long'])

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

    try:
        # Search for existing items (including the GeoJSON file and feature layer)
        existing_items = gis.content.search(f"(title:{title} OR title:data.geojson) AND owner:{gis.users.me.username}")

        # Delete the existing GeoJSON file
        for item in existing_items:
            if item.type == 'GeoJson':
                item.delete(force=True, permanent=True)
                print(f"..existing GeoJSON item '{item.title}' permanently deleted.")

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
            'fileName': 'freezer.geojson'
        }
        geojson_file = BytesIO(json.dumps(geojson_dict).encode('utf-8'))
        new_geojson_item = gis.content.add(item_properties=geojson_item_properties, data=geojson_file, folder=folder)

        # Update the existing feature layer or create a new one if it doesn't exist
        if feature_layer_item:
            feature_layer_item.update(data=new_geojson_item, folder=folder)
            print(f"..existing feature layer '{title}' updated successfully.")
        else:
            published_item = new_geojson_item.publish(overwrite=True)
            print(f"..new feature layer '{title}' published successfully.")
            return published_item

    except Exception as e:
        error_message = f"..error publishing/updating feature layer: {str(e)}"
        raise RuntimeError(error_message)
    

def apply_field_properties(gis, title, fprop_dict):
    """Applies field properties (Alias and Length) to the published Feature Layer"""
    # Retrieve the published feature layer
    feature_layer_item = gis.content.search(query=title, item_type="Feature Layer")[0]
    feature_layer = feature_layer_item.layers[0]   

    # Apply Field Lengths and Aliases
    fields = feature_layer.properties['fields']
    # Update the fields based on the dictionary
    for field in fields:
        field_name = field['name']
        if field_name in fprop_dict:
            field['length'] = fprop_dict[field_name]['Length']
            field['alias'] = fprop_dict[field_name]['Alias']

    # Update the field definitions
    response = feature_layer.manager.update_definition({
        "fields": fields
    })

    # Check and print the response
    if 'success' in response and response['success']:
        print("..field aliases updated successfully!")
    else:
        print("..failed to update field aliases. Response:", response)


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
    
    if s3_client:
        logging.info('\nRetrieving drop-off locations from Object Storage')
        df, fprop_dict= get_dropoff_locations(s3_client, bucket_name='whcwdd')

    logging.info('\nPublishing Drop-off locations to AGO')
    title='Animal Specimen Drop-off Locations for Chronic Wasting Disease Testing - Points'
    folder='2024_CWD'
    latcol='Lat'
    longcol= 'Long'
    publish_feature_layer(gis, df, latcol, longcol, title, folder)

    logging.info('\nApplying field properties to the Feature Layer')
    apply_field_properties(gis, title, fprop_dict)

    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    logging.info('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
