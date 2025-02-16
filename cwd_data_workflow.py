#-------------------------------------------------------------------------------
# Name:        Chronic Wasting Disease (CWD) Data Workflow
#
# Purpose:     This script streamlines the Chronic Wasting Disease (CWD) data processing pipeline by:
#               (1) Retrieving incoming lab data from Object Storage
#               (2) Retrieving data from the Hunter survey
#               (3) Merging and processing all data sources
#               (4) Generating a master dataset
#               (5) Publishing the master dataset to ArcGIS Online (AGO)
#              
# Input(s):    (1) Object Storage credentials.
#              (1) AGO credentials.           
#
# Author:      Moez Labiadh - GeoBC
#              Emma Armitage - GeoBC
#              Sasha Lees - GeoBC
#
# Created:     2024-08-15
# Updated:     
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


def process_master_dataset(df):
    """
    Populates missing Latitude and Longitude values
    Fromat Datetime columns
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


def get_hunter_data_from_ago(gis, AGO_HUNTER_ITEM):
    """
    Returns a df containing hunter survey data from AGO 
    """
    # get the ago item
    hunter_survey_item = gis.content.search(query=AGO_HUNTER_ITEM, item_type="Feature Layer")[0]

    # get the ago feature layer
    hunter_flayer = hunter_survey_item.layers[0]

    # query the feature layer to get its data
    hunter_data = hunter_flayer.query().sdf

    # convert the feature layer data to pandas dataframe
    hunter_df = pd.DataFrame(hunter_data)

    return hunter_df


def add_hunter_data_to_master(df, hunter_df):
    """
    Returns the final master df including hunter survey responses.
    """
    logging.info("..manipulating columns")
    # convert CWD Ear Card values from df to  integer to string
    #df['CWD_EAR_CARD_ID'] = df['CWD_EAR_CARD_ID'].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
    df['CWD_EAR_CARD_ID'] = df['CWD_EAR_CARD_ID'].apply(lambda x: str(int(x)) if pd.notnull(x) and x != 'None' else None)
 
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
    hunter_df =hunter_df[cols]
    
    logging.info("..merging dataframes")
    # merge the dataframes
    combined_df = pd.merge(left=df,
                           right=hunter_df,
                           how="left",
                           left_on="CWD_EAR_CARD_ID",
                           right_on="HUNTER_CWD_EAR_CARD_ID_TEXT")
    
    logging.info("..cleaning dataframes")
    # filter df for where hunters have updated data
    hunter_matches_df = combined_df[combined_df.HUNTER_SEX.notnull()].copy()

    # filter df for where hunters have not updated data
    xls_df = combined_df[combined_df.HUNTER_SEX.isnull()].copy()

    # for hunter_matches_df - update MAP_SOURCE_DESCRIPTOR w/ value = Hunter Survey
    xls_df['MAP_SOURCE_DESCRIPTOR']=  xls_df['SPATIAL_CAPTURE_DESCRIPTOR']
    hunter_matches_df['MAP_SOURCE_DESCRIPTOR'] = "Hunter Survey"
    # for xls_df - update MAP_SOURCE_DESCRIPTOR w/ value = Ear Card
    #xls_df['MAP_SOURCE_DESCRIPTOR'] = "Ear Card"
    
    # clean up xls_df to conform with ago field requirements 
    xls_df[['HUNTER_SPECIES', 'HUNTER_SEX', 'HUNTER_MORTALITY_DATE']] = None

    # populate MAP_LATITUDE and MAP_LONGITUDE columns
    hunter_matches_df[['MAP_LATITUDE', 'MAP_LONGITUDE']] = hunter_matches_df[['HUNTER_LATITUDE_DD', 'HUNTER_LONGITUDE_DD']]
    xls_df[['MAP_LATITUDE', 'MAP_LONGITUDE']] = xls_df[['LATITUDE_DD', 'LONGITUDE_DD']]

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

    return df_wh


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



def save_spatial_files(df, s3_client, bucket_name):
    """
    Saves spatial files of the master datasets in object storage
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


def publish_feature_layer(gis, df, latcol, longcol, title, folder):
    """
    Publishes the master dataset to AGO, overwriting if it already exists.
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

    # Fill NaN and NaT values
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
    Constructs a dictionnaries containing field properties.
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
    df_fprop= df_datadict[['GIS_FIELD_NAME', 'Type', 'Length', 'Alias']]
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


def apply_field_properties(gis, title, domains_dict, fprop_dict):
    """Applies Field proprities to the published Feature Layer"""
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

def sampled_summary_by_unit(df_sum_fld, fl_sum_fld, spatial_fl_id):
    """
    Summarize a count of  'CWD_SAMPLED' by spatial unit - e.g. MU or MOE Region
    """ 

    logging.info(f'\nSummarizing Data by...{df_sum_fld} and joining to the AGO Feature Layer' )
    df_sampled = df_wh[(df_wh['CWD_SAMPLED_IND'] == 'Yes')]
    print('CWD_SAMPLED_IND = Yes:  ',len(df_sampled))


    # Filter columns to include
    df_unit_sampled = df_sampled[[df_sum_fld,'CWD_SAMPLED_IND']]
    #Group by the spatial unit field and count the number of samples
    df_unit_count = df_unit_sampled.groupby(df_sum_fld)['CWD_SAMPLED_IND'].count().reset_index(name='Sample_Count')
    df_unit_count = df_unit_count.sort_values(by=[df_sum_fld])
    print(f'Number of summary units with sampled data for {df_sum_fld}: {df_unit_count}')

    ## Save the summary to a file in object storage - for initial testing
    logging.info('\nSaving the TEST summary')
    bucket_name='whcwdd'
    summary_file_name = df_sum_fld+'_rollup_summary.xlsx'
    save_xlsx_to_os(s3_client, 'whcwdd', df_unit_count, 'master_dataset/rollup_test/'+summary_file_name)
    
    
    ##### Use Pandas Dataframes to update AGO Layers
    ## get spatial features from AGO
    summ_zones = gis.content.get(spatial_fl_id)
    summ_zones_lyr = summ_zones.layers[0]
    summ_zones_fset = summ_zones_lyr.query()
    summ_zones_features = summ_zones_fset.features
    summ_zones_sdf = summ_zones_fset.sdf
    #print(summ_zones_sdf)

    # find  rows in sampling summary that don't have a spatial unit match.  These are probably due to data entry errors.
    df_mismatch_errors = df_unit_count[~df_unit_count[df_sum_fld].isin(summ_zones_sdf[fl_sum_fld])]
    print('\nCHECK DATA: The following unit(s) do not exist in the spatial data:  \n',df_mismatch_errors)

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
    
    uCount = 0   #reset counter

    logging.info('Updating the AGO Feature Layer with the summary data...')
    #Then update the Feature Layer with the summary data for matching records
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
        logging.info('\nRetrieving Incoming Data from Object Storage')
        df = get_incoming_data_from_os(s3_client)
        
        logging.info('\nRetrieving Lookup Tables from Object Storage')
        df_rg, df_mu= get_lookup_tables_from_os(s3_client, bucket_name='whcwdd')
      
    logging.info('\nProcessing the Master dataset')
    df, current_datetime_str, timenow_rounded = process_master_dataset (df)

    logging.info('\nGetting Hunter Survey Data from AGOL')
    AGO_HUNTER_ITEM='CWD_Hunter_Survey_Responses'
    hunter_df = get_hunter_data_from_ago(gis, AGO_HUNTER_ITEM)
    
    logging.info('\nAdding hunter data to Master dataset')
    df_wh= add_hunter_data_to_master(df, hunter_df)


    logging.info('\nInitiating .... Saving an XLSX for the webpage')
    #bucket_name_bbx = 'whcwddbcbox' # this points to BCBOX
    #folder = 'Web/'       # this points to BCBOX
    #save_web_results (df_wh, s3_client, bucket_name_bbx, folder, current_datetime_str)

    bucket_name= 'whcwdd' # this points to BGeoDrive
    folder= 'share_web/' # this points to GeoDrive
    save_web_results (df_wh, s3_client, bucket_name, folder, current_datetime_str)
    

    logging.info('\nInitiating .... Saving an XLSX for lab testing')
    #bucket_name_bbx= 'whcwddbcbox' # this points to BCBOX
    #folder_bbx= 'Lab/to_Lab/' # this points to BCBOX
    #save_lab_submission (df_wh, s3_client, bucket_name_bbx, folder_bbx)

    bucket_name= 'whcwdd' # this points to GeoDrive
    folder= 'share_labs/for_lab/' # this points to GeoDrive
    save_lab_submission (df_wh, s3_client, bucket_name, folder)
    
    logging.info('\nSaving the Master Dataset')
    bucket_name='whcwdd'
    backup_master_dataset(s3_client, bucket_name) #backup
    #save_xlsx_to_os(s3_client, 'whcwdd', df, 'master_dataset/cwd_master_dataset_sampling.xlsx') #just lab data - used for initial testing
    save_xlsx_to_os(s3_client, 'whcwdd', df_wh, 'master_dataset/cwd_master_dataset_sampling_w_hunter.xlsx') #lab + hunter data

    
    #print('\nExiting...\n')
    #sys.exit()
    
    #logging.info('\nSaving spatial data')
    #save_spatial_files(df_wh, s3_client, bucket_name)

    logging.info('\nPublishing the Master Dataset to AGO')
    title='CWD_Master_dataset'
    folder='2024_CWD'
    latcol='MAP_LATITUDE'
    longcol= 'MAP_LONGITUDE'
    published_item= publish_feature_layer(gis, df_wh, latcol, longcol, title, folder)

    logging.info('\nApplying field properties to the Feature Layer')
    domains_dict, fprop_dict= retrieve_field_properties(s3_client, bucket_name)
    apply_field_properties (gis, title, domains_dict, fprop_dict)
    
    logging.info(f'\nSummarizing Data by spatial units.' )
    sampled_summary_by_unit('WMU', 'WILDLIFE_MGMT_UNIT_ID', '0b81e46151184058a88c701853e09577')
    sampled_summary_by_unit('ENV_REGION_NAME', 'REGION_NAME', '118379ce29f94faeaa724d2055ea235c')


    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    logging.info('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))