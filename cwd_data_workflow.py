import os
import boto3
import botocore
import requests
import pandas as pd
from io import BytesIO
import geopandas as gpd
from shapely import geometry
from datetime import datetime
import logging


def configure_logging(log_file='app.log', log_level=logging.INFO):
    """
    Configures the logging settings
    """
    # Clear existing handlers from the root logger
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set up new logging configuration
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler with a simpler formatter
    console_formatter = logging.Formatter('%(message)s')  # Only message without date, time, level
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Set the logging level
    root_logger.setLevel(log_level)


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


def get_data_from_os(s3_client):
    """
    Returns a df of private data from Object Storage
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
                if key.startswith('incoming_from') and key.endswith('.xlsx') and 'private_session' in key.lower():
                    try:
                        logging.info(f"...reading file: {key}")
                        obj_data = s3_client.get_object(Bucket=bucket_name, Key=key)
                        df = pd.read_excel(BytesIO(obj_data['Body'].read()), 
                                           sheet_name='Sampling',
                                           converters={'CWD_EAR_CARD_ID': str})
                        df_list.append(df)
                    except botocore.exceptions.BotoCoreError as e:
                        logging.error(f"...failed to retrieve file: {e}")
    
    if df_list:
        logging.info("..appending dataframes")
        return pd.concat(df_list, ignore_index=True)
    else:
        logging.info("..no dataframes to append")
        return pd.DataFrame()


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
    #response = requests.post(TOKEN_URL, data=params, verify=False) #without SSL verification
    response = requests.post(TOKEN_URL, data=params, verify=True)  #with SSL verification
    response.raise_for_status()  
    
    return response.json().get('token')


def query_ago_feature_layer(token, ACCOUNT_ID, FEATURE_SERVICE, LYR_INDEX=0):
    """
    Returns data from AGO layer based on query 
    """
    query_url = f"https://services6.arcgis.com/{ACCOUNT_ID}/arcgis/rest/services/{FEATURE_SERVICE}/FeatureServer/{LYR_INDEX}/query"
    query_params = {
        'where': '1=1',  # Return all features
        'outFields': '*',  # Return all fields
        'returnGeometry': 'true',
        'geometryPrecision': 6,
        'outSR': 4326,  # WGS84 coordinate system
        'f': 'json',
        'token': token
    }
    #response = requests.get(query_url, params=query_params, verify=False) #without SSL verification
    response = requests.get(query_url, params=query_params, verify=True)   #with SSL verification
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    return response.json()


def features_to_gdf(features):
    """
    Returns a gdf based on an AGO json response 
    """
    features_list = features['features']
    data = []
    
    for feature in features_list:
        attributes = feature['attributes']
        geom = feature['geometry']
        
        # Convert ESRI JSON to GeoJSON
        if 'rings' in geom:
            geom_type = "Polygon"
            coordinates = geom['rings']
        elif 'paths' in geom:
            geom_type = "LineString"
            coordinates = geom['paths'][0]
        elif 'points' in geom:
            geom_type = "MultiPoint"
            coordinates = geom['points']
        else:
            geom_type = "Point"
            coordinates = [geom['x'], geom['y']]
        
        geojson = {
            "type": geom_type,
            "coordinates": coordinates
        }
        
        # Create Shapely geometry from GeoJSON
        shape = geometry.shape(geojson)
        
        attributes['geometry'] = shape
        data.append(attributes)
    
    gdf = gpd.GeoDataFrame(data, geometry='geometry', crs=4326)
    
    return gdf
    

def gdf_to_gdb(gdf, gdb_path, layer_name):
    """
    Saves a GeoDataFrame to an ESRI Geodatabase.
    """
    import fiona
    fiona.drvsupport.supported_drivers['OpenFileGDB'] = 'raw'
    


    # Save the gdf to the geodatabase
    gdf.to_file(gdb_path, layer=layer_name, driver="OpenFileGDB")

    print("..gdf successfully saved to the GDB.")


def save_csv_to_os(s3_client, bucket_name, df, file_name):
    """
    Saves a csv to Object Storage bucket.
    """
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
        logging.info(f'..data successfully saved to bucket {bucket_name}')
    except botocore.exceptions.ClientError as e:
        logging.error(f'..failed to save data to Object Storage: {e.response["Error"]["Message"]}')
        
        

if __name__ == "__main__":
    configure_logging(log_level=logging.INFO)
    
    logging.info('Connecting to Object Storage')
    S3_ENDPOINT = os.getenv('S3_ENDPOINT')
    S3_CWD_ACCESS_KEY = os.getenv('S3_CWD_ACCESS_KEY')
    S3_CWD_SECRET_KEY = os.getenv('S3_CWD_SECRET_KEY')
    s3_client = connect_to_os(S3_ENDPOINT, S3_CWD_ACCESS_KEY, S3_CWD_SECRET_KEY)
    
    if s3_client:
        logging.info('Retrieving Private Data from Object Storage')
        df_prv = get_data_from_os(s3_client)
    
    logging.info('Connecting to AGOL')
    AGO_TOKEN_URL = os.getenv('AGO_TOKEN_URL')
    AGO_HOST = os.getenv('AGO_HOST')
    AGO_USERNAME = os.getenv('AGO_USERNAME')
    AGO_PASSWORD = os.getenv('AGO_PASSWORD')
    AGO_ACCOUNT_ID = os.getenv('AGO_ACCOUNT_ID')
    
    token = get_ago_token(AGO_TOKEN_URL, AGO_HOST, AGO_USERNAME, AGO_PASSWORD)
    
    logging.info('Retrieving data from AGOL')
    FEATURE_SERVICE = "TEST_data_CWD_no_private_info"
    features = query_ago_feature_layer(token, AGO_ACCOUNT_ID, FEATURE_SERVICE)
    
    logging.info('Converting data to geodataframe')
    gdf = features_to_gdf(features)
     
    logging.info('Joining tables')    
    df = pd.merge(gdf, 
                  df_prv, 
                  how='left', 
                  left_on='CWD_Ear_Card', 
                  right_on='CWD_EAR_CARD_ID') 
    
    
    '''
    logging.info('Saving spatial data to GDB')
    gdb_path= os.path.join(script_dir, 'test_data.gdb')
    today = datetime.today().strftime('%Y%m%d')
    layer_name= today +'_cwd_spatial_data'
    gdf_to_gdb(gdf, gdb_path, layer_name)
    '''
    
    logging.info('Saving data to CSV')
    dytm = datetime.now().strftime("%Y%m%d_%H%M")
    df=df[['Region', 'Drop_off_Location', 'WLH_ID_x','CWD_EAR_CARD_ID']]
    
    #df.to_csv(os.path.join(script_dir, 'test_outs', dytm+'_samples.csv'), index= False)

    print (df.shape[0])
    
    save_csv_to_os(s3_client, 'whcwdd', df, f'master_dataset/{dytm}_samples.csv')