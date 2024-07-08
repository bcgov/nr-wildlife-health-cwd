import requests
import pandas as pd
import json

# Replace with your ArcGIS Online username and password
USERNAME = 'XXX'
PASSWORD = 'XXX
ORGANIZATION_ID = 'XXX'

# URL for ArcGIS Online token
TOKEN_URL = 'https://www.arcgis.com/sharing/rest/generateToken'

# URL for creating a new feature service
CREATE_SERVICE_URL = f'https://www.arcgis.com/sharing/rest/content/users/{USERNAME}/createService'

def get_token(username, password):
    params = {
        'f': 'json',
        'username': username,
        'password': password,
        'referer': f'https://{ORGANIZATION_ID}.maps.arcgis.com',
        'expiration': 60
    }
    response = requests.post(TOKEN_URL, data=params)
    response_json = response.json()
    if 'token' in response_json:
        return response_json['token']
    else:
        raise Exception(f"Error obtaining token: {response_json}")

def create_feature_service(token, service_name):
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
    response = requests.post(CREATE_SERVICE_URL, data=params)
    response_json = response.json()
    if 'serviceItemId' in response_json and 'encodedServiceURL' in response_json:
        service_id = response_json['serviceItemId']
        service_url = response_json['encodedServiceURL']
        admin_url = service_url.replace('/rest/', '/rest/admin/')
        return service_id, admin_url
    else:
        raise Exception(f"Error creating feature service: {response_json}")

def add_layer_to_service(token, admin_url):
    add_to_definition_url = f"{admin_url}/addToDefinition"
    layer_definition = {
        "layers": [
            {
                "name": "Points",
                "type": "Feature Layer",
                "geometryType": "esriGeometryPoint",
                "fields": [
                    {
                        "name": "ObjectID",
                        "type": "esriFieldTypeOID",
                        "alias": "ObjectID",
                        "sqlType": "sqlTypeOther",
                        "nullable": False,
                        "editable": False,
                        "domain": None,
                        "defaultValue": None
                    }
                ],
                "extent": {
                    "xmin": -180,
                    "ymin": -90,
                    "xmax": 180,
                    "ymax": 90,
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
    print(f"Add layer response status code: {response.status_code}")
    print(f"Add layer response content: {response.text}")
    response_json = response.json()
    if 'success' in response_json and response_json['success']:
        return True
    else:
        raise Exception(f"Error adding layer to feature service: {response_json}")

def add_features(token, service_url, df):
    add_features_url = f"{service_url}/0/addFeatures"
    features = []
    for index, row in df.iterrows():
        features.append({
            'geometry': {'x': row['Longitude'], 'y': row['Latitude'], 'spatialReference': {'wkid': 4326}},
            'attributes': {}
        })
    params = {
        'f': 'json',
        'token': token,
        'features': json.dumps(features)
    }
    response = requests.post(add_features_url, data=params)
    response_json = response.json()
    if 'addResults' in response_json:
        return response_json['addResults']
    else:
        raise Exception(f"Error adding features: {response_json}")

# Example DataFrame
data = {'Latitude': [34.05, 36.16], 'Longitude': [-118.25, -115.15]}
df = pd.DataFrame(data)

try:
    # Authenticate and get token
    token = get_token(USERNAME, PASSWORD)
    print("Token obtained successfully")

    # Create feature service
    service_name = 'MyFeatureService'
    service_id, admin_url = create_feature_service(token, service_name)
    print(f"Feature service created successfully with ID: {service_id}")
    print(f"Admin URL: {admin_url}")

    # Add a layer to the feature service
    layer_added = add_layer_to_service(token, admin_url)
    if layer_added:
        print("Layer added to the feature service successfully")

    # Add features to the feature service
    service_url = admin_url.replace('/admin/', '/')
    add_features_response = add_features(token, service_url, df)
    print("Features added successfully:")
    print(add_features_response)

except Exception as e:
    print(f"An error occurred: {e}")