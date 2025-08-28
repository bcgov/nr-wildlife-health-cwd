import requests
import logging
import sys
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(message)s')

FORM_ID = os.getenv("CHEFS_FORM_ID")
API_KEY = os.getenv("CHEFS_API_KEY")
BASE_URL = "https://submit.digital.gov.bc.ca/app/api/v1/forms"
CHEFS_HIDDEN_FIELDS = ["LATITUDE_DD_CALC","LONGITUDE_DD_CALC", "SAMPLE_FROM_SUBMITTER"]

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
        sys.exit()

def get_hunter_data_from_chefs(BASE_URL, FORM_ID, API_KEY, CHEFS_HIDDEN_FIELDS):
    logging.info("Fetching the published version ID of the CHEFS form")
    version_data = chefs_api_request(base_url=BASE_URL, 
                                     endpoint="version", 
                                     form_id=FORM_ID, 
                                     api_key=API_KEY)
    
    # extract version ID from the response
    version_id = version_data['versions'][0]['id']
    logging.info(f"Published version ID: {version_id}")

    logging.info("Fetching the published version ID of the CHEFS form")
    field_name_data = chefs_api_request(base_url=BASE_URL,
                                        endpoint=f"versions/{version_id}/fields",
                                        form_id=FORM_ID,
                                        api_key=API_KEY)
    
    # add hidden fields to the list of field names as they are not returned by the API
    for hidden_field in CHEFS_HIDDEN_FIELDS:
        field_name_data.append(hidden_field)
    # format the field names into a comma-separated string
    chefs_fields = ",".join(field_name_data)

    logging.info("Fetching CHEFS submissions")
    chefs_data = chefs_api_request(base_url=BASE_URL,
                                   endpoint="submissions",
                                   form_id=FORM_ID,
                                   api_key=API_KEY,
                                   params={"fields": chefs_fields})
    
    # convert json reponse to dataframe
    chefs_df = pd.json_normalize(chefs_data)
    # remove deleted submissions from the dataframe
    chefs_df = chefs_df[chefs_df['deleted'] == False]

    return chefs_df