import requests
import logging
import sys
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(message)s')

FORM_ID = os.getenv("CHEFS_FORM_ID")
API_KEY = os.getenv("CHEFS_API_KEY")
BASE_URL = "https://submit.digital.gov.bc.ca/app/api/v1/forms"
CHEFS_HIDDEN_FIELDS = ["HUNTER_LATITUDE_DD_CALC","HUNTER_LONGITUDE_DD_CALC", "SAMPLE_FROM_SUBMITTER"]

def get_chefs_form_version(base_url, form_id, api_key):
    """
    Retrieves the published version of the CHEFS form.

    Returns: the version ID of the published form.
    """

    v = requests.get(f"{base_url}/{form_id}/version", auth=(form_id, api_key))
    if v.status_code == 200:
        logging.info("Form version fetched successfully")
        version_data = v.json()
        version_id = version_data['versions'][0]['id']
        return version_id
    else:
        logging.error(f"Failed to fetch form version: {v.status_code} - {v.text}")


def get_chefs_field_names(base_url, version_id, form_id, api_key, hidden_fields):
    """
    Retrieves field names for a specific CHEFS form version, including hidden fields that are not returned by the API.

    Returns: a comma-separated string of field names.
    """

    f = requests.get(f"{base_url}/{form_id}/versions/{version_id}/fields", auth=(form_id, api_key))
    if f.status_code == 200:
        logging.info("Field names fetched successfully")
        field_names = f.json()
        for hidden_field in hidden_fields:
            field_names.append(hidden_field)
        chefs_fields = ",".join(field_names)
        return chefs_fields
    else:
        logging.error(f"Failed to fetch field names: {f.status_code} - {f.text}")
        sys.exit()

def get_chefs_submissions(base_url, form_id, api_key, fields):
    """
    Retrieves submissions for a specific CHEFS form, including only the fields retrieved from the form version used in get_chefs_field_names.

    Returns: a pandas DataFrame containing the submissions.
    """

    r = requests.get(f"{base_url}/{form_id}/submissions", auth=(form_id, api_key), params={"fields": fields})

    if r.status_code == 200:
        logging.info("Submissions fetched successfully")
        data = r.json()
        chefs_df = pd.json_normalize(data)

        logging.info(f"Removing deleted submissions from the DataFrame")
        chefs_df = chefs_df[chefs_df['deleted'] == False]

        return chefs_df
    else:
        logging.error(f"Failed to fetch submissions: {r.status_code} - {r.text}")

if __name__ == "__main__":
    logging.info("Fetching the published version ID of the CHEFS form")
    version_id = get_chefs_form_version(BASE_URL, FORM_ID, API_KEY)

    logging.info(f"Fetching field names for form verion {version_id}")
    chefs_fields = get_chefs_field_names(BASE_URL, version_id, FORM_ID, API_KEY, CHEFS_HIDDEN_FIELDS)

    logging.info("Fetching submissions from the form")
    chefs_df = get_chefs_submissions(BASE_URL, FORM_ID, API_KEY, chefs_fields)