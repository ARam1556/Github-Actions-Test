import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from configparser import ConfigParser
from azure.storage.blob import BlobServiceClient
from io import BytesIO  # For in-memory file handling

# Get Azure Blob Storage credentials from config
azure_connection_string = os.environ["connection_string"]
container_name = os.environ["container_name"]

# Get RingCentral credentials from config
username = os.environ["username"]
password = os.environ["password"]
client_id = os.environ["client_id"]
client_secret = os.environ["client_secret"]
start_date = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
end_date = date.today().strftime("%Y-%m-%d")


def main():
    def get_job_status(job_id, headers, max_retries=5):
        link = job_id.strip('"')
        url = f"https://na1.nice-incontact.com/data-extraction/v1/jobs/{link}"
        retries = 0

        while retries < max_retries:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                job_response = response.json()
                job_status = job_response.get("jobStatus", {}).get("status")

                if job_status is None:
                    print("Job status not available. Retrying in 10 seconds...")
                elif job_status.upper() == "SUCCEEDED":
                    print("Job Successful")
                    result_link = job_response.get("jobStatus", {}).get("result", {}).get("url")
                    return result_link
                elif job_status.upper() in ["FAILED", "CANCELLED"]:
                    print(f"Job Status: {job_status}. Exiting without result.")
                    return None
                else:
                    print(f"Job Status: {job_status}. Retrying in 10 seconds...")
            else:
                print(f"Error: {response.status_code} - {response.text}")
                return None

            retries += 1
            time.sleep(10)

    # Generate the access token
    url = "https://na1.nice-incontact.com/authentication/v1/token/access-key"
    headers = {"Content-Type": "application/json"}
    data = {
        "accessKeyId": username,
        "accessKeySecret": password,
        "client_id": client_id,
        "client_secret": client_secret
    }

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        access_token = response.json().get("access_token")
    else:
        print(f"Error obtaining access token: {response.text}")
        exit()

    # Prep for data extraction
    deheaders = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    dedata = {
        "entityName": "recording-interaction-metadata",
        "version": "7",
        "startDate": start_date,
        "endDate": end_date
    }

    deurl = "https://na1.nice-incontact.com/data-extraction/v1/jobs"

    # After the POST request to generate the job
    response2 = requests.post(deurl, headers=deheaders, json=dedata)

    # Print the response status code and content
    print(f"Response Status Code: {response2.status_code}")
    print(f"Response Content: {response2.text}")

    if response2.status_code == 202:
        job_id = response2.text.strip('"')  # Directly use the response text
    elif response2.status_code == 403:
        print(f"Error: {response2.text}. Please wait and try again later")
        exit()
    else:
        print(f"Error: {response2.status_code} - {response2.text}")
        exit()

    # Get the job status
    res = get_job_status(job_id, deheaders)
    df = pd.read_csv(res, low_memory=False)

    # Replace blank values with None
    df.fillna('', inplace=True)

    # Use BytesIO to avoid saving the file locally
    output_stream = BytesIO()
    df.to_csv(output_stream, index=False)
    output_stream.seek(0)  # Move to the start of the stream

    # Upload to Azure Blob Storage directly
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob="Ring_Central.csv")

        # Upload the file stream to Azure Blob Storage
        blob_client.upload_blob(output_stream, overwrite=True)
        print(f"File uploaded to Azure Blob Storage successfully.")
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")

if __name__ == '__main__':
    main()
