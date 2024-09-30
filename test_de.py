import os
import time
import requests
import json
import pandas as pd
from datetime import date, timedelta

username = os.environ["username"]
password = os.environ["password"]
client_id = os.environ["client_id"]
client_secret = os.environ["client_secret"]
start_date = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
end_date = date.today().strftime("%Y-%m-%d")

def main():
  # returns the url for the job if successful
    def get_job_status(job_id, headers, max_retries=5):
      link = job_id.strip('"')
      url = f"https://na1.nice-incontact.com/data-extraction/v1/jobs/{link}"
      retries = 0
  
      while retries < max_retries:
        response = requests.get(url, headers=headers)
  
        if response.status_code == 200:
          job_response = response.json()
          job_status = job_response.get("jobStatus",{}).get("status")
  
          if job_status is None:
            print("Job status not available. retrying in 10 seconds...")
          elif job_status.upper() == "SUCCEEDED":
            print("Job Successful")
            result_link = job_response.get("jobStatus",{}).get("result",{}).get("url")
            return result_link
          elif job_status.upper() in ["FAILED","CANCELLED"]:
            print(f"Job Status: {job_status}. Exiting without result.")
            return None
          else:
            print(f"Job Status: {job_status}. Retrying in 10 seconds...")
        else:
          print(f"Error: {response.status_code} - {response.text}")
          return none
  
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
  
    # prep for data extraction 
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
  
    # POST request to generate the job
    response2 = requests.post(deurl, headers=deheaders, json=dedata)
  
    if response2.status_code == 202:
      # since the response is a string, stripping quotes from around the job ID
      job_id = response2.text.strip('"')
    elif response2.status_code == 403:
      print(f"Error: {response2.text}. Please wait and try again later")
      exit()
    else:
      print(f"Error: {response2.status_code} - {response2.text}")
      exit()
  
    # get the job status
    res = get_job_status(job_id, deheaders)
    df = pd.read_csv(res)
  
    folder_path = "./data_folder"
    os.makedirs(folder_path, exist_ok=True)
  
    file_name = f"{end_date}_output.csv"
    file_path = os.path.join(folder_path, file_name)
    df.to_csv(file_path, index=False)

if __name__ == '__main__':
    main()
  
