import asyncio
import logging
import datetime
import time
import re
import os
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

import pandas as pd
import tqdm
import tqdm.asyncio
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient
from dataclasses import dataclass
from ETL import extract,transform,load, load_all

azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azure_logger.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

ACCOUNTNAME=''
STORAGEACCOUNTURL= ''
ACCOUNTKEY= ''
CONTAINERNAMEDOWN='boegelund'
CONTAINERNAMEUP= 'keyframes-boegelund'
BLOBNAME= 'dome_capture/video/24-28-fd-fe-f5-09/2022-07-01'
DEST_FILE_PATH= 'Blobs.txt'



def download_data(container_client):
    """Get all of the blobs from 24-28-fd-fe-f5-09"""
    list=[]
    for i in filter(lambda x: x["name"].startswith("dome_capture/video/24-28-fd-fe-f5-09"),container_client.list_blobs()):
        list.append(i)
    return list
def dates(list):
    """Extract the dates for later use"""
    dates=[]
    prev="0000-00-00"
    for i in list:
        match_str=re.search(r'\d{4}-\d{2}-\d{2}', i.name)
        res = str(datetime.datetime.strptime(match_str.group(), '%Y-%m-%d').date())
        if res!= prev:
            dates.append(res)
            prev=res
    return dates 
def list_by_dates(list,dates):
    final_list= [None]* len(dates)
    second_list=[]

    for n, date in enumerate (dates):
        for i in list:
            if i["name"].find(date)!= -1:
                second_list.append(i)
        final_list[n]=second_list
        second_list= []

    return final_list

def download_video(blob_list,dates: List[str],blob_service_client_instance,container_client):
    #Create directories to save the downloaded files
    parent_folder= 'Videos'
    

    path="/Users/jacobogfr/Documents/Task/Videos"
    for child_folder in dates:
        folder = os.path.join(parent_folder, child_folder)
        if not os.path.exists(folder):
            os.makedirs(folder)
        for date in blob_list:
            for blob in date:
                try:
                    blob_client = container_client.get_blob_client(blob=blob["name"])
                except:
                    logger.error(f"Couldn't find blob with expecific path on Azure")
                try:
                    data =  blob_client.download_blob()
                    with open(folder+'/'+blob["name"][-48:], "wb") as f:
                        f.write(data.readall())
                except ResourceNotFoundError:
                    logger.error(f"Couldn't find {blob} on Azure")

    

def simple(container_client):
    path="/Users/jacobogfr/Documents/Task/video.mp4"
    try:
        blob_client = container_client.get_blob_client(blob="dome_capture/video/24-28-fd-fe-f5-09/2022-07-01/20220701T040014+0200_t35.00a335.00z1.00_dome.mp4")
    except:
        logger.error(f"Couldn't find blob with expecific path on Azure")
    try:
        data =  blob_client.download_blob()
        with open(path, "wb") as f:
            #data.readinto(f)
            f.write(data.readall())
    except ResourceNotFoundError:
        logger.error(f"Couldn't find blob on Azure")
    return None   
    
def download_blob_Mads(container_client, blob_filepath: str, dest_filepath: str):
   # """Use the BlobServiceClient to download a specified blobs."""    async with service_client:  # type: ignore        container_client = service_client.get_container_client(CONTAINER)    
    try:
        blob_client = container_client.get_blob_client(blob=blob_filepath)
    except:
        logging.error(f"Couldn't find blob with path {blob_filepath} on Azure")
    with blob_client:
        try:
            data =  blob_client.download_blob()
            with open(dest_filepath, "wb") as file_handle:
                 data.readinto(file_handle)
        except ResourceNotFoundError:
            logging.error(f"Couldn't find {blob_filepath} on Azure") 

           
 

def main():
    connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + ACCOUNTNAME + ';AccountKey=' + ACCOUNTKEY + ';EndpointSuffix=core.windows.net'
    blob_service_client_instance = BlobServiceClient.from_connection_string(connect_str)
    #Connect to the Container
    container_client = blob_service_client_instance.get_container_client(CONTAINERNAMEDOWN)
    #Extracttion process
    list=download_data(container_client)
    dates_list= dates(list)
    #print('Number of dates: ',len(dates_list))
    final_list=list_by_dates(list,dates_list)
    extract(final_list,dates_list,blob_service_client_instance,container_client)
    #Loading process
    container_client_load = blob_service_client_instance.get_container_client(CONTAINERNAMEUP)
    load_all(container_client_load)
    print("End of main")
if __name__=="__main__":
    main()
