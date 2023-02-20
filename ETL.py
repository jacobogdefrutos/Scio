import subprocess
import os
import re
import logging
import pandas as pd
import tqdm
import tqdm.asyncio
from typing import List, Union
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobPrefix

azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azure_logger.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)




def extract(blob_list,dates: List[str],blob_service_client_instance,container_client):
    """Extraction of the videos"""
    #Create directories to save the downloaded files
    parent_folder= 'Videos'
    #path="/Users/jacobogfr/Documents/Task/Videos"
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
                        print(blob["name"][-48:]," located in the folder ",child_folder)
                except ResourceNotFoundError:
                    logger.error(f"Couldn't find {blob} on Azure")

def transform():
    """Extract the keyframes of an input video"""
    root_dir='/Users/jacobogfr/Documents/Task/Videos'
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if len(filenames) != 0:
            for file in filenames:
                if file[-3:]  != 'jpg':
                    keyf_folder=dirpath+'/'+file[:(len(file)-4)]+'_keyframes'
                    if not os.path.exists(keyf_folder):
                        os.makedirs(keyf_folder)
                    #command1='cd '+ keyf_folder
                    #command2= 'ffmpeg -skip_frame nokey -i '+'fmpeg -skip_frame nokey -i /Users/jacobogfr/Documents/Task/Videos/2022-07-01/'+file+'  -vsync vfr -frame_pts false '+keyf_folder+'/keyframes%02d.jpg'
                    command2='ffmpeg -skip_frame nokey -i ' +'/Users/jacobogfr/Documents/Task/Videos/2022-07-01/'+ file + ' -vsync vfr -frame_pts false '+keyf_folder+'/keyframes%02d.jpg'
                    #/Users/jacobogfr/Documents/Task/downscaled.mp4
                    #ffmpeg -skip_frame nokey -i /Users/jacobogfr/Documents/Task/downscaled.mp4  -vsync vfr -frame_pts false /Users/jacobogfr/Documents/Task/Videos/2022-07-01/downscaled_mine_keyframes/keyframes%02d.jpg
                    #ffmpeg -skip_frame nokey -i /Users/jacobogfr/Documents/Task/Videos/2022-07-01/downscaled_mine.mp4  -vsync vfr -frame_pts false /Users/jacobogfr/Documents/Task/Videos/2022-07-01/downscaled_mine_keyframes/keyframes%02d.jpg
                    print(command2)
                    #result = subprocess.run(command1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    try:
                        result2 = subprocess.run(command2, shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    except FileNotFoundError:
                        print('File not estractable:',file)
                        #print(result2.stdout.decode("utf-8"))
                    #print(result2.stderr.decode("utf-8"))
    return None
def load(container_client):
    """Loading the keyframes into azure. Here I try to load in date folders created over the course of the run"""
    pattern = r"\d{4}-\d{2}-\d{2}"
    root_dir='/Users/jacobogfr/Documents/Task/Videos'
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for file in filenames:
            if file[-3:]  == 'jpg':
                file_path= dirpath + '/' +file
                match = re.search(pattern, dirpath)
                folder=match.group()
                if match:
                    # Delete the folder if it already exists
                    blobs = container_client.list_blobs(name_starts_with=f"{folder}/")
                    if any(b['name']==(folder+'/') for b in blobs):
                        container_client.delete_blobs(name_starts_with=f"{folder}/")
                    try:
                        blob_client1 = container_client.get_blob_client(f"{folder}/")
                        blob_client2 = container_client.get_blob_client(file)
                    except:
                        logger.error(f"Couldn't get client from blob")
                    with open(file_path, "rb") as data:
                        blob_client1.upload_blob(b"")
                        blob_client2.upload_blob(f"{folder}/{file}")
                        blob_client2.upload_blob(data, overwrite=True)
                        print(f"File {file} updated succesfully ")

def load_all(container_client):
    """Loading the keyframes into azure, all in the same container, no difference between folder"""
    pattern = r"\d{4}-\d{2}-\d{2}"
    root_dir='/Users/jacobogfr/Documents/Task/Videos'
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for file in filenames:
            if file[-3:]  == 'jpg':
                file_path= dirpath + '/' +file
                match = re.search(pattern, dirpath)
                folder=match.group()
                if match:
                    try:
                        blob_client2 = container_client.get_blob_client(file)
                    except:
                        logger.error(f"Couldn't get client from blob")
                    with open(file_path, "rb") as data:
                        blob_client2.upload_blob(data, overwrite=True)
                        print(f"File {file} updated succesfully ")
                    # Now we delete locally the file
                    os.remove(file_path)





