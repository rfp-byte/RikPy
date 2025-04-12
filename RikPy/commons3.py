import os
import uuid 
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime
from .commonfunctions import download_file_local, delete_local_file, download_file_local_with_query_parameters


#### AUX FUNCTIONS
def generate_new_filename(original_filename):
    """
    Generate a new filename with a timestamp and shortened UUID.
    """
    # Extract the file extension from the original filename
    file_extension = os.path.splitext(original_filename)[1]

    # Generate a timestamp in the format YYYYMMDDHHMMSS
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Generate a UUID
    generated_uuid = uuid.uuid4()

    # Convert the UUID to a hexadecimal string and truncate it to the desired length
    shortened_uuid = str(generated_uuid.hex)[:8]  # Example: Truncate to 8 characters

    # Create a new filename using the shortened UUID
    new_filename = f"{timestamp}_{shortened_uuid}{file_extension}"

    return new_filename

#### MAIN FUNCTIONS

def s3_environment():

    load_dotenv()  # This loads the environment variables from .env
    S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
    S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
    # S3_URL = os.getenv("S3_URL", 'https://eu-central-1.s3.amazonaws.com/getaiir')
    S3_URL = os.getenv("S3_URL")#, 'https://getaiir.s3.eu-central-1.amazonaws.com')
    S3_CUBE_PUBLIC = os.getenv("S3_CUBE_PUBLIC", 'getaiir/public/')
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", 'getaiir')
    S3_CUBE_NAME = os.getenv("S3_CUBE_NAME", 'eu-central-1')

    # Create a dictionary to store the values
    s3_config_dict = {
        "S3_ACCESS_KEY_ID": S3_ACCESS_KEY_ID,
        "S3_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
        "S3_URL": S3_URL,
        "S3_CUBE_NAME": S3_CUBE_NAME,
        "S3_CUBE_PUBLIC": S3_CUBE_PUBLIC,
        "S3_BUCKET_NAME": S3_BUCKET_NAME
    }

    return s3_config_dict

def s3_list_files_in_folder(folder_name="", s3_config_dict=None):

    '''
    Lists all object in a folder_name from the bucket_name
    '''
    access_key=s3_config_dict['S3_ACCESS_KEY_ID']
    secret_key=s3_config_dict['S3_SECRET_ACCESS_KEY']
    bucket_name=s3_config_dict['S3_BUCKET_NAME']
    cube_name=s3_config_dict['S3_CUBE_NAME']

    #s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = boto3.client('s3', 
            aws_access_key_id=access_key, 
            aws_secret_access_key=secret_key,
            region_name=cube_name)

    prefix = folder_name + '/' if folder_name else None
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
  
    # Check if objects were listed successfully
    if 'Contents' in response:
        objects = response['Contents']
        for obj in objects:
            print("Object Key:", obj['Key'])
    else:
        print("No objects found in the bucket.")

    return objects

def s3_download_files_in_folder(folder_name="", destination_folder="", s3_config_dict=None):
    '''
    Downloads all files in folder_name from the bucket_name to the destination_folder
    examples of working parameters
    dest_folder="C:\\Users\\rforn\\Dropbox\\Code\\RikPy\\Downloads\\Wine"
    folder_name="wine/image"
    '''
    access_key = s3_config_dict['S3_ACCESS_KEY_ID']
    secret_key = s3_config_dict['S3_SECRET_ACCESS_KEY']
    bucket_name = s3_config_dict['S3_BUCKET_NAME']
    cube_name = s3_config_dict['S3_CUBE_NAME']

    s3 = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key,
                      region_name=cube_name)

    prefix = folder_name + '/' if folder_name else None
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in response:
        objects = response['Contents']
        for obj in objects:
            # Get the key (filename) of the object
            key = obj['Key']
            if key.endswith('/'):
                print(f"Skipping folder: {key}")
                continue
            filename = os.path.basename(key)
            destination_path = os.path.join(destination_folder, filename)
            s3.download_file(bucket_name, key, destination_path)
            print(f"Downloaded {key} to {destination_path}")
    else:
        print("No objects found in the bucket.")
    return

def s3_upload_local_file(file_name="", folder_name=None, s3_config_dict=None, bnewname=False, make_public=False, b_delete_local_file=True):
    '''
    Uploads from file name to the folder_name
    file_name is local, must not be a file_path
    
    '''

    if s3_config_dict is None:
        raise ValueError("s3_config_dict is required for this function.")
    
    # Extract relevant values from s3_config_dict
    access_key=s3_config_dict['S3_ACCESS_KEY_ID']
    secret_key=s3_config_dict['S3_SECRET_ACCESS_KEY']
    bucket_name=s3_config_dict['S3_BUCKET_NAME']
    s3_url=s3_config_dict['S3_URL']
    cube_name=s3_config_dict['S3_CUBE_NAME']
    cube_public=s3_config_dict['S3_CUBE_PUBLIC']

    s3 = boto3.client('s3', 
                      aws_access_key_id=access_key, 
                      aws_secret_access_key=secret_key,
                      region_name=cube_name)

    # Ensure folder ends with a '/' if it's not empty
    if folder_name is None:
        folder_name = ''
    elif not folder_name.endswith('/'):
        folder_name += '/'

    if bnewname:
        # Generate a new name for the file
        new_file_name = generate_new_filename(file_name)
        new_file_path = os.path.join(os.path.dirname(file_name), new_file_name)
        os.rename(file_name, new_file_path)
        file_name = new_file_path

    # object_key = f"{folder_name}{file_name}"
    object_key = f"{folder_name}{os.path.basename(file_name)}"
    s3_url = s3_config_dict['S3_URL']
    file_url = s3_url + '/' + object_key

    try:
        response = s3.upload_file(file_name, bucket_name, object_key)

        # Set the ACL to public-read if specified
        if make_public:
            s3.put_object_acl(Bucket=bucket_name, Key=object_key, ACL='public-read')

        print(f"File '{object_key}' uploaded successfully to bucket '{bucket_name}'.")
        
        if b_delete_local_file: delete_local_file(file_name)
        
        return file_url
        return object_key
    except Exception as e:
        print(f"An error occurred while uploading the file: {str(e)}")
        return None
    
def s3_delete_file(object_key, s3_config_dict):
    access_key = s3_config_dict['S3_ACCESS_KEY_ID']
    secret_key = s3_config_dict['S3_SECRET_ACCESS_KEY']
    bucket_name = s3_config_dict['S3_BUCKET_NAME']
    cube_name = s3_config_dict['S3_CUBE_NAME']

    #s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = boto3.client('s3', 
                    aws_access_key_id=access_key, 
                    aws_secret_access_key=secret_key,
                    region_name=cube_name)

    try:
        response = s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"File '{object_key}' deleted successfully from bucket '{bucket_name}'.")
        return response
    except Exception as e:
        print(f"An error occurred while deleting the file: {str(e)}")
        return None
   
def s3_upload_file_from_url(file_url="", folder_name="", s3_config_dict=None, bnewname=False, make_public=False):
       
    if s3_config_dict is None:
        raise ValueError("s3_config_dict is required for this function.")

    # Download the file to a local path
    #file_path = download_file_local(file_url)
    file_path = download_file_local_with_query_parameters(file_url=file_url)

    if bnewname:
        # Generate a new filename
        new_filename = generate_new_filename(file_path)

        # Rename the local file with the new filename
        new_file_path = os.path.join(os.path.dirname(file_path), new_filename)
        os.rename(file_path, new_file_path)

        # Upload the file with the new filename
        file_url = s3_upload_local_file(file_name=new_file_path, folder_name=folder_name, s3_config_dict=s3_config_dict, make_public=make_public)
        # print(f"object_key {object_key}")
        # Delete the local file (both the original and renamed versions)
        delete_local_file(new_file_path)
    else:
        # Upload the file with its original name
        file_url = s3_upload_local_file(file_name=file_path, folder_name=folder_name, s3_config_dict=s3_config_dict)

        # Delete the local file
        delete_local_file(file_path)

    # s3_url = s3_config_dict['S3_URL']
    # file_url = s3_url + '/' + object_key
    
    return file_url