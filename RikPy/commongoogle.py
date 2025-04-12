from dataclasses import dataclass
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

@dataclass
class Response:
    success: bool
    message: str
    data: dict = None

def google_drive_upload_or_update_file(service_account_file, google_drive_folder, google_drive_file, upload_file, mime_type='application/xml'):
    
    """
    Uploads a file to a specified folder in Google Drive. Updates the file if it 
    already exists.

    Parameters:
    service_account_file (str): Path to the service account credentials file.
    google_drive_folder (str): Name of the folder in Google Drive.
    google_drive_file (str): Name of the file to be uploaded/updated in Google Drive.
    upload_file (str): Path of the file to upload.
    mime_type (str): MIME type of the file. Defaults to 'application/xml'.

    Returns:
    Response: indicating the result of
    """
    
    try:
        creds = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=['https://www.googleapis.com/auth/drive']
        )

        drive_service = build('drive', 'v3', credentials=creds)

        folder_query = f"name='{google_drive_folder}' and mimeType='application/vnd.google-apps.folder'"
        folder_result = drive_service.files().list(q=folder_query).execute()

        if 'files' in folder_result and len(folder_result['files']) > 0:
            folder_id = folder_result['files'][0]['id']
        else:
            return Response(False, f"Folder '{google_drive_folder}' not found in Google Drive.")

        file_query = f"name='{google_drive_file}' and '{folder_id}' in parents"
        file_result = drive_service.files().list(q=file_query).execute()

        if 'files' in file_result and len(file_result['files']) > 0:
            existing_file_id = file_result['files'][0]['id']
            media = MediaFileUpload(upload_file, mimetype=mime_type)
            updated_file = drive_service.files().update(fileId=existing_file_id, media_body=media).execute()
            return Response(True, f"File '{google_drive_file}' updated in folder '{google_drive_folder}'.", {'file_id': updated_file['id']})
        else:
            file_metadata = {'name': google_drive_file, 'mimeType': mime_type, 'parents': [folder_id]}
            media = MediaFileUpload(upload_file, mimetype=mime_type)
            created_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            return Response(True, f"File '{google_drive_file}' created in folder '{google_drive_folder}'.", {'file_id': created_file['id']})

    except HttpError as error:
        return Response(False, f"HTTP error occurred: {error}")
    except Exception as error:
        return Response(False, f"An unexpected error occurred: {error}")