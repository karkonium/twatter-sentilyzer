from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from config import UPLOAD_DIR_ID

def upload_file(service, file_name, upload_file_path):
    try:
        file_metadata = {
            'name': file_name,
            # 'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [UPLOAD_DIR_ID]
        }
        media = MediaFileUpload(upload_file_path, resumable=True)
        # pylint: disable=maybe-no-member
        file = service.files().create(body=file_metadata, media_body=media,
                                      fields='id').execute()
        print(f'File with ID: "{file.get("id")}" has been uploaded.')

    except HttpError as error:
        print(f'An error occurred: {error}')
        file = None

    return file.get('id')