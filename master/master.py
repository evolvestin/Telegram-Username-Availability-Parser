import io
import os
import re
import pickle
import objects
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
# ========================================================================================================
standard_file_fields = 'files(id, name, parents, createdTime, modifiedTime)'
objects.environmental_files()


class Drive:
    def __init__(self, path):
        scope = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_file(path, scopes=scope)
        self.client = build('drive', 'v3', credentials=credentials)

    @staticmethod
    def revoke_time(file):
        for key in ['modifiedTime', 'createdTime']:
            if file.get(key):
                stamp = re.sub(r'\..*?Z', '', file[key])
                file[key] = objects.stamper(stamp, '%Y-%m-%dT%H:%M:%S')
        return file

    def download_file(self, file_id, file_path):
        done = False
        file = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(file, self.client.files().get_media(fileId=file_id))
        while done is False:
            try:
                status, done = downloader.next_chunk()
            except IndexError and Exception:
                done = False

    def files(self, fields=standard_file_fields, only_folders=None, name_startswith=None, parents=None):
        query = ''
        response = []
        if only_folders:
            query = "mimeType='application/vnd.google-apps.folder'"
        if name_startswith:
            if query:
                query += ' and '
            query += f"name contains '{name_startswith}'"
        if parents:
            if query:
                query += ' and '
            query += f"'{parents}' in parents"
        result = self.client.files().list(q=query, pageSize=1000, fields=fields).execute()
        for file in result['files']:
            response.append(self.revoke_time(file))
        return response
# ========================================================================================================


def start(stamp):
    array = []
    temp_id = ''
    print(stamp)
    drive_client = Drive('google.json')
    for folder in drive_client.files(only_folders=True):
        if folder['name'] == 'temp':
            temp_id = folder['id']

    for file in drive_client.files(parents=temp_id):
        drive_client.download_file(file['id'], file['name'])
        with open(file['name'], 'rb') as local_file:
            if 'clear' in file['name']:
                array.extend(pickle.load(local_file))
                print(file['name'], len(array))
        os.remove(file['name'])
    print('all', len(array))
    array.clear()
    with open('db1', 'wb') as file:
        client = gspread.service_account('google.json')
        google_values = client.open_by_key('1d0OS28iDsUEkQm1Rh_bCRrgpAlQWZQWG2q-VWHjSCXA').sheet1.col_values(1)
        perv_set = set(''.join(google_values).split(' '))
        print(len(perv_set))
        pickle.dump(perv_set, file)
        google_values.clear()

    with open('db2', 'wb') as file:
        client = gspread.service_account('google.json')
        google_values = client.open_by_key('1d0OS28iDsUEkQm1Rh_bCRrgpAlQWZQWG2q-VWHjSCXA').sheet1.col_values(1)
        pickle.dump(set(''.join(google_values).split(' ')), file)
        google_values.clear()
    print('конец, файл записан')
