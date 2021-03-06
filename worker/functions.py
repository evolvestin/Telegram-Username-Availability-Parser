import io
import os
import re
import pickle
import heroku3
import gspread
import objects
import _thread
import requests
from time import sleep
from copy import deepcopy
import concurrent.futures
from bs4 import BeautifulSoup
from itertools import product
from google.oauth2 import service_account
from googleapiclient.discovery import build
from string import ascii_lowercase, ascii_uppercase
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
# ========================================================================================================
stamp1 = objects.time_now()
standard_file_fields = 'files(id, name, parents, createdTime, modifiedTime)'


class Drive:
    def __init__(self, path):
        scope = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_file(path, scopes=scope)
        self.client = build('drive', 'v3', credentials=credentials)

    def update_file(self, file_id, file_path, description=''):
        file_metadata = {'description': description}
        media_body = MediaFileUpload(file_path, resumable=True)
        return self.client.files().update(fileId=file_id, media_body=media_body, body=file_metadata).execute()

    def create_file(self, file_path, folder_id, same_file_name='True'):
        if same_file_name == 'True':
            same_file_name = re.sub('(.*)/', '', file_path)
        media_body = MediaFileUpload(file_path, resumable=True)
        file_metadata = {'name': same_file_name, 'parents': [folder_id]}
        return self.client.files().create(body=file_metadata, media_body=media_body, fields='id').execute()

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
            for key in ['modifiedTime', 'createdTime']:
                if file.get(key):
                    stamp = re.sub(r'\..*?Z', '', file[key])
                    file[key] = objects.stamper(stamp, '%Y-%m-%dT%H:%M:%S')
            response.append(file)
        return response


def save_array_to_file(path, array):
    with open(path, 'wb') as file:
        pickle.dump(array, file)


def combinations_generate(combinations_count=False):
    global worker
    chunk = []
    counter = 0
    for length in range(3, 5 + 1):
        for value in product(f"{ascii_lowercase}_", repeat=length):
            username = ''.join(value)
            if username.startswith('_') is False and re.search('__', username) is None \
                    and username[:1].isdigit() is False:
                if length in [3, 4]:
                    username += 'bot'
                if username.endswith('_') is False:
                    counter += 1
                    if counter in worker['range'] and combinations_count is False:
                        chunk.append(username)
                    elif combinations_count:
                        worker['combinations_count'] += 1
    return chunk


def update_status_in_google(status):
    global worker
    client = gspread.service_account('google.json')
    spreadsheet = client.open_by_key(worker['master_sheet_id'])
    worksheet = spreadsheet.worksheet('main')
    resources = worksheet.get('A1:Z50000')
    worker['status'] = status
    for row in resources:
        if worker['api'] in row:
            row[7] = worker['status']
            values_indexes = range(0, len(row))
            row_index = resources.index(row) + 1
            google_range = f'A{row_index}:{ascii_uppercase[values_indexes[-1]]}{row_index}'
            work_range = worksheet.range(google_range)
            for i in values_indexes:
                work_range[i].value = row[i]
            worksheet.update_cells(work_range)
            break


def checking():
    global array_db
    from datetime import datetime
    while True:
        try:
            for chunk in combinations:
                results = []
                stamp = datetime.now().timestamp()
                if worker['status'] != '✅':
                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as future_executor:
                        futures = [future_executor.submit(requests.get, t_me + future) for future in chunk]
                        for future in concurrent.futures.as_completed(futures):
                            results.append(future.result())

                for result in results:
                    soup = BeautifulSoup(result.content, 'html.parser')
                    url = soup.find('meta', {'name': 'twitter:app:url:googleplay'})
                    is_username_exist = soup.find('a', class_='tgme_action_button_new')
                    if url:
                        username = re.sub(t_me, '', str(url.get('content')))
                        if username not in ['None', '']:
                            if is_username_exist is None:
                                array_db[f"{worker['prefix']}_clear"].append(username)
                            array_db[f"{worker['prefix']}_used"].append(username)

                delay = int(60 - datetime.now().timestamp() + stamp) + 1
                if delay < 0:
                    delay = 0
                sleep(delay)
            if len(combinations) == 0:
                print('Ожидаем перезапуска от master скрипта')
                sleep(500)
        except IndexError and Exception:
            ErrorAuth.thread_exec()


def files_upload():
    global worker, array_db, drive_client
    while True:
        try:
            sleep(100)
            temp_db = deepcopy(array_db)
            for key in worker:
                if key.endswith(('clear', 'used')):
                    save_array_to_file(key, temp_db[key])
                    description = f"len({key}) = {len(temp_db[key])} / {len(worker['range'])}"
                    try:
                        drive_client.update_file(worker[key], key, description)
                    except IndexError and Exception:
                        drive_client = Drive('google.json')
                        drive_client.update_file(worker[key], key, description)

            if len(temp_db[f"{worker['prefix']}_used"]) >= len(worker['range']) - 1 and worker['status'] == '🅰️':
                update_status_in_google('✅')
                objects.printer('Цикл проверок доступности юзеров пройден.')
                _thread.exit()
        except IndexError and Exception:
            ErrorAuth.thread_exec()


def variables_creation():
    global worker
    spreadsheet = gspread.service_account('google.json').open_by_key(worker['master_sheet_id'])
    resources = spreadsheet.worksheet('main').get('A1:Z50000')
    combinations_generate(combinations_count=True)
    worker['workers_count'] = len(resources) - 1
    db = {}

    for row in resources:
        if worker['api'] in row:
            worker['prefix'] = row[0]
            worker['row'] = resources.index(row) + 1
            step = worker['combinations_count'] // worker['workers_count']
            range_end = (worker['row'] - 1) * step
            if worker['row'] - 1 == worker['workers_count']:
                range_end = worker['combinations_count'] + 1
            if len(row) >= 8:
                worker['status'] = row[7]
                if worker['api'] == row[6]:
                    worker['another_api'] = row[3]
                if worker['api'] == row[3]:
                    worker['another_api'] = row[6]
            for postfix in ['clear', 'used']:
                file_name = f"{worker['prefix']}_{postfix}"
                worker[file_name] = ''
                db[file_name] = []
            for file in drive_client.files(name_startswith=worker['prefix']):
                if file['name'] in worker:
                    worker[file['name']] = file['id']
                    drive_client.download_file(file['id'], file['name'])
            worker['range'] = range((worker['row'] - 2) * step, range_end)

    for file in drive_client.files(only_folders=True):
        if file['name'] == 'temp':
            worker['folder'] = file['id']

    for key in worker:
        if key.endswith(('clear', 'used')) and worker[key] == '':
            save_array_to_file(key, [])
            response = drive_client.create_file(key, worker['folder'])
            objects.printer(f'{key} создан')
            worker[key] = response['id']

    if str(worker['workers_count']) != worker['saved_workers_count'] or worker['status'] == '♿':
        apis = [worker['api']]
        if worker['another_api']:
            apis.insert(0, worker['another_api'])
        for key in worker:
            if key.endswith(('clear', 'used')):
                save_array_to_file(key, [])
                drive_client.update_file(worker[key], key, f"len({key}) = 0 / {len(worker['range'])}")
        update_status_in_google('🅰️')
        for api in apis:
            for app in heroku3.from_key(api).apps():
                config = app.config()
                config['workers_count'] = str(worker['workers_count'])

    for key in worker:
        if key.endswith(('clear', 'used')):
            with open(key, 'rb') as file:
                db[key] = pickle.load(file)

    combs = combinations_generate()
    combs = list(set(combs) - set(db[f"{worker['prefix']}_used"]))
    print('len(combs) =', len(combs))
    return db, [combs[i:i + 300] for i in range(0, len(combs), 300)]


worker = {
    'row': 0,
    'prefix': '',
    'folder': '',
    'status': '✅',
    'another_api': '',
    'workers_count': 0,
    'range': range(0, 0),
    'combinations_count': 0,
    'api': os.environ.get('api'),
    'saved_workers_count': os.environ.get('workers_count'),
    'master_sheet_id': '1OdAyu4zUTgww6AXaCeuJwT6OJzokWqPxPpjx1zbKvKQ'}

t_me = 'https://t.me/'
objects.environmental_files()
array_db, combinations = {}, []
drive_client = Drive('google.json')
ErrorAuth = objects.AuthCentre(os.environ['ERROR-TOKEN'])


def start():
    global array_db, combinations
    if os.environ.get('api'):
        array_db, combinations = variables_creation()
        for m in worker:
            print(m + ':', worker[m])
        print('len(sessions) =', len(combinations))
        print('len(array_db[' + worker['prefix'] + '_used]) =', len(array_db[worker['prefix'] + '_used']))
        if worker['prefix'] and worker['folder']:
            print('Запуск скрипта за', objects.time_now() - stamp1, 'секунд')
            _thread.start_new_thread(files_upload, ())
            checking()

    ErrorAuth.start_message(stamp1, f"\nОшибка с переменными окружения.\n{objects.bold('Скрипт выключен')}")
# ========================================================================================================
