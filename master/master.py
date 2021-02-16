import io
import os
import re
import pickle
import gspread
import objects
import heroku3
from time import sleep
from copy import deepcopy
from itertools import product
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from string import ascii_lowercase, ascii_uppercase
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
# ========================================================================================================
standard_file_fields = 'files(id, name, parents, createdTime, modifiedTime)'


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

    def update_file(self, file_id, file_path):
        media_body = MediaFileUpload(file_path, resumable=True)
        return self.client.files().update(fileId=file_id, media_body=media_body).execute()

    def file(self, file_id):
        fields = 'id, name, parents, createdTime, modifiedTime'
        result = self.client.files().get(fileId=file_id, fields=fields).execute()
        return self.revoke_time(result)

    def create_folder(self, name, folder_id):
        file_metadata = {'name': name, 'parents': [folder_id], 'mimeType': 'application/vnd.google-apps.folder'}
        result = self.client.files().create(body=file_metadata, fields='id, name, createdTime').execute()
        return self.revoke_time(result)

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


def to_len(value):
    if type(value) == list:
        value = len(value)
    return value


def clear_stats(stamp):
    response = {'stamp': stamp}
    for stats_key in ['cleared', 'used']:
        response[stats_key] = {'5': []}
        for i in [2, 3, 4, 'any']:
            response[stats_key][f'{i}+bot'] = []
        for deep_key in list(response[stats_key]):
            response[stats_key][f'_{deep_key}'] = []
            if deep_key.startswith('any'):
                response[stats_key]['any+_bot'] = []
    return response


def variables_creation():
    global master
    objects.environmental_files()
    client = gspread.service_account('google.json')
    sheet = client.open_by_key(master['sheet_id']).worksheet('main')
    drive_client = Drive('google.json')
    resources = sheet.get('A1:Z50000')
    keys = resources.pop(0)
    scripts = []
    for row in resources:
        if len(row) == len(keys):
            worker = {'update': 0, 'server': '1'}
            for i in range(0, len(keys)):
                worker[keys[i]] = row[i]
            scripts.append(worker)

    for worker in scripts:
        split = {'1': {}, '2': {}}
        for key in worker:
            if key[-1] in ['1', '2']:
                split[key[-1]][key[:-1].strip()] = worker[key]
        if os.environ.get('local') is None:
            for key in split:
                connection = heroku3.from_key(split[key]['HEROKU API'])
                if connection.account().email != split[key]['EMAIL']:
                    worker[f'EMAIL {key}'] = connection.account().email
                    worker['update'] = 1
                for app in connection.apps():
                    if len(app.dynos()) > 0:
                        worker['server'] = key
                    if app.name != split[key]['THREAD']:
                        worker[f'THREAD {key}'] = app.name
                        worker['update'] = 1

    for folder in drive_client.files(only_folders=True):
        if folder['name'] == 'temp':
            master['temp_id'] = folder['id']
        elif folder['name'] == 'telegram_usernames':
            master['main_id'] = folder['id']
        elif folder['name'].startswith('logs'):
            if master['logs_created_time'] <= folder['createdTime']:
                master['logs_number'] = int(re.sub(r'\D', '', folder['name']))
                master['logs_created_time'] = folder['createdTime']
                master['logs_id'] = folder['id']

    for length in range(3, 5 + 1):
        for value in product(f"{ascii_lowercase}_", repeat=length):
            username = ''.join(value)
            if username.startswith('_') is False and re.search('__', username) is None \
                    and username[:1].isdigit() is False:
                if length in [3, 4]:
                    username += 'bot'
                if username.endswith('_') is False:
                    master['max_users_count'] += 1

    print(scripts)
    print(master)
    return keys, sheet, scripts


# ========================================================================================================


def logs_to_google():
    global worksheet, workers
    while True:
        try:
            sleep(20)
            google_updated = True
            try:
                resources = worksheet.get('A1:Z50000')
            except IndexError and Exception:
                client = gspread.service_account('google.json')
                worksheet = client.open_by_key(master['sheet_id']).worksheet('main')
                resources = worksheet.get('A1:Z50000')
            for worker in workers:
                if worker['update'] == 1:
                    row_index = len(resources) + 1
                    for row in resources:
                        if worker['PREFIX'] in row:
                            row_index = resources.index(row) + 1
                    values_indexes = range(0, len(google_keys))
                    client = gspread.service_account('google.json')
                    connection = heroku3.from_key(worker[f"HEROKU API {worker['server']}"])
                    google_range = f'A{row_index}:{ascii_uppercase[values_indexes[-1]]}{row_index}'
                    worksheet = client.open_by_key(master['sheet_id']).worksheet('main')
                    work_range = worksheet.range(google_range)
                    for i in values_indexes:
                        work_range[i].value = worker[google_keys[i]]
                    worksheet.update_cells(work_range)
                    for app in connection.apps():
                        app.restart()
                    google_updated = False
                    worker['update'] = 0
                    sleep(5)

            resources.pop(0)
            workers_count = 0
            ended_workers_count = 0
            for google_worker in resources:
                if len(google_worker) == len(google_keys):
                    workers_count += 1
                    if google_worker[google_keys.index('PROGRESS')] == '✅':
                        ended_workers_count += 1
            if str(workers_count) != master['workers_count']:
                for worker in workers:
                    connection = heroku3.from_key(worker[f"HEROKU API {worker['server']}"])
                    for app in connection.apps():
                        app.restart()
                for api in [master['another_api'], master['api']]:
                    connection = heroku3.from_key(api)
                    for app in connection.apps():
                        config = app.config()
                        config['workers_count'] = str(workers_count)
            if workers_count == ended_workers_count and google_updated:
                drive_client = Drive('google.json')
                logs_db = {'clear': [], 'used_count': 0}
                logs = {'previous': {'id': '', 'createdTime': 0}}
                objects.printer('Цикл проверок доступности юзеров пройден. '
                                'Записываем список свободных username в google')

                if len(drive_client.files(parents=master['logs_id'])) >= 400:
                    folder = drive_client.create_folder(f"logs-{master['logs_number'] + 1}", master['main_id'])
                    master['logs_created_time'] = folder['createdTime']
                    master['logs_id'] = folder['id']
                    master['logs_number'] += 1

                for file in drive_client.files(parents=master['temp_id']):
                    drive_client.download_file(file['id'], file['name'])
                    with open(file['name'], 'rb') as local_file:
                        array = pickle.load(local_file)
                        if 'clear' in file['name']:
                            logs_db['clear'].extend(array)
                        else:
                            logs_db['used_count'] += len(array)
                        array.clear()
                    os.remove(file['name'])

                if logs_db['used_count'] >= master['max_users_count']:
                    step = 50
                    with open('logs_raw', 'wb') as file:
                        pickle.dump(logs_db['clear'], file)
                    logs_db.clear()
                    print('записали в файл, очистили логс_дб')
                    sleep(300)
                    print('начинаем сет')
                    with open('logs_raw', 'rb') as file:
                        logs_set = set(pickle.load(file))
                    os.remove('logs_raw')
                    print('сет закончен')
                    with open('main', 'wb') as file:
                        pickle.dump(logs_set, file)
                    print('запись сета в файл')
                    text = ' '.join(list(logs_set))
                    print('сделали текст из сета')
                    logs_set.clear()
                    for worker in workers:
                        worker['update'] = 1
                        worker['PROGRESS'] = '♿'
                    spreadsheet = gspread.service_account('google.json').create('logs', master['logs_id'])
                    chunks = [text[offset: offset + 50000] for offset in range(0, len(text), 50000)]
                    logs_worksheet = spreadsheet.sheet1
                    this_logs = drive_client.file(spreadsheet.id)
                    logs_worksheet.resize(rows=len(chunks), cols=1)
                    creation_year = datetime.utcfromtimestamp(this_logs['createdTime']).strftime('%Y')
                    spreadsheet.batch_update(objects.properties_json(logs_worksheet.id, len(chunks), chunks[:step]))
                    this_logs['creation_year'] = int(creation_year)
                    if len(chunks) > step:
                        request_counter = 1
                        for loop in range(step, len(chunks), step):
                            row_begin = loop + 1
                            row_end = row_begin + step
                            if row_end > len(chunks):
                                row_end = len(chunks)
                            work_range = logs_worksheet.range(f'A{row_begin}:A{row_end}')
                            for row in range(row_begin, row_end + 1):
                                if row <= len(chunks):
                                    work_range[row - loop - 1].value = chunks[row - 1]
                            logs_worksheet.update_cells(work_range)
                            request_counter += 1
                            if request_counter == 50:
                                request_counter = 0
                                sleep(100)

                    log_text = 'Успешно записана информация в telegram_usernames'
                    objects.printer(f"{log_text}/logs-{master['logs_number']}/")
                    logs.update({i: this_logs for i in logs_keys})
                    drive_client = Drive('google.json')
                    stats_files = {}
                    full_stats = {}
                    folders_id = []
                    files = []

                    for folder in drive_client.files(only_folders=True):
                        if folder['name'].startswith('logs'):
                            folders_id.append(folder['id'])
                        if folder['name'].startswith('arrays'):
                            stats_files['folder_id'] = folder['id']

                    for folder in folders_id:
                        for file in drive_client.files(parents=folder):
                            creation_year = datetime.utcfromtimestamp(file['createdTime']).strftime('%Y')
                            file['creation_year'] = int(creation_year)
                            files.append(file)

                    for file in drive_client.files(parents=stats_files['folder_id']):
                        stats_files[file['name']] = file['id']

                    for file in files:
                        if file['createdTime'] >= logs['previous']['createdTime'] and \
                                file['id'] != logs['this']['id']:
                            logs['previous'] = deepcopy(file)
                        if file['createdTime'] <= logs['first_in_year']['createdTime'] and \
                                file['creation_year'] == logs['this']['creation_year']:
                            logs['first_in_year'] = deepcopy(file)
                        if file['createdTime'] <= logs['first_ever']['createdTime']:
                            logs['first_ever'] = deepcopy(file)
                        if logs['this']['createdTime'] - 30 * 24 * 60 * 60 <= file['createdTime']:
                            logs['month_ago'] = deepcopy(file)
                        if logs['this']['createdTime'] - 7 * 24 * 60 * 60 <= file['createdTime']:
                            logs['week_ago'] = deepcopy(file)

                    drive_client.update_file(stats_files['main'], 'main')
                    for log_key in logs:
                        stats = clear_stats(logs[log_key].get('createdTime'))
                        if log_key == 'this':
                            with open('main', 'rb') as file:
                                db1 = pickle.load(file)
                            arrays = {'cleared': list(db1)}
                            del stats['used']
                        else:
                            logs_id = logs[log_key].get('id')
                            with open('db2', 'wb') as file:
                                client = gspread.service_account('google.json')
                                google_values = client.open_by_key(logs_id).sheet1.col_values(1)
                                pickle.dump(set(''.join(google_values).split(' ')), file)
                                google_values.clear()
                            with open('main', 'rb') as file:
                                db1 = pickle.load(file)
                            with open('db2', 'rb') as file:
                                db2 = pickle.load(file)
                            arrays = {'cleared': list(db2 - db1), 'used': list(db1 - db2)}
                            os.remove('db2')
                            db2.clear()
                        db1.clear()
                        for key in arrays:
                            for username in arrays[key]:
                                under = ''
                                if username.endswith('bot'):
                                    stats[key]['any+bot'].append(username)
                                if '_' in username:
                                    under = '_'
                                    if username.endswith('_bot'):
                                        stats[key]['any+_bot'].append(username)
                                    if username.endswith('bot'):
                                        stats[key]['_any+bot'].append(username)
                                if len(username) == 5:
                                    stats[key][f'{under}5'].append(username)
                                else:
                                    stats[key][f'{under}{len(username) - 3}+bot'].append(username)
                        arrays.clear()
                        with open(log_key, 'wb') as file:
                            pickle.dump(stats, file)
                        drive_client.update_file(stats_files[log_key], log_key)
                        for key in stats:
                            if type(stats[key]) == dict:
                                for deep_key in stats[key]:
                                    stats[key][deep_key] = to_len(stats[key][deep_key])
                        full_stats.update({log_key: stats})
                        os.remove(log_key)
                        stats.clear()
                    with open('stats', 'wb') as file:
                        pickle.dump(full_stats, file)
                    drive_client.update_file(stats_files['stats'], 'stats')
                    os.remove('stats')
                    os.remove('main')
                    sleep(100)
                else:
                    log_text = objects.bold('Нарушена целостность массива\n(все workers закончили работу):\n') + \
                        f"Необходимая длина массива проверок = {objects.code(master['max_users_count'])}\n" \
                        f"Длина полученного массива = {objects.code(logs_db['used_count'])}"
                    objects.printer(re.sub('<.*?>', '', log_text))
                    ErrorAuth.send_dev_message(log_text, tag=None)
                    logs_db.clear()
                    sleep(10800)
        except IndexError and Exception:
            ErrorAuth.thread_exec()


master = {
    'main_id': '',
    'temp_id': '',
    'logs_id': '',
    'logs_number': 0,
    'max_users_count': 0,
    'logs_created_time': 0,
    'api': os.environ.get('api'),
    'another_api': os.environ.get('another_api'),
    'workers_count': os.environ.get('workers_count'),
    'sheet_id': '1OdAyu4zUTgww6AXaCeuJwT6OJzokWqPxPpjx1zbKvKQ'}

t_me = 'https://t.me/'
Auth = objects.AuthCentre(os.environ['DEV-TOKEN'])
google_keys, worksheet, workers = variables_creation()
ErrorAuth = objects.AuthCentre(os.environ['ERROR-TOKEN'])
logs_keys = ['this', 'week_ago', 'month_ago', 'first_ever', 'first_in_year']
# ========================================================================================================


def start(stamp):
    for key in master:
        if master[key] in ['', 0, None]:
            break
    else:
        if os.environ.get('local') is None:
            Auth.start_message(stamp)
        logs_to_google()

    ErrorAuth.start_message(stamp, f"\nОшибка с переменными окружения.\n{objects.bold('Бот выключен')}")


if os.environ.get('local'):
    start(objects.time_now())
