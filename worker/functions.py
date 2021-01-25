import os
import re
import heroku3
import gspread
import objects
import _thread
import requests
from time import sleep
from GDrive import Drive
from copy import deepcopy
from bs4 import BeautifulSoup
from itertools import product
from statistics import median
from string import ascii_lowercase, ascii_uppercase
# ========================================================================================================
stamp1 = objects.time_now()
t_me = 'https://t.me/'
objects.environmental_files()
array_db, combinations = {}, []
drive_client = Drive('google.json')
ErrorAuth = objects.AuthCentre(os.environ['ERROR-TOKEN'])

worker = {
    'row': 0,
    'prefix': '',
    'folder': '',
    'status': '✅',
    'workers_count': 0,
    'range': range(0, 0),
    'combinations_count': 0,
    'api': os.environ.get('api'),
    'saved_workers_count': os.environ.get('workers_count')}


def save_array_to_file(path, array):
    file = open(path, 'w')
    text = ' '.join(array)
    if text == '':
        text = 'CLEAR_FILE_FILLER'
    file.write(text)
    file.close()


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


def checking():
    global array_db
    from datetime import datetime
    if combinations:
        counter = 0
        counter_array = []
        stamp = datetime.now().timestamp()
        while True:
            try:
                for username in combinations:
                    if counter == 0:
                        stamp = datetime.now().timestamp()
                    if username not in array_db[f"{worker['prefix']}_used.txt"] and worker['status'] != '✅':
                        sleep(0.0001)
                        try:
                            response = requests.get(t_me + username)
                        except IndexError and Exception:
                            sleep(0.01)
                            try:
                                response = requests.get(t_me + username)
                            except IndexError and Exception:
                                response = None
                        if response:
                            counter += 1
                            soup = BeautifulSoup(response.text, 'html.parser')
                            is_username_exist = soup.find('a', class_='tgme_action_button_new')
                            if is_username_exist is None:
                                array_db[f"{worker['prefix']}_clear.txt"].append(username)
                            array_db[f"{worker['prefix']}_used.txt"].append(username)
                            if counter == 300:
                                counter_array.append(datetime.now().timestamp() - stamp)
                                print(median(counter_array))
                                counter = 0
            except IndexError and Exception:
                ErrorAuth.thread_exec()


def files_upload():
    global worker, array_db, drive_client
    while True:
        try:
            sleep(100)
            temp_db = deepcopy(array_db)
            for key in worker:
                if key.endswith('.txt'):
                    save_array_to_file(key, temp_db[key])
                    try:
                        drive_client.update_file(worker[key], key)
                    except IndexError and Exception:
                        drive_client = Drive('google.json')
                        drive_client.update_file(worker[key], key)

            if len(temp_db[f"{worker['prefix']}_used.txt"]) == len(combinations):
                worksheet = gspread.service_account('google.json').open('master').worksheet('main')
                resources = worksheet.get('A1:Z50000')
                worker['status'] = '✅'
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
                objects.printer('Цикл проверок доступности юзеров пройден.')
        except IndexError and Exception:
            ErrorAuth.thread_exec()


def variables_creation():
    global worker
    spreadsheet = gspread.service_account('google.json').open('master')
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
                range_end = worker['combinations_count']
            if len(row) >= 8:
                worker['status'] = row[7]
            for postfix in ['used', 'clear']:
                file_name = f"{worker['prefix']}_{postfix}.txt"
                worker[file_name] = ''
                db[file_name] = []
            for file in drive_client.files(name_startswith=worker['prefix']):
                if file['name'] in worker:
                    worker[file['name']] = file['id']
                    drive_client.download_file(file['id'], file['name'])
            worker['range'] = range((worker['row'] - 2) * step, range_end + 1)

    for file in drive_client.files(only_folders=True):
        if file['name'] == 'temp':
            worker['folder'] = file['id']

    for key in worker:
        if key.endswith('.txt') and worker[key] == '':
            save_array_to_file(key, [])
            response = drive_client.create_file(key, worker['folder'])
            objects.printer(f'{key} создан')
            worker[key] = response['id']

    if str(worker['workers_count']) != worker['saved_workers_count']:
        for key in worker:
            if key.endswith('.txt'):
                save_array_to_file(key, [])
        account = heroku3.from_key(worker['api'])
        for app in account.apps():
            config = app.config()
            config['workers_count'] = str(worker['workers_count'])

    for key in worker:
        if key.endswith('.txt'):
            file = open(key, 'r')
            text = re.sub('CLEAR_FILE_FILLER', '', file.read())
            if text:
                db[key] = text.split(' ')
            file.close()

    combs = combinations_generate()
    return db, combs


def start():
    global array_db, combinations
    if os.environ.get('api'):
        array_db, combinations = variables_creation()
        print('len(combs) =', len(combinations))
        print(objects.time_now() - stamp1)
        for m in worker:
            print(m + ':', worker[m])
        print('len(array_db[' + worker['prefix'] + '_used.txt]) =', len(array_db[worker['prefix'] + '_used.txt']))
        if worker['prefix'] and worker['folder']:
            print('Запуск скрипта за', objects.time_now() - stamp1, 'секунд')
            _thread.start_new_thread(files_upload, ())
            checking()

    ErrorAuth.start_message(stamp1, f"\nОшибка с переменными окружения.\n{objects.bold('Бот выключен')}")
# ========================================================================================================
